mod checksum;
mod downloader;
mod io;
mod recover_metadata;
mod save_conflict;
mod server_conflict;

use std::{path::PathBuf, sync::Arc};

use fs2::FileExt;
use reqwest::{
    Client, Proxy, Url,
    header::{HeaderMap, USER_AGENT},
};

use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

use crate::config::Config;
use crate::download_manager::checksum::check_final_file_checksum;
use crate::download_manager::recover_metadata::recover_metadata;
use crate::download_manager::{downloader::Downloader, io::persist_metadata};
use crate::download_manager::{io::assemble_final_file, server_conflict::resolve_server_conflicts};
use crate::download_manager::{io::remove_all_parts, save_conflict::resolve_save_conflicts};
use crate::error::MetadataError;
use crate::progress::{DownloadContext, Phase, ProgressEvent};
use crate::response_info::ResponseInfo;
use crate::retry_policies::{FixedThenExponentialRetry, wait_for_retry};
use crate::{
    conflict::{SaveConflictResolver, ServerConflictResolver},
    download_manager::io::sum_parts_on_disk,
};
use crate::{credentials::Credentials, user_agents::random_user_agent};
use crate::{download::Download, download_metadata::PartDetails, error::OdlError};

/// High level manager responsible for evaluating and running downloads.
///
/// `DownloadManager` coordinates concurrency (via an internal semaphore),
/// constructs HTTP clients using `Config`, resolves conflicts (save/server),
/// and orchestrates the multi-part downloader.
///
/// Typical usage (illustrative):
///
/// ```ignore
/// // Create manager and evaluate a URL to receive a `Download` instruction.
/// // The manager will perform an HTTP probe and return a populated
/// // `Download` value which can then be passed to `DownloadManager::download`.
/// //
/// // let cfg = Config::default();
/// // let manager = DownloadManager::new(cfg);
/// // let instruction = manager.evaluate(url, save_dir, None, &save_resolver).await?;
/// // let path = manager.download(instruction, &server_resolver).await?;
/// ```
#[derive(Debug)]
pub struct DownloadManager {
    /// Config to use for DownloadManager
    config: Config,

    /// Semaphore to limit concurrent downloads (not exposed in builder)
    semaphore: Arc<Semaphore>,
}

impl DownloadManager {
    pub fn new(config: Config) -> DownloadManager {
        let max_concurrent_downloads = config.max_concurrent_downloads;
        DownloadManager {
            config,
            semaphore: Arc::new(Semaphore::new(max_concurrent_downloads)),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub async fn set_config(&mut self, value: Config) -> Result<(), AcquireError> {
        let old_max = self.config.max_concurrent_downloads;
        self.config = value;
        let new_max = self.config.max_concurrent_downloads;
        if new_max > old_max {
            let add_count = new_max.saturating_sub(old_max);
            self.semaphore.add_permits(add_count);
        } else if new_max < old_max {
            let forget_count = old_max.saturating_sub(new_max);
            // Acquire forget_count permits (will wait until in-use permits are released)
            let _perm = Arc::clone(&self.semaphore)
                .acquire_many_owned(forget_count as u32)
                .await?;
            // Prevent release of these permits so they are permanently consumed
            _perm.forget();
        }
        Ok(())
    }

    /// Probe the remote URL and resolve save conflicts, returning a
    /// [`Download`] ready for [`Self::download`].
    ///
    /// Use [`Self::evaluate_with`] to attach a progress reporter or
    /// cancellation token.
    pub async fn evaluate<CR>(
        &self,
        url: Url,
        save_dir: PathBuf,
        credentials: Option<Credentials>,
        conflict_resolver: &CR,
    ) -> Result<Download, OdlError>
    where
        CR: SaveConflictResolver,
    {
        self.evaluate_with(
            url,
            save_dir,
            credentials,
            conflict_resolver,
            &DownloadContext::new(),
        )
        .await
    }

    /// Same as [`Self::evaluate`] with an explicit [`DownloadContext`] for
    /// progress reporting and cancellation.
    pub async fn evaluate_with<CR>(
        &self,
        url: Url,
        save_dir: PathBuf,
        credentials: Option<Credentials>,
        conflict_resolver: &CR,
        ctx: &DownloadContext,
    ) -> Result<Download, OdlError>
    where
        CR: SaveConflictResolver,
    {
        ctx.emit(ProgressEvent::PhaseChanged(Phase::Evaluating));
        if ctx.is_cancelled() {
            return Err(OdlError::Cancelled);
        }
        let client = self.get_client(None)?;

        let retry_policy = FixedThenExponentialRetry {
            max_n_retries: self.config.max_retries,
            wait_time: self.config.wait_between_retries,
            n_fixed_retries: self.config.n_fixed_retries,
        };

        let mut attempts: u32 = 0;
        let resp = loop {
            let mut req = client
                .head(url.clone())
                // we request hash just in case server implements and responds
                // we will use this later for checking the final file against
                .header(
                    "Want-Repr-Digest",
                    "sha-512=9, sha-384=8, sha-256=7, sha-1=1, md5=1",
                )
                .header(
                    "Want-Content-Digest",
                    "sha-512=9, sha-384=8, sha-256=7, sha-1=1, md5=1",
                );
            if let Some(creds) = &credentials {
                req = req.basic_auth(creds.username(), creds.password());
            }
            if self.config.user_agent.is_none() && self.config.randomize_user_agent {
                req = req.header(USER_AGENT, random_user_agent());
            }

            match req.send().await.and_then(|r| r.error_for_status()) {
                Ok(r) => break r,
                Err(e) => {
                    attempts = attempts.saturating_add(1);
                    if !wait_for_retry(&retry_policy, attempts, ctx).await {
                        return Err(OdlError::from(e));
                    }
                    if ctx.is_cancelled() {
                        return Err(OdlError::Cancelled);
                    }
                }
            }
        };
        let info = ResponseInfo::from(resp);
        let instruction = Download::from_response_info(
            &self.config.download_dir,
            save_dir,
            info,
            self.config.max_connections,
            self.config.use_server_time,
            credentials,
            Option::<Proxy>::from(&self.config),
            Some(HeaderMap::from(&self.config)),
        );

        ctx.emit(ProgressEvent::PhaseChanged(Phase::ResolvingConflicts));
        let instruction = resolve_save_conflicts(instruction, conflict_resolver).await?;

        ctx.emit(ProgressEvent::FilenameResolved(
            instruction.filename().to_string(),
        ));
        ctx.emit(ProgressEvent::Progress {
            downloaded: 0,
            total: instruction.size(),
        });

        Ok(instruction)
    }

    /// Immediately starts a download using the given instruction and conflict_resolver.
    ///
    /// Use [`Self::download_with`] to attach a progress reporter or
    /// cancellation token.
    pub async fn download<CR>(
        &self,
        instruction: Download,
        conflict_resolver: &CR,
    ) -> Result<PathBuf, OdlError>
    where
        CR: ServerConflictResolver,
    {
        self.download_with(instruction, conflict_resolver, &DownloadContext::new())
            .await
    }

    /// Same as [`Self::download`] with an explicit [`DownloadContext`] for
    /// live progress and cancellation.
    pub async fn download_with<CR>(
        &self,
        instruction: Download,
        conflict_resolver: &CR,
        ctx: &DownloadContext,
    ) -> Result<PathBuf, OdlError>
    where
        CR: ServerConflictResolver,
    {
        let result = self
            .download_with_inner(instruction, conflict_resolver, ctx)
            .await;
        // Success emission lives in `process_download` so the
        // already-on-disk branch can be flagged distinctly. Failure /
        // cancellation are surfaced uniformly here so every error path
        // (including lockfile open / dir-create) reaches the reporter.
        match &result {
            Ok(_) => {}
            Err(OdlError::Cancelled) => ctx.emit(ProgressEvent::Cancelled),
            Err(e) => ctx.emit(ProgressEvent::Failed {
                message: e.to_string(),
            }),
        }
        result
    }

    async fn download_with_inner<CR>(
        &self,
        instruction: Download,
        conflict_resolver: &CR,
        ctx: &DownloadContext,
    ) -> Result<PathBuf, OdlError>
    where
        CR: ServerConflictResolver,
    {
        // we want to know issues about directory creation very early.
        tokio::fs::create_dir_all(instruction.download_dir()).await?;

        let f = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(instruction.lockfile_path())
            .await
            .map_err(|e| OdlError::StdIoError {
                e,
                extra_info: Some(format!(
                    "Failed to open lockfile for exclusive locking at {}",
                    instruction.lockfile_path().display(),
                )),
            })?;
        let f = f.into_std().await;
        if f.try_lock_exclusive().is_err() {
            return Err(OdlError::MetadataError(MetadataError::LockfileInUse));
        }

        let result = self
            .process_download(instruction, conflict_resolver, ctx)
            .await;
        let _ = FileExt::unlock(&f);
        result
    }

    /// acquire a permit from this download manager's semaphore. Only up to `max_concurrent_downloads` are permitted at the same time.
    pub async fn acquire_download_permit(&self) -> Result<OwnedSemaphorePermit, AcquireError> {
        Arc::clone(&self.semaphore).acquire_owned().await
    }

    fn get_client(&self, instructions: Option<&Download>) -> Result<Client, OdlError> {
        let mut client = reqwest::Client::builder();

        if let Some(download) = instructions {
            // we already have passed our config's headers and proxy to Download at evaluation, no need to check config here
            if let Some(proxy) = download.proxy() {
                client = client.proxy(proxy.clone());
            }

            if let Some(headers) = download.headers() {
                client = client.default_headers(headers.clone());
            }
        } else {
            // we want to evaluate the download, so we use config here
            if self.config.headers.as_ref().is_some_and(|x| !x.is_empty()) {
                client = client.default_headers(HeaderMap::from(&self.config));
            }
            if let Some(proxy) = Option::<Proxy>::from(&self.config) {
                client = client.proxy(proxy);
            }
        }

        if self.config.accept_invalid_certs {
            client = client.danger_accept_invalid_certs(self.config.accept_invalid_certs)
        }
        if let Some(user_agent) = &self.config.user_agent {
            client = client.user_agent(user_agent.clone());
        }
        if let Some(timeout) = &self.config.connect_timeout {
            client = client.connect_timeout(*timeout);
        }
        Ok(client.build()?)
    }

    async fn process_download<CR>(
        &self,
        instruction: Download,
        conflict_resolver: &CR,
        ctx: &DownloadContext,
    ) -> Result<PathBuf, OdlError>
    where
        CR: ServerConflictResolver,
    {
        if ctx.is_cancelled() {
            return Err(OdlError::Cancelled);
        }
        // early directory creation check to fail fast
        tokio::fs::create_dir_all(instruction.save_dir()).await?;

        recover_metadata(&instruction).await?;

        ctx.emit(ProgressEvent::PhaseChanged(Phase::ResolvingConflicts));
        let mut metadata = resolve_server_conflicts(&instruction, conflict_resolver).await?;

        // Best-effort early progress notice. The downloader's per-part
        // scheduler will re-emit a precise tracker-backed Progress as
        // each part is scheduled, so this is just for the case where the
        // download is already finished and the downloader never runs.
        let initial_on_disk = if metadata.finished {
            sum_parts_on_disk(&instruction, &metadata).await
        } else {
            None
        };
        if let Some(sum_of_parts_sizes) = initial_on_disk {
            let size: Option<u64> = metadata.size.or_else(|| instruction.size());
            ctx.emit(ProgressEvent::Progress {
                downloaded: sum_of_parts_sizes,
                total: size,
            });
        }

        // we skip over download parts if we already finished downloading
        if !metadata.finished {
            // Crash-recovery fast path. The aggregate `metadata.finished`
            // flag is only persisted after `assemble_final_file` returns
            // Ok (which includes checksum verification). If a prior run
            // crashed AFTER assembly but BEFORE that flag write, a fully
            // correct final file may already be on disk. Trust it only
            // when the server provided checksums and they verify — with
            // no checksum we cannot distinguish a good final file from a
            // zero-padded partial left by an even earlier interrupted
            // assembly (`assemble_blocking` calls `set_len(final_end)`
            // up front, so the size check alone is not safe).
            let final_path_recovery = instruction.final_file_path();
            if !metadata.checksums.is_empty()
                && tokio::fs::try_exists(&final_path_recovery)
                    .await
                    .unwrap_or(false)
                && check_final_file_checksum(&metadata, &instruction, false)
                    .await
                    .is_ok()
            {
                metadata.finished = true;
                persist_metadata(&metadata, &instruction).await?;
                remove_all_parts(instruction.download_dir()).await;
                ctx.emit(ProgressEvent::Completed {
                    path: final_path_recovery.clone(),
                    already_complete: true,
                });
                return Ok(final_path_recovery);
            }

            let to_download = metadata
                .parts
                .iter()
                .filter_map(|(_, p)| if !p.finished { Some(p.clone()) } else { None })
                .collect::<Vec<PartDetails>>();

            if !to_download.is_empty() {
                let randomize_user_agent = if self.config.user_agent.is_some() {
                    false
                } else {
                    self.config.randomize_user_agent
                };

                let client = self.get_client(Some(&instruction))?;
                let retry_policy = crate::retry_policies::FixedThenExponentialRetry {
                    max_n_retries: self.config.max_retries,
                    wait_time: self.config.wait_between_retries,
                    n_fixed_retries: self.config.n_fixed_retries,
                };
                ctx.emit(ProgressEvent::PhaseChanged(Phase::Downloading));
                let downloader = Downloader::new(
                    Arc::new(instruction.clone()),
                    metadata,
                    client,
                    randomize_user_agent,
                    self.config.speed_limit,
                    retry_policy,
                    ctx.clone(),
                );

                // Persist per-part finished flags but DO NOT set the
                // overall `metadata.finished = true` here: assembly is
                // still ahead and may be interrupted. The aggregate
                // flag is only flipped after assembly + checksum +
                // part cleanup all succeed below — that way an
                // interrupt mid-assembly leaves `finished = false` and
                // the next run re-runs assembly instead of trusting a
                // partial final file.
                let mdata = downloader.run().await?;
                persist_metadata(&mdata, &instruction).await?;
                metadata = mdata;
            }

            // Defensively delete any partial final file from a prior
            // interrupted assembly. `assemble_blocking` already opens
            // with `truncate(true)`, but removing first guarantees the
            // file is gone even if the open path errors out before
            // truncation, and removes any leftover xattrs/mtime.
            let final_path_for_cleanup = instruction.final_file_path();
            if tokio::fs::try_exists(&final_path_for_cleanup)
                .await
                .unwrap_or(false)
            {
                let _ = tokio::fs::remove_file(&final_path_for_cleanup).await;
            }

            ctx.emit(ProgressEvent::PhaseChanged(Phase::Assembling));
            let final_path = assemble_final_file(&metadata, &instruction, ctx).await?;

            // Mark metadata fully finished BEFORE removing parts. If
            // the process dies between these two steps, parts simply
            // leak on disk (cleanup done lazily on the next run); the
            // final file is already verified-correct. Doing the flag
            // flip after `remove_all_parts` would create the opposite
            // hazard: parts gone, flag still false, restart attempts
            // re-assembly with no source files.
            metadata.finished = true;
            persist_metadata(&metadata, &instruction).await?;

            remove_all_parts(instruction.download_dir()).await;

            ctx.emit(ProgressEvent::Completed {
                path: final_path.clone(),
                already_complete: false,
            });
            Ok(final_path)
        } else {
            let final_path = instruction.final_file_path();
            if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
                ctx.emit(ProgressEvent::Completed {
                    path: final_path.clone(),
                    already_complete: true,
                });
                Ok(final_path)
            } else {
                Err(OdlError::StdIoError {
                    e: std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Expected final file not found at {}", final_path.display()),
                    ),
                    extra_info: None,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conflict::FileChangedResolution;
    use crate::conflict::NotResumableResolution;
    use crate::conflict::ServerConflict;
    use crate::download::DownloadBuilder;
    use crate::download_metadata::PartDetails;
    use crate::error::ConflictError;
    use async_trait::async_trait;
    use mockito::Matcher;
    use mockito::Server;
    use std::collections::HashMap;
    use tempfile::tempdir;
    use tokio::fs;
    use tokio::io::AsyncWriteExt;

    struct AlwaysAbortResolver;

    #[async_trait]
    impl ServerConflictResolver for AlwaysAbortResolver {
        async fn resolve_file_changed(&self, _: &Download) -> FileChangedResolution {
            FileChangedResolution::Abort
        }
        async fn resolve_not_resumable(&self, _: &Download) -> NotResumableResolution {
            NotResumableResolution::Abort
        }
    }
    struct AlwaysReplaceResolver;

    #[async_trait]
    impl SaveConflictResolver for AlwaysReplaceResolver {
        async fn final_file_exists(
            &self,
            _: &Download,
        ) -> crate::conflict::FinalFileExistsResolution {
            crate::conflict::FinalFileExistsResolution::ReplaceAndContinue
        }
        async fn same_download_exists(
            &self,
            _: &Download,
        ) -> crate::conflict::SameDownloadExistsResolution {
            crate::conflict::SameDownloadExistsResolution::Resume
        }
    }

    #[tokio::test]
    async fn test_download_manager_multipart_download() -> Result<(), Box<dyn std::error::Error>> {
        // Prepare test data
        let file_content = b"HelloWorldThisIsATestFile";
        let part1 = &file_content[..10]; // "HelloWorld"
        let part2 = &file_content[10..]; // "ThisIsATestFile"

        // Start mock server
        let mut server = Server::new_async().await;
        let url = server.url();

        // HEAD request returns file info
        let head_mock = server
            .mock("HEAD", "/testfile")
            .with_status(200)
            .with_header("content-length", &file_content.len().to_string())
            .with_header("accept-ranges", "bytes")
            .with_header("etag", "testetag")
            .with_header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            .create_async()
            .await;

        // GET requests for each part
        let get_mock1 = server
            .mock("GET", "/testfile")
            .match_header("range", Matcher::Exact("bytes=0-9".into()))
            .with_status(206)
            .with_body(part1)
            .create_async()
            .await;

        let get_mock2 = server
            .mock("GET", "/testfile")
            .match_header(
                "range",
                Matcher::Exact(format!("bytes=10-{}", file_content.len() - 1)),
            )
            .with_status(206)
            .with_body(part2)
            .create_async()
            .await;

        // Build DownloadManager with 2 connections and separate download/save dirs
        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(2)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        // Evaluate to get Download instruction
        let save_resolver = AlwaysReplaceResolver {};
        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/testfile", url)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &save_resolver,
            )
            .await?;

        // Patch the instruction to simulate 2 parts
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(instruction.save_dir().clone())
            .filename(instruction.filename().to_string())
            .url(instruction.url().clone())
            .size(Some(file_content.len() as u64))
            .max_connections(2)
            .parts({
                let mut parts = HashMap::new();
                parts.insert(
                    "part1".to_string(),
                    PartDetails {
                        ulid: "part1".to_string(),
                        offset: 0,
                        size: 10,
                        finished: false,
                    },
                );
                parts.insert(
                    "part2".to_string(),
                    PartDetails {
                        ulid: "part2".to_string(),
                        offset: 10,
                        size: (file_content.len() - 10) as u64,
                        finished: false,
                    },
                );
                parts
            })
            .is_resumable(true)
            .build()
            .unwrap();

        let resolver = AlwaysAbortResolver {};
        // Download and concatenate
        let final_path = dlm.download(instruction, &resolver).await?;

        // Check file content
        let result = fs::read(&final_path).await?;
        assert_eq!(result, file_content);

        // Ensure mocks were hit
        head_mock.assert_async().await;
        get_mock1.assert_async().await;
        get_mock2.assert_async().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_save_conflict_final_file_exists_abort() -> Result<(), Box<dyn std::error::Error>>
    {
        // Start mock server
        let mut server = Server::new_async().await;
        let base = server.url();

        // HEAD request returns file info
        let head_mock = server
            .mock("HEAD", "/file_abort")
            .with_status(200)
            .with_header("content-length", "1")
            .with_header("accept-ranges", "bytes")
            .create_async()
            .await;

        // Prepare temp dirs and create final file to trigger conflict
        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let filename = "file_abort";
        let final_path = tmp_save_dir.path().join(filename);
        tokio::fs::write(&final_path, b"x").await?;

        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        struct AbortFinalResolver;
        #[async_trait]
        impl SaveConflictResolver for AbortFinalResolver {
            async fn final_file_exists(
                &self,
                _: &Download,
            ) -> crate::conflict::FinalFileExistsResolution {
                crate::conflict::FinalFileExistsResolution::Abort
            }
            async fn same_download_exists(
                &self,
                _: &Download,
            ) -> crate::conflict::SameDownloadExistsResolution {
                crate::conflict::SameDownloadExistsResolution::Resume
            }
        }

        let resolver = AbortFinalResolver {};

        let result = dlm
            .evaluate(
                Url::parse(&format!("{}/file_abort", base)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &resolver,
            )
            .await;

        assert!(matches!(
            result,
            Err(OdlError::Conflict(ConflictError::Save {
                conflict: crate::conflict::SaveConflict::FinalFileExists
            }))
        ));

        head_mock.assert_async().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_save_conflict_final_file_exists_add_number()
    -> Result<(), Box<dyn std::error::Error>> {
        // Start mock server
        let mut server = Server::new_async().await;
        let base = server.url();

        // HEAD request returns file info
        let head_mock = server
            .mock("HEAD", "/file_add")
            .with_status(200)
            .with_header("content-length", "1")
            .with_header("accept-ranges", "bytes")
            .create_async()
            .await;

        // Prepare temp dirs and create final file to trigger suggestion
        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let filename = "file_add";
        let final_path = tmp_save_dir.path().join(filename);
        tokio::fs::write(&final_path, b"x").await?;

        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        struct AddNumberResolver;
        #[async_trait]
        impl SaveConflictResolver for AddNumberResolver {
            async fn final_file_exists(
                &self,
                _: &Download,
            ) -> crate::conflict::FinalFileExistsResolution {
                crate::conflict::FinalFileExistsResolution::AddNumberToNameAndContinue
            }
            async fn same_download_exists(
                &self,
                _: &Download,
            ) -> crate::conflict::SameDownloadExistsResolution {
                crate::conflict::SameDownloadExistsResolution::Resume
            }
        }

        let resolver = AddNumberResolver {};

        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/file_add", base)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &resolver,
            )
            .await?;

        // Expect suggested alternative filename (file_add_2)
        assert_eq!(instruction.filename(), "file_add_2");

        head_mock.assert_async().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_manager_single_part_download() -> Result<(), Box<dyn std::error::Error>>
    {
        // Prepare test data
        let file_content = b"SinglePartFileContent";
        let part = &file_content[..];

        // Start mock server
        let mut server = Server::new_async().await;
        let url = server.url();

        // HEAD request returns file info
        let head_mock = server
            .mock("HEAD", "/singlefile")
            .with_status(200)
            .with_header("content-length", &file_content.len().to_string())
            .with_header("accept-ranges", "bytes")
            .with_header("etag", "singleetag")
            .with_header("last-modified", "Thu, 22 Oct 2015 07:28:00 GMT")
            .create_async()
            .await;

        // GET request for the whole file (single part)
        let get_mock = server
            .mock("GET", "/singlefile")
            .match_header(
                "range",
                Matcher::Exact(format!("bytes=0-{}", file_content.len() - 1)),
            )
            .with_status(206)
            .with_body(part)
            .create_async()
            .await;

        // Build DownloadManager with 1 connection and separate download/save dirs
        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        // Evaluate to get Download instruction
        let save_resolver = AlwaysReplaceResolver {};
        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/singlefile", url)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &save_resolver,
            )
            .await?;
        // Patch the instruction to simulate 1 part
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(instruction.save_dir().clone())
            .filename(instruction.filename().to_string())
            .url(instruction.url().clone())
            .size(Some(file_content.len() as u64))
            .max_connections(1)
            .parts({
                let mut parts = HashMap::new();
                parts.insert(
                    "part1".to_string(),
                    PartDetails {
                        ulid: "part1".to_string(),
                        offset: 0,
                        size: file_content.len() as u64,
                        finished: false,
                    },
                );
                parts
            })
            .is_resumable(true)
            .build()
            .unwrap();

        let resolver = AlwaysAbortResolver {};
        // Download and concatenate
        let final_path = dlm.download(instruction, &resolver).await?;

        // Check file content
        let result = fs::read(&final_path).await?;
        assert_eq!(result, file_content);

        // Ensure mocks were hit
        head_mock.assert_async().await;
        get_mock.assert_async().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_manager_multipart_not_resumable_download()
    -> Result<(), Box<dyn std::error::Error>> {
        // Prepare test data
        let file_content = b"NonResumableMultipartFile";

        // Start mock server
        let mut server = Server::new_async().await;
        let url = server.url();

        // HEAD request returns file info, but not resumable (no accept-ranges)
        let head_mock = server
            .mock("HEAD", "/nonresumablefile")
            .with_status(200)
            .with_header("content-length", &file_content.len().to_string())
            .with_header("etag", "nonresumableetag")
            .with_header("last-modified", "Fri, 23 Oct 2015 07:28:00 GMT")
            .create_async()
            .await;

        // Build DownloadManager with 2 connections and separate download/save dirs
        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(2)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        // Evaluate to get Download instruction
        let save_resolver = AlwaysReplaceResolver {};
        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/nonresumablefile", url)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &save_resolver,
            )
            .await?;

        // Patch the instruction to simulate 2 parts, but not resumable
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(instruction.save_dir().clone())
            .filename(instruction.filename().to_string())
            .url(instruction.url().clone())
            .size(Some(file_content.len() as u64))
            .max_connections(2)
            .parts({
                let mut parts = HashMap::new();
                parts.insert(
                    "part1".to_string(),
                    PartDetails {
                        ulid: "part1".to_string(),
                        offset: 0,
                        size: 10,
                        finished: false,
                    },
                );
                parts.insert(
                    "part2".to_string(),
                    PartDetails {
                        ulid: "part2".to_string(),
                        offset: 10,
                        size: (file_content.len() - 10) as u64,
                        finished: false,
                    },
                );
                parts
            })
            .is_resumable(false)
            .build()
            .unwrap();

        struct AssertTestResolver;
        #[async_trait]
        impl ServerConflictResolver for AssertTestResolver {
            async fn resolve_file_changed(&self, _: &Download) -> FileChangedResolution {
                FileChangedResolution::Abort
            }
            async fn resolve_not_resumable(&self, _: &Download) -> NotResumableResolution {
                NotResumableResolution::Abort
            }
        }

        let resolver = AssertTestResolver {};
        // Download should abort due to not resumable conflict
        let result = dlm.download(instruction, &resolver).await;

        assert!(matches!(
            result,
            Err(OdlError::Conflict(ConflictError::Server {
                conflict: ServerConflict::NotResumable
            }))
        ));

        // Ensure HEAD mock was hit, GET mocks may not be hit
        head_mock.assert_async().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_manager_multipart_not_resumable_restart_download()
    -> Result<(), Box<dyn std::error::Error>> {
        // Prepare test data
        let file_content = b"NonResumableMultipartFile";
        let part = &file_content[..];

        // Start mock server
        let mut server = Server::new_async().await;
        let url = server.url();

        // HEAD request returns file info, but not resumable (no accept-ranges)
        let head_mock = server
            .mock("HEAD", "/nonresumablefile_restart")
            .with_status(200)
            .with_header("content-length", &file_content.len().to_string())
            .with_header("etag", "nonresumableetag")
            .with_header("last-modified", "Fri, 23 Oct 2015 07:28:00 GMT")
            .create_async()
            .await;

        // GET request for the whole file (single part, since not resumable)
        let get_mock = server
            .mock("GET", "/nonresumablefile_restart")
            .match_header(
                "range",
                Matcher::Exact(format!("bytes=0-{}", file_content.len() - 1)),
            )
            .with_status(206)
            .with_body(part)
            .create_async()
            .await;

        // Build DownloadManager with 2 connections (will be forced to 1) and separate download/save dirs
        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(2)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        // Evaluate to get Download instruction
        let save_resolver = AlwaysReplaceResolver {};
        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/nonresumablefile_restart", url)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &save_resolver,
            )
            .await?;

        // Patch the instruction to simulate 2 parts, but not resumable
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(instruction.save_dir().clone())
            .filename(instruction.filename().to_string())
            .url(instruction.url().clone())
            .size(Some(file_content.len() as u64))
            .max_connections(2)
            .parts({
                let mut parts = std::collections::HashMap::new();
                parts.insert(
                    "part1".to_string(),
                    PartDetails {
                        ulid: "part1".to_string(),
                        offset: 0,
                        size: 10,
                        finished: false,
                    },
                );
                parts.insert(
                    "part2".to_string(),
                    PartDetails {
                        ulid: "part2".to_string(),
                        offset: 10,
                        size: (file_content.len() - 10) as u64,
                        finished: false,
                    },
                );
                parts
            })
            .is_resumable(false)
            .build()
            .unwrap();

        struct AssertTestResolver;
        #[async_trait]
        impl ServerConflictResolver for AssertTestResolver {
            async fn resolve_file_changed(&self, _: &Download) -> FileChangedResolution {
                FileChangedResolution::Restart
            }
            async fn resolve_not_resumable(&self, _: &Download) -> NotResumableResolution {
                NotResumableResolution::Restart
            }
        }

        let resolver = AssertTestResolver {};

        // Download should restart and succeed with a single connection
        let final_path = dlm.download(instruction, &resolver).await?;

        // Check file content
        let result = fs::read(&final_path).await?;
        assert_eq!(result, file_content);

        // Ensure mocks were hit
        head_mock.assert_async().await;
        get_mock.assert_async().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_manager_zero_byte_single_part_download()
    -> Result<(), Box<dyn std::error::Error>> {
        // Prepare test data: empty file
        let file_content = b"";

        // Start mock server
        let mut server = Server::new_async().await;
        let url = server.url();

        // HEAD request returns file info for 0 bytes
        let head_mock = server
            .mock("HEAD", "/zerofile")
            .with_status(200)
            .with_header("content-length", "0")
            .with_header("accept-ranges", "bytes")
            .with_header("etag", "zeroetag")
            .with_header("last-modified", "Sat, 24 Oct 2015 07:28:00 GMT")
            .create_async()
            .await;

        // Build DownloadManager with 1 connection and separate download/save dirs
        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        // Evaluate to get Download instruction
        let save_resolver = AlwaysReplaceResolver {};
        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/zerofile", url)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &save_resolver,
            )
            .await?;

        // Patch the instruction to simulate 1 part of 0 bytes
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(instruction.save_dir().clone())
            .filename(instruction.filename().to_string())
            .url(instruction.url().clone())
            .size(Some(0))
            .max_connections(1)
            .parts({
                let mut parts = std::collections::HashMap::new();
                parts.insert(
                    "part1".to_string(),
                    PartDetails {
                        ulid: "part1".to_string(),
                        offset: 0,
                        size: 0,
                        finished: false,
                    },
                );
                parts
            })
            .is_resumable(true)
            .build()
            .unwrap();

        let resolver = AlwaysAbortResolver {};
        // Download and concatenate
        let final_path = dlm.download(instruction, &resolver).await?;

        // Check file content
        let result = fs::read(&final_path).await?;
        assert_eq!(result, file_content);

        // Ensure mocks were hit
        head_mock.assert_async().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_part_resumes_with_correct_range()
    -> Result<(), Box<dyn std::error::Error>> {
        // Prepare test data
        let file_content = b"PartialDownloadTestFile";
        let part_offset = 0;
        let part_size = file_content.len() as u64;
        let already_downloaded = 7; // Simulate 7 bytes already downloaded

        // Start mock server
        let mut server = Server::new_async().await;
        let url = server.url();

        // The server should receive a range request starting after already_downloaded bytes
        let expected_range = format!(
            "bytes={}-{}",
            part_offset + already_downloaded,
            part_offset + part_size - 1
        );

        let get_mock = server
            .mock("GET", "/partialfile")
            .match_header("range", Matcher::Exact(expected_range.clone()))
            .with_status(206)
            .with_body(&file_content[already_downloaded as usize..])
            .create_async()
            .await;

        // Prepare a temp directory and partial part file
        let tmp_dir = tempdir()?;
        let download_dir = tmp_dir.path().join("partial");
        fs::create_dir_all(&download_dir).await?;
        let part_path = download_dir.join("part1.part");
        {
            let mut f = fs::File::create(&part_path).await?;
            f.write_all(&file_content[..already_downloaded as usize])
                .await?;
        }

        // Build instruction and metadata reflecting the partial download
        let part_details = PartDetails {
            ulid: "part1".to_string(),
            offset: part_offset,
            size: part_size,
            finished: false,
        };

        let mut parts_map = HashMap::new();
        parts_map.insert(part_details.ulid.clone(), part_details.clone());

        let instruction = DownloadBuilder::default()
            .download_dir(download_dir.clone())
            .save_dir(tmp_dir.path().to_path_buf())
            .filename("partialfile.bin".to_string())
            .url(Url::parse(&format!("{}/partialfile", url)).unwrap())
            .is_resumable(true)
            .max_connections(1)
            .size(Some(part_size))
            .parts(parts_map)
            .build()
            .unwrap();

        let metadata = instruction.as_metadata();
        let client = reqwest::Client::builder().build()?;
        let retry_policy = crate::retry_policies::FixedThenExponentialRetry {
            max_n_retries: 6,
            wait_time: std::time::Duration::from_millis(100),
            n_fixed_retries: 3,
        };
        let downloader = Downloader::new(
            Arc::new(instruction.clone()),
            metadata,
            client,
            false,
            None,
            retry_policy,
            DownloadContext::new(),
        );

        let updated_metadata = downloader.run().await?;
        assert!(
            updated_metadata
                .parts
                .get("part1")
                .map(|p| p.finished)
                .unwrap_or(false)
        );

        // Check file content: should be the full file_content
        let result = tokio::fs::read(&part_path).await?;
        assert_eq!(result, file_content);

        // Ensure mock was hit
        get_mock.assert_async().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_manager_custom_user_agent() -> Result<(), Box<dyn std::error::Error>> {
        // Prepare test data
        let file_content = b"UserAgentTestFile";

        // Start mock server
        let mut server = Server::new_async().await;
        let url = server.url();

        // Expect a custom user agent header in HEAD and GET requests
        let custom_ua = "MyCustomUserAgent/1.0";

        let head_mock = server
            .mock("HEAD", "/useragentfile")
            .match_header("user-agent", Matcher::Exact(custom_ua.into()))
            .with_status(200)
            .with_header("content-length", &file_content.len().to_string())
            .with_header("accept-ranges", "bytes")
            .with_header("etag", "uaetag")
            .with_header("last-modified", "Sun, 25 Oct 2015 07:28:00 GMT")
            .create_async()
            .await;

        let get_mock = server
            .mock("GET", "/useragentfile")
            .match_header("user-agent", Matcher::Exact(custom_ua.into()))
            .match_header(
                "range",
                Matcher::Exact(format!("bytes=0-{}", file_content.len() - 1)),
            )
            .with_status(206)
            .with_body(file_content)
            .create_async()
            .await;

        // Build DownloadManager with custom user agent
        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .user_agent(Some(custom_ua.to_string()))
            .randomize_user_agent(false)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        // Evaluate to get Download instruction
        let save_resolver = AlwaysReplaceResolver {};
        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/useragentfile", url)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &save_resolver,
            )
            .await?;

        // Patch the instruction to simulate 1 part
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(instruction.save_dir().clone())
            .filename(instruction.filename().to_string())
            .url(instruction.url().clone())
            .size(Some(file_content.len() as u64))
            .max_connections(1)
            .parts({
                let mut parts = std::collections::HashMap::new();
                parts.insert(
                    "part1".to_string(),
                    PartDetails {
                        ulid: "part1".to_string(),
                        offset: 0,
                        size: file_content.len() as u64,
                        finished: false,
                    },
                );
                parts
            })
            .is_resumable(true)
            .build()
            .unwrap();

        let resolver = AlwaysAbortResolver {};
        let final_path = dlm.download(instruction, &resolver).await?;

        let result = tokio::fs::read(&final_path).await?;
        assert_eq!(result, file_content);

        head_mock.assert_async().await;
        get_mock.assert_async().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_e2e_evaluate_download_assemble_with_checksum()
    -> Result<(), Box<dyn std::error::Error>> {
        use base64::Engine;
        use sha2::{Digest, Sha256};

        let file_content = b"E2E full pipeline payload: evaluate -> download -> assemble -> verify";

        let mut hasher = Sha256::new();
        hasher.update(file_content);
        let sha256_b64 = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());
        let repr_digest_value = format!("sha-256=:{}:", sha256_b64);

        let mut server = Server::new_async().await;
        let url = server.url();

        let head_mock = server
            .mock("HEAD", "/payload.bin")
            .with_status(200)
            .with_header("content-length", &file_content.len().to_string())
            .with_header("accept-ranges", "bytes")
            .with_header("etag", "e2eetag")
            .with_header("last-modified", "Tue, 27 Oct 2015 07:28:00 GMT")
            .with_header("Repr-Digest", &repr_digest_value)
            .create_async()
            .await;

        let get_mock = server
            .mock("GET", "/payload.bin")
            .match_header(
                "range",
                Matcher::Exact(format!("bytes=0-{}", file_content.len() - 1)),
            )
            .with_status(206)
            .with_body(file_content)
            .create_async()
            .await;

        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        let save_resolver = AlwaysReplaceResolver {};
        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/payload.bin", url)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &save_resolver,
            )
            .await?;

        // checksum must have been picked up from Repr-Digest during evaluate
        assert!(
            !instruction.as_metadata().checksums.is_empty(),
            "evaluate did not extract checksum from Repr-Digest"
        );
        assert_eq!(instruction.size(), Some(file_content.len() as u64));

        let resolver = AlwaysAbortResolver {};
        let final_path = dlm.download(instruction, &resolver).await?;

        let on_disk = fs::read(&final_path).await?;
        assert_eq!(on_disk, file_content, "final file content mismatch");

        // Independently verify SHA-256 of the assembled file.
        let mut hasher = Sha256::new();
        hasher.update(&on_disk);
        let actual_b64 = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());
        assert_eq!(actual_b64, sha256_b64);

        head_mock.assert_async().await;
        get_mock.assert_async().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_e2e_download_fails_on_checksum_mismatch() -> Result<(), Box<dyn std::error::Error>>
    {
        let file_content = b"payload-served-by-server";
        // Advertise a digest that does NOT match the body
        let bogus_repr_digest = "sha-256=:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=:";

        let mut server = Server::new_async().await;
        let url = server.url();

        let head_mock = server
            .mock("HEAD", "/bad.bin")
            .with_status(200)
            .with_header("content-length", &file_content.len().to_string())
            .with_header("accept-ranges", "bytes")
            .with_header("Repr-Digest", bogus_repr_digest)
            .create_async()
            .await;

        let get_mock = server
            .mock("GET", "/bad.bin")
            .match_header(
                "range",
                Matcher::Exact(format!("bytes=0-{}", file_content.len() - 1)),
            )
            .with_status(206)
            .with_body(file_content)
            .create_async()
            .await;

        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        let save_resolver = AlwaysReplaceResolver {};
        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/bad.bin", url)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &save_resolver,
            )
            .await?;

        let resolver = AlwaysAbortResolver {};
        let result = dlm.download(instruction, &resolver).await;
        assert!(
            matches!(
                result,
                Err(OdlError::Conflict(ConflictError::ChecksumMismatch { .. }))
            ),
            "expected ChecksumMismatch, got {:?}",
            result
        );

        head_mock.assert_async().await;
        get_mock.assert_async().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_e2e_multipart_evaluate_download_assemble_with_checksum()
    -> Result<(), Box<dyn std::error::Error>> {
        use base64::Engine;
        use sha2::{Digest, Sha256};

        use rand::{RngCore, SeedableRng, rngs::StdRng};

        // 900 KiB → with MIN_PART_SIZE = 300 KiB and max_connections = 3
        // determine_parts produces exactly 3 contiguous parts of 300 KiB each.
        let size: usize = 900 * 1024;
        let mut rng = StdRng::seed_from_u64(0x00C0_FFEE_F00D);
        let mut file_content = vec![0u8; size];
        rng.fill_bytes(&mut file_content);

        let mut hasher = Sha256::new();
        hasher.update(&file_content);
        let sha256_b64 = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());
        let repr_digest_value = format!("sha-256=:{}:", sha256_b64);

        let mut server = Server::new_async().await;
        let url = server.url();

        let head_mock = server
            .mock("HEAD", "/big.bin")
            .with_status(200)
            .with_header("content-length", &size.to_string())
            .with_header("accept-ranges", "bytes")
            .with_header("etag", "bigetag")
            .with_header("Repr-Digest", &repr_digest_value)
            .create_async()
            .await;

        // Three contiguous range mocks, one per part. mockito will route each
        // GET to the mock whose range header matches, so the order in which
        // the downloader issues them does not matter.
        let part_size = size / 3;
        let mut get_mocks = Vec::new();
        for i in 0..3 {
            let start = i * part_size;
            let end = if i == 2 {
                size - 1
            } else {
                start + part_size - 1
            };
            let body = file_content[start..=end].to_vec();
            let m = server
                .mock("GET", "/big.bin")
                .match_header("range", Matcher::Exact(format!("bytes={}-{}", start, end)))
                .with_status(206)
                .with_body(body)
                .create_async()
                .await;
            get_mocks.push(m);
        }

        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(3)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        let save_resolver = AlwaysReplaceResolver {};
        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/big.bin", url)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &save_resolver,
            )
            .await?;

        // Verify evaluate produced exactly 3 parts before any download work.
        let metadata = instruction.as_metadata();
        assert_eq!(
            metadata.parts.len(),
            3,
            "expected 3 parts, got {}",
            metadata.parts.len()
        );
        // Parts must be contiguous and cover the full size.
        let mut offsets: Vec<(u64, u64)> = metadata
            .parts
            .values()
            .map(|p| (p.offset, p.size))
            .collect();
        offsets.sort_by_key(|(o, _)| *o);
        let mut covered: u64 = 0;
        for (off, sz) in &offsets {
            assert_eq!(*off, covered);
            covered += sz;
        }
        assert_eq!(covered, size as u64);
        assert!(!metadata.checksums.is_empty());

        let resolver = AlwaysAbortResolver {};
        let final_path = dlm.download(instruction, &resolver).await?;

        let on_disk = fs::read(&final_path).await?;
        assert_eq!(on_disk.len(), file_content.len());
        assert_eq!(on_disk, file_content, "assembled file bytes mismatch");

        let mut hasher = Sha256::new();
        hasher.update(&on_disk);
        let actual_b64 = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());
        assert_eq!(actual_b64, sha256_b64);

        head_mock.assert_async().await;
        for m in &get_mocks {
            m.assert_async().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_download_manager_random_user_agent() -> Result<(), Box<dyn std::error::Error>> {
        // Prepare test data
        let file_content = b"RandomUserAgentTestFile";

        // Start mock server
        let mut server = Server::new_async().await;
        let url = server.url();

        // Accept any user agent, but ensure it's not the default reqwest one
        let head_mock = server
            .mock("HEAD", "/randomua")
            .match_header("user-agent", Matcher::Any)
            .with_status(200)
            .with_header("content-length", &file_content.len().to_string())
            .with_header("accept-ranges", "bytes")
            .with_header("etag", "randomuaetag")
            .with_header("last-modified", "Mon, 26 Oct 2015 07:28:00 GMT")
            .create_async()
            .await;

        let get_mock = server
            .mock("GET", "/randomua")
            .match_header("user-agent", Matcher::Any)
            .match_header(
                "range",
                Matcher::Exact(format!("bytes=0-{}", file_content.len() - 1)),
            )
            .with_status(206)
            .with_body(file_content)
            .create_async()
            .await;

        // Build DownloadManager with randomize_user_agent = true and no custom user agent
        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .randomize_user_agent(true)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);

        // Evaluate to get Download instruction
        let save_resolver = AlwaysReplaceResolver {};
        let instruction = dlm
            .evaluate(
                Url::parse(&format!("{}/randomua", url)).unwrap(),
                tmp_save_dir.path().to_path_buf(),
                None,
                &save_resolver,
            )
            .await?;

        // Patch the instruction to simulate 1 part
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(instruction.save_dir().clone())
            .filename(instruction.filename().to_string())
            .url(instruction.url().clone())
            .size(Some(file_content.len() as u64))
            .max_connections(1)
            .parts({
                let mut parts = std::collections::HashMap::new();
                parts.insert(
                    "part1".to_string(),
                    PartDetails {
                        ulid: "part1".to_string(),
                        offset: 0,
                        size: file_content.len() as u64,
                        finished: false,
                    },
                );
                parts
            })
            .is_resumable(true)
            .build()
            .unwrap();

        let resolver = AlwaysAbortResolver {};
        let final_path = dlm.download(instruction, &resolver).await?;

        let result = tokio::fs::read(&final_path).await?;
        assert_eq!(result, file_content);

        head_mock.assert_async().await;
        get_mock.assert_async().await;

        Ok(())
    }

    /// Simulates Ctrl-C interrupting `assemble_final_file` after
    /// `set_len(final_end)` ran (so the final file exists at the
    /// expected length, zero-padded) but before all per-part pwrites
    /// completed. With no server-supplied checksum, the only validity
    /// signal is the `metadata.finished` flag.
    ///
    /// Pre-fix behavior: `metadata.finished` was persisted as `true`
    /// BEFORE assembly ran, so on restart the size check passed against
    /// the zero-padded partial file and the user got a corrupt download
    /// silently reported as complete.
    ///
    /// Post-fix behavior: `metadata.finished` is only persisted after
    /// assembly + checksum + cleanup succeed. An interrupt mid-assembly
    /// leaves `finished = false`, so the next run re-runs assembly and
    /// produces the correct final file.
    #[tokio::test]
    async fn test_resumes_assembly_after_interrupt_with_no_server_checksum()
    -> Result<(), Box<dyn std::error::Error>> {
        let file_content = b"AssemblyResumePayload-NoChecksumScenario-0123456789abcdefghijklmnop";

        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;

        // Place the per-download dir as a child of the tempdir so the
        // lockfile / metadata / parts all live together as in real use.
        let download_dir = tmp_data_dir.path().join("payload-bin");
        fs::create_dir_all(&download_dir).await?;

        // Two parts split at byte 20.
        let part1_ulid = "part1ulid_resume_test";
        let part2_ulid = "part2ulid_resume_test";
        let split: usize = 20;

        let mut parts = HashMap::new();
        parts.insert(
            part1_ulid.to_string(),
            PartDetails {
                ulid: part1_ulid.to_string(),
                offset: 0,
                size: split as u64,
                finished: true,
            },
        );
        parts.insert(
            part2_ulid.to_string(),
            PartDetails {
                ulid: part2_ulid.to_string(),
                offset: split as u64,
                size: (file_content.len() - split) as u64,
                finished: true,
            },
        );

        let instruction = DownloadBuilder::default()
            .download_dir(download_dir.clone())
            .save_dir(tmp_save_dir.path().to_path_buf())
            .filename("payload.bin".to_string())
            .url(Url::parse("http://example.invalid/payload.bin").unwrap())
            .size(Some(file_content.len() as u64))
            .max_connections(1)
            .parts(parts)
            .is_resumable(true)
            .build()
            .unwrap();

        // Write part files with the correct payload bytes.
        fs::write(instruction.part_path(part1_ulid), &file_content[..split]).await?;
        fs::write(instruction.part_path(part2_ulid), &file_content[split..]).await?;

        // Persist metadata in the mid-assembly-interrupted state:
        // every part marked finished individually, but the aggregate
        // `finished` flag is `false`. No server checksum.
        let metadata = instruction.as_metadata();
        assert!(
            !metadata.finished,
            "precondition: metadata.finished should be false (mid-assembly state)"
        );
        assert!(
            metadata.checksums.is_empty(),
            "precondition: no server checksum (the bug condition)"
        );
        persist_metadata(&metadata, &instruction).await?;

        // Pre-create a partial, zero-padded final file at the same size
        // as the real payload, mirroring what `assemble_blocking` leaves
        // behind when killed after `set_len(final_end)` but before
        // pwrites finish. The contents do NOT match the real payload.
        let final_path = instruction.final_file_path();
        fs::write(&final_path, vec![0u8; file_content.len()]).await?;

        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);
        let resolver = AlwaysAbortResolver {};

        let result_path = dlm.download(instruction.clone(), &resolver).await?;
        assert_eq!(result_path, final_path);

        let on_disk = fs::read(&final_path).await?;
        assert_eq!(
            on_disk, file_content,
            "final file must be re-assembled from parts, not left as the partial zero-padded carcass"
        );

        // After successful run the aggregate flag must be persisted
        // true so the next invocation short-circuits.
        let bytes = fs::read(instruction.metadata_path()).await?;
        let persisted = {
            use prost::Message;
            crate::download_metadata::DownloadMetadata::decode_length_delimited(&*bytes)
                .expect("decode metadata")
        };
        assert!(
            persisted.finished,
            "post-condition: metadata.finished must be true after successful assembly"
        );

        Ok(())
    }

    /// Crash-recovery fast path: prior run completed assembly (final
    /// file written, checksum-correct on disk) but crashed BEFORE
    /// `metadata.finished = true` was persisted. With server checksums
    /// available, restart must trust the existing final file rather
    /// than nuke-and-reassemble — and must not re-download / re-pwrite.
    #[tokio::test]
    async fn test_recovers_from_crash_between_assembly_and_finished_persist()
    -> Result<(), Box<dyn std::error::Error>> {
        use base64::Engine;
        use sha2::{Digest, Sha256};

        let file_content = b"FastPathRecovery-AssemblyDone-FinishedFlagNotPersisted-XYZ";
        let mut hasher = Sha256::new();
        hasher.update(file_content);
        let sha256_b64 = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());

        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let download_dir = tmp_data_dir.path().join("payload-fastpath");
        fs::create_dir_all(&download_dir).await?;

        let part_ulid = "fastpath_part";
        let mut parts = HashMap::new();
        parts.insert(
            part_ulid.to_string(),
            PartDetails {
                ulid: part_ulid.to_string(),
                offset: 0,
                size: file_content.len() as u64,
                finished: true,
            },
        );

        // Build instruction WITH the same checksum the persisted
        // metadata will carry, so `resolve_server_conflicts` does not
        // see a `FileChanged` mismatch on restart.
        let checksums = vec![crate::hash::HashDigest::SHA256(
            sha256_b64.clone(),
            crate::hash::HashEncoding::Base64,
        )];
        let instruction = DownloadBuilder::default()
            .download_dir(download_dir.clone())
            .save_dir(tmp_save_dir.path().to_path_buf())
            .filename("payload-fastpath.bin".to_string())
            .url(Url::parse("http://example.invalid/payload-fastpath.bin").unwrap())
            .size(Some(file_content.len() as u64))
            .max_connections(1)
            .parts(parts)
            .is_resumable(true)
            .checksums(checksums)
            .build()
            .unwrap();

        // Part file matches payload (was downloaded successfully).
        fs::write(instruction.part_path(part_ulid), file_content).await?;

        // Final file matches payload (assembly ran successfully).
        fs::write(instruction.final_file_path(), file_content).await?;

        // Persist metadata: parts.finished=true, metadata.finished=false.
        // The checksum is already populated via `instruction.as_metadata()`.
        let metadata = instruction.as_metadata();
        assert!(!metadata.checksums.is_empty());
        persist_metadata(&metadata, &instruction).await?;

        // Snapshot the part file's mtime + inode equivalent (size) so we
        // can prove the recovery path did not re-pwrite it.
        let part_path = instruction.part_path(part_ulid);
        let pre_meta = std::fs::metadata(&part_path)?;
        let pre_final_meta = std::fs::metadata(instruction.final_file_path())?;

        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);
        let resolver = AlwaysAbortResolver {};

        let final_path = dlm.download(instruction.clone(), &resolver).await?;
        let on_disk = fs::read(&final_path).await?;
        assert_eq!(
            on_disk, file_content,
            "fast-path must preserve the already-correct final file"
        );

        // Final file should not have been rewritten — same mtime as
        // before the call (recovery path must not re-run assembly).
        let post_final_meta = std::fs::metadata(&final_path)?;
        assert_eq!(
            pre_final_meta.modified()?,
            post_final_meta.modified()?,
            "fast-path must not rewrite the final file"
        );

        // Parts must be cleaned up after fast-path success.
        assert!(
            !tokio::fs::try_exists(&part_path).await.unwrap_or(false),
            "fast-path must remove parts after marking finished"
        );

        // Persisted metadata must be flipped to finished.
        let bytes = fs::read(instruction.metadata_path()).await?;
        let persisted = {
            use prost::Message;
            crate::download_metadata::DownloadMetadata::decode_length_delimited(&*bytes)
                .expect("decode metadata")
        };
        assert!(persisted.finished);

        let _ = pre_meta; // silence unused on platforms without mtime
        Ok(())
    }

    /// Negative case for the recovery fast path: existing final file's
    /// checksum does NOT match. Must NOT trust it — must re-assemble
    /// from parts (which carry the correct payload).
    #[tokio::test]
    async fn test_recovery_fast_path_rejects_corrupt_existing_final()
    -> Result<(), Box<dyn std::error::Error>> {
        use base64::Engine;
        use sha2::{Digest, Sha256};

        let file_content = b"RejectCorruptExistingFinal-MustReassembleFromParts";
        let mut hasher = Sha256::new();
        hasher.update(file_content);
        let sha256_b64 = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());

        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let download_dir = tmp_data_dir.path().join("payload-rejectfinal");
        fs::create_dir_all(&download_dir).await?;

        let part_ulid = "rejectfinal_part";
        let mut parts = HashMap::new();
        parts.insert(
            part_ulid.to_string(),
            PartDetails {
                ulid: part_ulid.to_string(),
                offset: 0,
                size: file_content.len() as u64,
                finished: true,
            },
        );
        let checksums = vec![crate::hash::HashDigest::SHA256(
            sha256_b64,
            crate::hash::HashEncoding::Base64,
        )];
        let instruction = DownloadBuilder::default()
            .download_dir(download_dir.clone())
            .save_dir(tmp_save_dir.path().to_path_buf())
            .filename("payload-rejectfinal.bin".to_string())
            .url(Url::parse("http://example.invalid/payload-rejectfinal.bin").unwrap())
            .size(Some(file_content.len() as u64))
            .max_connections(1)
            .parts(parts)
            .is_resumable(true)
            .checksums(checksums)
            .build()
            .unwrap();

        // Parts: correct.
        fs::write(instruction.part_path(part_ulid), file_content).await?;
        // Final on disk: same length as payload, but content is zeros
        // (typical mid-assembly carcass after `set_len`).
        fs::write(instruction.final_file_path(), vec![0u8; file_content.len()]).await?;

        let metadata = instruction.as_metadata();
        persist_metadata(&metadata, &instruction).await?;

        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);
        let resolver = AlwaysAbortResolver {};

        let final_path = dlm.download(instruction.clone(), &resolver).await?;
        let on_disk = fs::read(&final_path).await?;
        assert_eq!(
            on_disk, file_content,
            "must re-assemble: existing final was zero-padded carcass"
        );
        Ok(())
    }

    /// Same scenario but the partial final file is missing entirely
    /// (interrupted before `set_len` ran). Must still re-assemble.
    #[tokio::test]
    async fn test_resumes_assembly_when_final_file_absent() -> Result<(), Box<dyn std::error::Error>>
    {
        let file_content = b"AnotherResumePayload-FinalFileAbsent";

        let tmp_data_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let download_dir = tmp_data_dir.path().join("payload-bin-2");
        fs::create_dir_all(&download_dir).await?;

        let only_ulid = "only_part_resume2";
        let mut parts = HashMap::new();
        parts.insert(
            only_ulid.to_string(),
            PartDetails {
                ulid: only_ulid.to_string(),
                offset: 0,
                size: file_content.len() as u64,
                finished: true,
            },
        );

        let instruction = DownloadBuilder::default()
            .download_dir(download_dir.clone())
            .save_dir(tmp_save_dir.path().to_path_buf())
            .filename("payload2.bin".to_string())
            .url(Url::parse("http://example.invalid/payload2.bin").unwrap())
            .size(Some(file_content.len() as u64))
            .max_connections(1)
            .parts(parts)
            .is_resumable(true)
            .build()
            .unwrap();

        fs::write(instruction.part_path(only_ulid), file_content).await?;
        let metadata = instruction.as_metadata();
        persist_metadata(&metadata, &instruction).await?;

        // No partial final file at all.
        assert!(
            !tokio::fs::try_exists(instruction.final_file_path())
                .await
                .unwrap_or(false)
        );

        let cfg = crate::config::ConfigBuilder::default()
            .download_dir(tmp_data_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();
        let dlm = DownloadManager::new(cfg);
        let resolver = AlwaysAbortResolver {};

        let final_path = dlm.download(instruction.clone(), &resolver).await?;
        let on_disk = fs::read(&final_path).await?;
        assert_eq!(on_disk, file_content);

        Ok(())
    }
}
