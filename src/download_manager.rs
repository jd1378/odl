mod checksum;
mod downloader;
mod io;
mod recover_metadata;
mod save_conflict;
mod server_conflict;

use std::{path::PathBuf, sync::Arc, time::Duration};

use derive_builder::Builder;
use fs2::FileExt;
use reqwest::{
    Client, Proxy, Url,
    header::{HeaderMap, USER_AGENT},
};

use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};
use tracing::Span;
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::download_manager::recover_metadata::recover_metadata;
use crate::download_manager::{downloader::Downloader, io::persist_metadata};
use crate::download_manager::{io::assemble_final_file, server_conflict::resolve_server_conflicts};
use crate::download_manager::{io::remove_all_parts, save_conflict::resolve_save_conflicts};
use crate::error::MetadataError;
use crate::response_info::ResponseInfo;
use crate::{
    conflict::{SaveConflictResolver, ServerConflictResolver},
    download_manager::io::sum_parts_on_disk,
};
use crate::{credentials::Credentials, user_agents::random_user_agent};
use crate::{
    download::Download,
    download_metadata::PartDetails,
    error::OdlError,
    fs_utils::{self},
};

#[derive(Builder, Debug)]
#[builder(build_fn(validate = "Self::validate", private, name = "private_build"))]
pub struct DownloadManager {
    /// Directory of where to keep files when downloading. This is where we keep track of our downloads and configuration.
    #[builder(default = fs_utils::get_odl_dir().unwrap_or_else(|| {
                let tmp_dir = std::path::PathBuf::from("/tmp/odl");
                std::fs::create_dir_all(&tmp_dir).ok();
                tmp_dir
            }))]
    download_dir: PathBuf,

    /// Max connections that download manager can make in parallel for a single file
    #[builder(default = 4)]
    max_connections: u64,

    /// The maximum number of files that the download manager can download in parallel.
    ///
    /// This controls the overall concurrency of downloads. For example, if set to 4, up to 4 files
    /// will be downloaded at the same time, regardless of how many connections are used for each file.
    ///
    /// Note: For controlling how many parts of a single file can be downloaded concurrently,
    /// see the `max_connections` option.
    #[builder(default = 3)]
    max_concurrent_downloads: usize,

    /// Number of maximum retries after which a download is considered failed. After third retry it increases exponentially.
    /// For example the time for max_retries=6 and wait_between_retries=500ms will be:
    /// 500ms, 500ms, 500ms, 1000ms, 2000ms, 4000ms
    #[builder(default = 3)]
    max_retries: u64,

    /// Amount of time to wait between retries. After third retry it increases exponentially.
    #[builder(default = Duration::from_millis(500))]
    wait_between_retries: Duration,

    /// Custom HTTP headers.
    #[builder(default = None)]
    headers: Option<HeaderMap>,

    /// Custom user agent. This option overrides `randomize_user_agent`
    #[builder(default = None)]
    user_agent: Option<String>,

    /// Randomize user agent for each request.
    #[builder(default = true)]
    randomize_user_agent: bool,

    /// Custom request Proxy to use for downloads
    #[builder(default = None)]
    proxy: Option<Proxy>,

    /// Whether to use the last-modified sent by server when saving the file
    #[builder(default = false)]
    use_server_time: bool,

    /// Should we accept invalid SSL certificates? Do not use unless you are absolutely sure of what you are doing.
    #[builder(default = false)]
    accept_invalid_certs: bool,

    /// Optional maximum aggregate download speed per download in bytes per second.
    #[builder(default)]
    download_speed_limit: Option<u64>,

    /// Optional timeout for connect phase of request
    #[builder(default)]
    connect_timeout: Option<Duration>,

    /// Semaphore to limit concurrent downloads (not exposed in builder)
    #[builder(setter(skip), default = "Arc::new(Semaphore::new(0))")]
    semaphore: Arc<Semaphore>,
}

impl DownloadManager {
    pub fn max_connections(self: &Self) -> u64 {
        return self.max_connections;
    }

    pub fn set_max_connections(self: &mut Self, value: u64) {
        self.max_connections = if value > 0 { value } else { 1 }
    }

    pub fn max_concurrent_downloads(self: &Self) -> usize {
        return self.max_concurrent_downloads;
    }

    pub fn set_max_concurrent_downloads(self: &mut Self, value: usize) {
        self.max_concurrent_downloads = if value > 0 { value } else { 1 }
    }

    pub fn max_retries(self: &Self) -> u64 {
        return self.max_retries;
    }

    pub fn set_max_retries(self: &mut Self, value: u64) {
        self.max_retries = value
    }

    pub fn wait_between_retries(self: &Self) -> Duration {
        return self.wait_between_retries;
    }

    pub fn set_wait_between_retries(self: &mut Self, value: Duration) {
        self.wait_between_retries = value;
    }

    pub fn user_agent(self: &Self) -> &Option<String> {
        return &self.user_agent;
    }

    pub fn set_user_agent(self: &mut Self, value: Option<String>) {
        self.user_agent = value
    }

    pub fn random_user_agent(self: &Self) -> bool {
        return self.randomize_user_agent;
    }

    pub fn set_random_user_agent(self: &mut Self, value: bool) {
        self.randomize_user_agent = value
    }

    pub fn proxy(self: &Self) -> &Option<Proxy> {
        return &self.proxy;
    }

    pub fn set_proxy(self: &mut Self, value: Option<Proxy>) {
        self.proxy = value
    }

    pub fn use_server_time(self: &Self) -> bool {
        return self.use_server_time;
    }

    pub fn set_use_server_time(self: &mut Self, value: bool) {
        self.use_server_time = value
    }

    pub fn accept_invalid_certs(self: &Self) -> bool {
        return self.accept_invalid_certs;
    }

    pub fn set_accept_invalid_certs(self: &mut Self, value: bool) {
        self.accept_invalid_certs = value
    }

    pub fn download_speed_limit(&self) -> Option<u64> {
        self.download_speed_limit
    }

    pub fn connect_timeout(&self) -> Option<Duration> {
        self.connect_timeout
    }

    pub fn set_connect_timeout(&mut self, value: Option<Duration>) {
        self.connect_timeout = value;
    }

    pub fn set_download_speed_limit(&mut self, value: Option<u64>) {
        self.download_speed_limit = value;
    }

    pub async fn evaluate<CR>(
        self: &Self,
        url: Url,
        save_dir: PathBuf,
        credentials: Option<Credentials>,
        conflict_resolver: &CR,
    ) -> Result<Download, OdlError>
    where
        CR: SaveConflictResolver,
    {
        let current_span = Span::current();
        current_span.pb_set_message("Evaluating");
        let client = self.get_client(None)?;

        let mut req = client
            .head(url)
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
        if !self.user_agent.is_some() && self.randomize_user_agent {
            req = req.header(USER_AGENT, random_user_agent());
        }

        let resp = req.send().await?;
        let info = ResponseInfo::from(resp);
        let instruction = Download::from_response_info(
            &self.download_dir,
            save_dir,
            info,
            self.max_connections,
            self.use_server_time,
            credentials,
            self.proxy.clone(),
            self.headers.clone(),
        );

        let instruction = resolve_save_conflicts(instruction, conflict_resolver).await?;

        current_span.pb_set_message(instruction.filename());
        if let Some(size) = instruction.size() {
            current_span.pb_set_length(size);
        } else {
            current_span.pb_set_length(0);
        }

        return Ok(instruction);
    }

    /// Immediately starts a download using the given instruction and conflict_resolver
    pub async fn download<CR>(
        self: &Self,
        instruction: Download,
        conflict_resolver: &CR,
    ) -> Result<PathBuf, OdlError>
    where
        CR: ServerConflictResolver,
    {
        // we want to know issues about directory creation very early.
        tokio::fs::create_dir_all(instruction.download_dir()).await?;

        match tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(instruction.lockfile_path())
            .await
        {
            Ok(f) => {
                let f = f.into_std().await;
                if let Err(_) = f.try_lock_exclusive() {
                    return Err(OdlError::MetadataError(MetadataError::LockfileInUse));
                }

                let result = self.process_download(instruction, conflict_resolver).await;
                let _ = FileExt::unlock(&f);
                result
            }
            Err(e) => {
                return Err(OdlError::StdIoError {
                    e,
                    extra_info: Some(format!(
                        "Failed to open lockfile for exclusive locking at {}",
                        instruction.lockfile_path().display(),
                    )),
                });
            }
        }
    }

    /// acquire a permit from this download manager's semaphore. Only up to `max_concurrent_downloads` are permitted at the same time.
    pub async fn acquire_download_permit(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        self.semaphore.acquire().await
    }

    fn get_client(self: &Self, instructions: Option<&Download>) -> Result<Client, OdlError> {
        let mut client = reqwest::Client::builder();
        if let Some(proxy) = &self.proxy {
            client = client.proxy(proxy.clone());
        }
        if let Some(headers) = &self.headers {
            client = client.default_headers(headers.clone());
        }
        if let Some(download) = instructions {
            if let Some(proxy) = download.proxy() {
                client = client.proxy(proxy.clone());
            }

            if let Some(headers) = download.headers() {
                client = client.default_headers(headers.clone());
            }
        }
        if self.accept_invalid_certs {
            client = client.danger_accept_invalid_certs(self.accept_invalid_certs)
        }
        if let Some(user_agent) = &self.user_agent {
            client = client.user_agent(user_agent.clone());
        }
        if let Some(timeout) = self.connect_timeout {
            client = client.connect_timeout(timeout);
        }
        Ok(client.build()?)
    }

    async fn process_download<CR>(
        self: &Self,
        instruction: Download,
        conflict_resolver: &CR,
    ) -> Result<PathBuf, OdlError>
    where
        CR: ServerConflictResolver,
    {
        // early directory creation check to fail fast
        tokio::fs::create_dir_all(instruction.save_dir()).await?;

        recover_metadata(&instruction).await?;

        let mut metadata = resolve_server_conflicts(&instruction, conflict_resolver).await?;

        if let Some(sum_of_parts_sizes) = sum_parts_on_disk(&instruction, &metadata).await {
            let current_span = Span::current();
            current_span.pb_set_position(sum_of_parts_sizes);
            current_span.pb_reset_eta();
        }

        // we skip over download parts if we already finished downloading
        if !metadata.finished {
            let to_download = metadata
                .parts
                .iter()
                .filter_map(|(_, p)| {
                    if !p.finished {
                        return Some(p.clone());
                    }
                    return None;
                })
                .collect::<Vec<PartDetails>>();

            if !to_download.is_empty() {
                let randomize_user_agent = if self.user_agent.is_some() {
                    false
                } else {
                    self.randomize_user_agent
                };

                let client = self.get_client(Some(&instruction))?;
                let downloader = Downloader::new(
                    Arc::new(instruction.clone()),
                    metadata,
                    client,
                    randomize_user_agent,
                    Span::current(),
                    self.download_speed_limit,
                );

                let mut mdata = downloader.run().await?;
                mdata.finished = true;
                persist_metadata(&mdata, &instruction).await?;
                metadata = mdata;
            }

            let final_path = assemble_final_file(&mut metadata, &instruction).await?;
            remove_all_parts(&instruction.download_dir()).await;
            Ok(final_path)
        } else {
            let final_path = instruction.final_file_path();
            if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
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

impl DownloadManagerBuilder {
    fn validate(&self) -> Result<(), DownloadManagerBuilderError> {
        if self
            .max_concurrent_downloads
            .is_some_and(|max| max <= 0 || max >= Semaphore::MAX_PERMITS)
        {
            return Err(DownloadManagerBuilderError::UninitializedField(
                "max_concurrent_downloads",
            ));
        }
        if let Some(max_connections) = self.max_connections {
            if max_connections == 0 {
                return Err(DownloadManagerBuilderError::UninitializedField(
                    "max_connections",
                ));
            }
        }
        if let Some(wait_between_retries) = self.wait_between_retries {
            if wait_between_retries == Duration::from_millis(0) {
                return Err(DownloadManagerBuilderError::UninitializedField(
                    "wait_between_retries",
                ));
            }
        }
        if let Some(Some(limit)) = self.download_speed_limit {
            if limit == 0 {
                return Err(DownloadManagerBuilderError::UninitializedField(
                    "download_speed_limit",
                ));
            }
        }
        if let Some(Some(timeout)) = self.connect_timeout {
            if timeout == Duration::from_millis(0) {
                return Err(DownloadManagerBuilderError::UninitializedField(
                    "request_timeout",
                ));
            }
        }
        Ok(())
    }

    pub fn build(&self) -> Result<DownloadManager, DownloadManagerBuilderError> {
        let result = self.private_build()?;
        result
            .semaphore
            .add_permits(self.max_concurrent_downloads.unwrap_or(3));
        Ok(result)
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
        let tmp_download_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let dlm = DownloadManagerBuilder::default()
            .download_dir(tmp_download_dir.path().to_path_buf())
            .max_connections(2)
            .build()
            .unwrap();

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
        let tmp_download_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let filename = "file_abort";
        let final_path = tmp_save_dir.path().join(filename);
        tokio::fs::write(&final_path, b"x").await?;

        let dlm = DownloadManagerBuilder::default()
            .download_dir(tmp_download_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();

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
        let tmp_download_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let filename = "file_add";
        let final_path = tmp_save_dir.path().join(filename);
        tokio::fs::write(&final_path, b"x").await?;

        let dlm = DownloadManagerBuilder::default()
            .download_dir(tmp_download_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();

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
        let tmp_download_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let dlm = DownloadManagerBuilder::default()
            .download_dir(tmp_download_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();

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
        let tmp_download_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let dlm = DownloadManagerBuilder::default()
            .download_dir(tmp_download_dir.path().to_path_buf())
            .max_connections(2)
            .build()
            .unwrap();

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
                assert!(true, "NotResumable conflict should be triggered");
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
        let tmp_download_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let dlm = DownloadManagerBuilder::default()
            .download_dir(tmp_download_dir.path().to_path_buf())
            .max_connections(2)
            .build()
            .unwrap();

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
                assert!(true, "NotResumable conflict should be triggered");
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
        let tmp_download_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let dlm = DownloadManagerBuilder::default()
            .download_dir(tmp_download_dir.path().to_path_buf())
            .max_connections(1)
            .build()
            .unwrap();

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
        let downloader = Downloader::new(
            Arc::new(instruction.clone()),
            metadata,
            client,
            false,
            Span::current(),
            None,
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
        let tmp_download_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let dlm = DownloadManagerBuilder::default()
            .download_dir(tmp_download_dir.path().to_path_buf())
            .max_connections(1)
            .user_agent(Some(custom_ua.to_string()))
            .randomize_user_agent(false)
            .build()
            .unwrap();

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
        let tmp_download_dir = tempfile::tempdir()?;
        let tmp_save_dir = tempfile::tempdir()?;
        let dlm = DownloadManagerBuilder::default()
            .download_dir(tmp_download_dir.path().to_path_buf())
            .max_connections(1)
            .randomize_user_agent(true)
            .build()
            .unwrap();

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
}
