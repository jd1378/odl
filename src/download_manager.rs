use derive_builder::Builder;
use fs2::FileExt;
use futures::future::join_all;
use futures::stream::{FuturesOrdered, StreamExt};
use prost::Message;
use reqwest::{
    Proxy, Url,
    header::{HeaderMap, HeaderValue, RANGE},
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::RetryTransientMiddleware;
use std::path::Path;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio::{io::AsyncWriteExt, sync::Mutex};

use crate::conflict::{SaveConflictResolver, ServerConflictResolver};
use crate::credentials::Credentials;
use crate::fs_utils::{atomic_replace, set_file_mtime_async};
use crate::response_info::ResponseInfo;
use crate::{
    conflict::{SaveConflict, SaveConflictResolution, ServerConflict, ServerConflictResolution},
    download::Download,
    download_metadata::{DownloadMetadata, PartDetails},
    error::OdlError,
    fs_utils::{
        self, IsUnique, atomic_write, is_filename_unique, read_delimited_message_from_path,
    },
    retry_policies::FixedThenExponentialRetry,
};

#[derive(Builder, Debug)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct DownloadManager {
    /// Directory of where to keep files when downloading. This is where we keep track of our downloads.
    #[builder(default = fs_utils::get_odl_dir().unwrap_or_else(|| {
                let tmp_dir = std::path::PathBuf::from("/tmp/odl");
                std::fs::create_dir_all(&tmp_dir).ok();
                tmp_dir
            }))]
    download_dir: PathBuf,
    /// Number of maximum connections.
    #[builder(default = 6)]
    max_connections: u64,
    /// Number of maximum retries after which a download is considered failed.
    #[builder(default = 3)]
    max_retries: u32,
    /// Amount of time to wait between retries.
    #[builder(default = Duration::from_millis(500))]
    wait_between_retries: Duration,
    /// Custom HTTP headers.
    #[builder(default = None)]
    headers: Option<HeaderMap>,
    /// Custom request Proxy to use for downloads
    #[builder(default = None)]
    proxy: Option<Proxy>,
    /// Whether to use the last-modified sent by server when saving the file
    #[builder(default = false)]
    use_server_time: bool,
}

impl DownloadManager {
    pub fn wait_between_retries(self: &Self) -> Duration {
        return self.wait_between_retries;
    }

    pub fn set_wait_between_retries(self: &mut Self, value: Duration) {
        self.wait_between_retries = value;
    }

    pub fn max_connections(self: &Self) -> u64 {
        return self.max_connections;
    }

    pub fn set_max_connections(self: &mut Self, value: u64) {
        self.max_connections = if value > 0 { value } else { 1 }
    }

    pub fn proxy(self: &Self) -> &Option<Proxy> {
        return &self.proxy;
    }

    pub fn set_proxy(self: &mut Self, value: Option<Proxy>) {
        self.proxy = value
    }

    pub fn max_retries(self: &Self) -> u32 {
        return self.max_retries;
    }

    pub fn set_max_retries(self: &mut Self, value: u32) {
        self.max_retries = value
    }

    pub fn use_server_time(self: &Self) -> bool {
        return self.use_server_time;
    }

    pub fn set_use_server_time(self: &mut Self, value: bool) {
        self.use_server_time = value
    }

    pub async fn evaluate<CR>(
        self: &Self,
        url: Url,
        credentials: Option<Credentials>,
        conflict_resolver: &CR,
    ) -> Result<Download, OdlError>
    where
        CR: SaveConflictResolver,
    {
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

        let resp = req.send().await?;
        let info = ResponseInfo::from(resp);
        // TODO: fix save_dir based on detected file type category
        let instruction = Download::from_response_info(
            &self.download_dir,
            Path::new("./").to_path_buf(),
            info,
            self.max_connections,
            self.use_server_time,
            credentials,
            self.proxy.clone(),
        );

        let instruction =
            DownloadManager::resolve_save_conflicts(instruction, conflict_resolver).await?;

        return Ok(instruction);
    }

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
                    return Err(OdlError::LockfileInUse);
                }

                let result = self.process_download(instruction, conflict_resolver).await;
                let _ = FileExt::unlock(&f);
                result
            }
            Err(e) => {
                return Err(OdlError::StdIoError { e });
            }
        }
    }

    fn get_client(
        self: &Self,
        instructions: Option<&Download>,
    ) -> Result<ClientWithMiddleware, OdlError> {
        let retry_policy = FixedThenExponentialRetry {
            max_n_retries: self.max_retries,
            wait_time: self.wait_between_retries,
            n_fixed_retries: 3,
        };
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
        }

        Ok(ClientBuilder::new(client.build()?)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build())
    }

    /// Checks for common storage conflicts before the download begins
    async fn resolve_save_conflicts<CR>(
        mut instruction: Download,
        conflict_resolver: &CR,
    ) -> Result<Download, OdlError>
    where
        CR: SaveConflictResolver,
    {
        let mut conflict: Option<SaveConflict> = None;
        if tokio::fs::try_exists(instruction.download_dir())
            .await
            .unwrap_or(false)
        {
            conflict = Some(SaveConflict::SameDownloadExists);
        }

        let final_path = instruction.save_dir().join(instruction.filename());
        if conflict.is_none() {
            if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
                conflict = Some(SaveConflict::FinalFileExists);
            }
        }

        if let Some(conflict) = conflict {
            let resolution = conflict_resolver.resolve_save_conflict(conflict.clone());
            match resolution {
                SaveConflictResolution::Abort => {
                    return Err(OdlError::DownloadSaveAbortedDuetoConflict { conflict });
                }
                SaveConflictResolution::ReplaceAndContinue => {
                    match conflict {
                        SaveConflict::SameDownloadExists => {
                            if let Some(download_dir) = instruction.download_dir().to_str() {
                                if download_dir == "/" {
                                    return Err(OdlError::Other {
                                        message:
                                            "Refusing to remove root directory as download_dir"
                                                .to_string(),
                                        origin: Box::new(std::io::Error::new(
                                            std::io::ErrorKind::Other,
                                            "download_dir is root",
                                        )),
                                    });
                                }
                            }
                            // remove the metadata and parts, effectively replacing it
                            tokio::fs::remove_dir_all(instruction.download_dir()).await?;
                        }
                        SaveConflict::FinalFileExists => {
                            // Do nothing here, it will be truncated and written into at the end of download if we do nothing
                        }
                    }
                }
                SaveConflictResolution::AddNumberToNameAndContinue => {
                    // in either of cases we need to change the filename
                    // we just need to know which path to use to determine the new file name
                    let result = match conflict {
                        SaveConflict::SameDownloadExists => {
                            is_filename_unique(&instruction.download_dir()).await?
                        }
                        SaveConflict::FinalFileExists => is_filename_unique(&final_path).await?,
                    };

                    if let IsUnique::SuggestedAlternative(filename) = result {
                        instruction.set_filename(filename);
                    }
                }
            }
        }

        Ok(instruction)
    }

    /// Attempt to recover from an interrupted metadata write.
    async fn recover_metadata(instruction: &Download) -> Result<(), OdlError> {
        let metadata_temp_path = instruction.metadata_temp_path();

        match read_delimited_message_from_path::<DownloadMetadata, PathBuf>(&metadata_temp_path)
            .await
        {
            Ok(_) => {
                // If temp metadata is valid, atomically replace the main metadata file
                atomic_replace(metadata_temp_path, instruction.metadata_path()).await?;
            }
            Err(_) => {
                // If temp metadata is invalid or unreadable, remove it if it exists
                if tokio::fs::try_exists(&metadata_temp_path)
                    .await
                    .unwrap_or(false)
                {
                    // successful removal is important at this point
                    tokio::fs::remove_file(&metadata_temp_path).await?;
                }
            }
        }

        Ok(())
    }

    /// Checks for common conflicts between new instruction and metadata on disk
    /// and attemps to resolve them before the download starts.
    /// writes the updated metadata to disk and returns it
    async fn resolve_server_conflicts<CR>(
        instruction: &Download,
        conflict_resolver: &CR,
    ) -> Result<DownloadMetadata, OdlError>
    where
        CR: ServerConflictResolver,
    {
        let mut metadata: DownloadMetadata = match read_delimited_message_from_path::<
            DownloadMetadata,
            PathBuf,
        >(&instruction.metadata_path())
        .await
        {
            Ok(mut disk_metadata) => {
                // update disk_metadata from instruction
                disk_metadata.is_resumable = instruction.is_resumable();
                disk_metadata.filename = instruction.filename().to_string();
                disk_metadata.max_connections = instruction.max_connections();
                disk_metadata.requires_auth = instruction.requires_auth();
                disk_metadata.requires_basic_auth = instruction.requires_basic_auth();
                disk_metadata.use_server_time = instruction.use_server_time();
                disk_metadata.save_dir = instruction.save_dir().to_string_lossy().into_owned();
                disk_metadata
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(OdlError::StdIoError { e });
                }
                instruction.as_metadata()
            }
        };

        if !metadata.finished {
            // Do possible corruption checks between new download instructions and the metadata on disk
            let new_checksums = instruction.as_metadata().checksums;
            let mut conflict: Option<ServerConflict> = None;

            // Since resolution of either of issues is restarting the download, we just need to check one.
            if !metadata.is_resumable {
                conflict = Some(ServerConflict::NotResumable)
            } else if metadata.last_etag != *instruction.etag()
                || metadata.last_modified != instruction.last_modified()
                || metadata.size != instruction.size()
                || metadata.checksums != new_checksums
            {
                conflict = Some(ServerConflict::FileChanged);
            }

            if let Some(conflict) = conflict {
                let resolution = conflict_resolver.resolve_server_conflict(conflict.clone());
                if resolution == ServerConflictResolution::Abort {
                    return Err(OdlError::DownloadAbortedDuetoConflict { conflict });
                } else if resolution == ServerConflictResolution::Restart {
                    metadata.last_etag = instruction.etag().clone();
                    metadata.last_modified = instruction.last_modified();
                    metadata.size = instruction.size();
                    DownloadManager::remove_all_parts(instruction.download_dir()).await;
                    metadata.parts =
                        Download::determine_parts(metadata.size, metadata.max_connections);
                    metadata.checksums = new_checksums;
                }
            }
        }

        // write metadata changes back to disk, if any
        let encoded = metadata.encode_length_delimited_to_vec();
        atomic_write(
            instruction.metadata_path(),
            instruction.metadata_temp_path(),
            &encoded,
        )
        .await?;

        Ok(metadata)
    }

    async fn assemble_final_file(
        &self,
        metadata: &DownloadMetadata,
        instruction: &Download,
    ) -> Result<PathBuf, OdlError> {
        let final_path = instruction.save_dir().join(&metadata.filename);
        let mut final_file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&final_path)
            .await?;
        let mut sorted_parts: Vec<&PartDetails> = metadata.parts.values().collect();
        sorted_parts.sort_by_key(|p| p.offset);
        for p in sorted_parts.iter() {
            let part_path = instruction.part_path(&p.ulid);
            let mut part_file = tokio::fs::File::open(&part_path).await?;
            tokio::io::copy(&mut part_file, &mut final_file).await?;
        }

        if metadata.use_server_time {
            if let Some(last_modified) = metadata.last_modified {
                if let Err(e) = set_file_mtime_async(&final_path, last_modified).await {
                    tracing::error!(
                        "Failed to set file mtime for {}: {}",
                        final_path.display(),
                        e
                    );
                }
            }
        }

        return Ok(final_path);
    }

    async fn get_unfinished_parts_and_update_status(
        metadata: &mut DownloadMetadata,
        instruction: &Download,
    ) -> Result<Vec<PartDetails>, OdlError> {
        let mut to_download: Vec<PartDetails> = Vec::new();
        // Collect all unfinished part ulids
        let unfinished_ulids: Vec<String> = metadata
            .parts
            .iter()
            .filter_map(|(_, p)| {
                if !p.finished {
                    return Some(p.ulid.clone());
                }
                return None;
            })
            .collect();

        // get file stats in parallel
        let stats_futures = unfinished_ulids.into_iter().map(|ulid: String| {
            let part_path = instruction.part_path(&ulid);
            async move {
                let file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(part_path)
                    .await?;
                let metadata = file.metadata().await?;
                Ok::<(String, u64), std::io::Error>((ulid, metadata.len()))
            }
        });
        let stats_results = join_all(stats_futures).await;

        let mut changed = false;
        for stat_result in stats_results.into_iter() {
            match stat_result {
                Ok((ulid, size)) => {
                    if let Some(part) = metadata.parts.get_mut(&ulid) {
                        if size == part.size {
                            part.finished = true;
                            changed = true;
                        } else {
                            to_download.push(part.clone());
                        }
                    } else {
                        return Err(OdlError::MetadataError {
                            message: format!("Part with ulid {} not found in metadata", ulid),
                        });
                    }
                }
                Err(e) => {
                    return Err(OdlError::MetadataError {
                        message: format!("Failed to read size of part file: {}", e.to_string()),
                    });
                }
            }
        }

        if changed {
            let encoded = metadata.encode_length_delimited_to_vec();
            atomic_write(
                instruction.metadata_path(),
                instruction.metadata_temp_path(),
                &encoded,
            )
            .await?;
        }

        Ok(to_download)
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

        DownloadManager::recover_metadata(&instruction).await?;

        let mut metadata =
            DownloadManager::resolve_server_conflicts(&instruction, conflict_resolver).await?;

        // we skip over download parts if we already finished downloading
        if !metadata.finished {
            let to_download = DownloadManager::get_unfinished_parts_and_update_status(
                &mut metadata,
                &instruction,
            )
            .await?;

            // Download all parts: first part serial, rest in parallel if first succeeds
            // Downloads count should be according to max_connections of metadata
            if !to_download.is_empty() {
                // Mutex for safe access across threads
                // We need this because downloads can happen in parallel and may finish at any point in time
                // we want to safely write the metadata in case any of them finish at the same time
                let metadata_mutex = Arc::new(Mutex::new(metadata));

                // reqwest is thread-safe
                let client = Arc::new(self.get_client(Some(&instruction))?);

                // we will add permits once we confirm everything is okay on first download
                let semaphore = Arc::new(Semaphore::new(1));
                let mut first_iter = true;
                let first_permit = semaphore.acquire().await?;

                let mut futures = FuturesOrdered::new();

                for part in to_download.into_iter() {
                    let semaphore = semaphore.clone();
                    let metadata_mutex = Arc::clone(&metadata_mutex);
                    let url = instruction.url().clone();
                    let ulid = part.ulid.clone();
                    let part_path = instruction.part_path(&ulid);
                    let part_details = part;
                    let client: Arc<ClientWithMiddleware> = Arc::clone(&client);
                    let first_push = first_iter.clone();
                    first_iter = false;

                    futures.push_back(tokio::spawn(async move {
                        let _permit = if !first_push {
                            Some(semaphore.acquire().await?)
                        } else {
                            None
                        };
                        // Dummy progress callback, replace as needed
                        let progress_callback = |_downloaded: u64| {};
                        let started_callback = || {
                            if first_push {
                                let metadata = Arc::clone(&metadata_mutex);
                                let semaphore: Arc<Semaphore> = Arc::clone(&semaphore);
                                tokio::spawn(async move {
                                    let mdata = metadata.lock().await;
                                    if mdata.max_connections - 1 > 0 {
                                        semaphore.add_permits(
                                            mdata.max_connections.try_into().unwrap_or(1),
                                        );
                                    }
                                });
                            }
                        };
                        let res = DownloadManager::download_part(
                            &client,
                            &url,
                            Some(&part_details),
                            &part_path,
                            started_callback,
                            progress_callback,
                        )
                        .await;

                        // Mark part as finished and update metadata safely, but as soon as possible
                        if res.is_ok() {
                            let mut mdata = metadata_mutex.lock().await;
                            if let Some(part) = mdata.parts.get_mut(&ulid) {
                                part.finished = true;
                            } else {
                                return Err(OdlError::MetadataError {
                                    message: format!(
                                        "Part with ulid {} not found in metadata",
                                        ulid
                                    ),
                                });
                            }
                        }
                        drop(_permit);
                        res
                    }));
                }

                // Wait for first future that finishes
                // If it was not successful, close the semaphore and return the error
                if let Some(res) = futures.next().await {
                    if let Err(e) = res? {
                        // If the first download fails, prevent further downloads and return the error
                        semaphore.close();
                        return Err(e);
                    }
                }
                drop(first_permit);

                // Wait for all downloads to finish
                while let Some(result) = futures.next().await {
                    result??;
                }

                // Move metadata back from mutex to metadata variable
                let mut mdata = Arc::try_unwrap(metadata_mutex)
                    .map_err(|_| OdlError::MetadataError {
                        message: "Failed to unwrap Arc for metadata".to_string(),
                    })?
                    .into_inner();
                mdata.finished = true;
                let encoded = mdata.encode_length_delimited_to_vec();
                atomic_write(
                    instruction.metadata_path(),
                    instruction.metadata_temp_path(),
                    &encoded,
                )
                .await?;
                metadata = mdata;
            }
        }

        let final_path =
            DownloadManager::assemble_final_file(&self, &mut metadata, &instruction).await?;

        Self::remove_all_parts(&instruction.download_dir()).await;

        Ok(final_path)
    }

    async fn download_part<S, F>(
        client: &ClientWithMiddleware,
        url: &Url,
        part_details: Option<&PartDetails>,
        part_path: &PathBuf,
        started_callback: S,
        mut progress_callback: F,
    ) -> Result<(), OdlError>
    where
        S: FnOnce() + Send,
        F: FnMut(u64) + Send,
    {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(part_path)
            .await?;

        let mut req = client.get(url.clone());
        if let Some(part) = part_details {
            let range_header = format!("bytes={}-{}", part.offset, part.offset + part.size - 1);
            req = req.header(
                RANGE,
                HeaderValue::from_str(&range_header).map_err(|e| OdlError::Other {
                    message: "Internal Error: Invalid header value was used at download_part"
                        .to_string(),
                    origin: Box::new(e),
                })?,
            );
        }

        let mut resp = req.send().await.map_err(OdlError::from)?;

        let mut downloaded: u64 = 0;
        // Read the first chunk
        match resp.chunk().await.map_err(OdlError::from)? {
            Some(b) => {
                downloaded += b.len() as u64;
                file.write_all(&b).await?;
                progress_callback(downloaded);
                started_callback(); // Only called once, after first successful chunk
            }
            None => {
                started_callback(); // Not even sure if it's possible, but anyway
                return Ok(());
            }
        }

        // Read the rest of the chunks
        while let Some(b) = resp.chunk().await.map_err(OdlError::from)? {
            downloaded += b.len() as u64;
            file.write_all(&b).await?;
            progress_callback(downloaded);
        }

        file.flush().await?;

        Ok(())
    }

    async fn remove_all_parts(download_dir: &PathBuf) {
        // Remove all .part files in the download directory
        // Effectively resetting the download progress
        if let Ok(mut entries) = tokio::fs::read_dir(&download_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if ext == "part" {
                        let _ = tokio::fs::remove_file(&path).await;
                    }
                }
            }
        }
    }
}

impl DownloadManagerBuilder {
    fn validate(&self) -> Result<(), DownloadManagerBuilderError> {
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::DownloadBuilder;
    use crate::download_metadata::PartDetails;
    use mockito::Matcher;
    use mockito::Server;
    use std::collections::HashMap;
    use tokio::fs;

    struct AlwaysAbortResolver;
    impl ServerConflictResolver for AlwaysAbortResolver {
        fn resolve_server_conflict(&self, _: ServerConflict) -> ServerConflictResolution {
            ServerConflictResolution::Abort
        }
    }
    struct AlwaysReplaceResolver;
    impl SaveConflictResolver for AlwaysReplaceResolver {
        fn resolve_save_conflict(&self, _: SaveConflict) -> SaveConflictResolution {
            SaveConflictResolution::ReplaceAndContinue
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
                None,
                &save_resolver,
            )
            .await?;

        // Patch the instruction to simulate 2 parts
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(tmp_save_dir.path().to_path_buf())
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
                None,
                &save_resolver,
            )
            .await?;
        // Patch the instruction to simulate 1 part
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(tmp_save_dir.path().to_path_buf())
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
                None,
                &save_resolver,
            )
            .await?;

        // Patch the instruction to simulate 2 parts, but not resumable
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(tmp_save_dir.path().to_path_buf())
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
        impl ServerConflictResolver for AssertTestResolver {
            fn resolve_server_conflict(
                &self,
                conflict: ServerConflict,
            ) -> ServerConflictResolution {
                assert_eq!(conflict, ServerConflict::NotResumable);
                ServerConflictResolution::Abort
            }
        }

        let resolver = AssertTestResolver {};
        // Download should abort due to not resumable conflict
        let result = dlm.download(instruction, &resolver).await;

        assert!(matches!(
            result,
            Err(OdlError::DownloadAbortedDuetoConflict {
                conflict: ServerConflict::NotResumable
            })
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
                None,
                &save_resolver,
            )
            .await?;

        // Patch the instruction to simulate 2 parts, but not resumable
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(tmp_save_dir.path().to_path_buf())
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
        impl ServerConflictResolver for AssertTestResolver {
            fn resolve_server_conflict(
                &self,
                conflict: ServerConflict,
            ) -> ServerConflictResolution {
                assert_eq!(conflict, ServerConflict::NotResumable);
                ServerConflictResolution::Restart
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
                None,
                &save_resolver,
            )
            .await?;

        // Patch the instruction to simulate 1 part of 0 bytes
        let instruction = DownloadBuilder::default()
            .download_dir(instruction.download_dir().clone())
            .save_dir(tmp_save_dir.path().to_path_buf())
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
}
