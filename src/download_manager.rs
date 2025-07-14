use derive_builder::Builder;
use fs2::FileExt;
use futures::stream::{FuturesOrdered, StreamExt};
use prost::Message;
use reqwest::{
    Client, Proxy, Url,
    header::{HeaderMap, HeaderValue, RANGE, USER_AGENT},
};
use std::{path::Path, path::PathBuf, sync::Arc, time::Duration};
use tokio::io::BufWriter;
use tokio::sync::Semaphore;
use tokio::{io::AsyncWriteExt, io::BufReader, sync::Mutex};

use crate::{
    conflict::SameDownloadExistsResolution, download_metadata::FileChecksum, hash::HashDigest,
};
use crate::{
    conflict::{FileChangedResolution, NotResumableResolution},
    response_info::ResponseInfo,
};
use crate::{
    conflict::{SaveConflict, ServerConflict},
    download::Download,
    download_metadata::{DownloadMetadata, PartDetails},
    error::OdlError,
    fs_utils::{
        self, IsUnique, atomic_write, is_filename_unique, read_delimited_message_from_path,
    },
    retry_policies::{FixedRetry, FixedThenExponentialRetry},
};
use crate::{
    conflict::{SaveConflictResolver, ServerConflictResolver},
    error::ConflictError,
};
use crate::{credentials::Credentials, user_agents::random_user_agent};
use crate::{
    error::MetadataError,
    fs_utils::{atomic_replace, set_file_mtime_async},
};

#[derive(Builder, Debug)]
#[builder(build_fn(validate = "Self::validate", private, name = "private_build"))]
pub struct DownloadManager {
    /// Directory of where to keep files when downloading. This is where we keep track of our downloads.
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
        if !self.user_agent.is_some() && self.randomize_user_agent {
            req = req.header(USER_AGENT, random_user_agent());
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
            self.headers.clone(),
        );

        let instruction = Self::resolve_save_conflicts(instruction, conflict_resolver).await?;

        return Ok(instruction);
    }

    /// Like `evaluate`, but acquires a permit to respect `max_concurrent_downloads` before evaluating.
    pub async fn evaluate_queued<CR>(
        &self,
        url: Url,
        credentials: Option<Credentials>,
        conflict_resolver: &CR,
    ) -> Result<Download, OdlError>
    where
        CR: SaveConflictResolver,
    {
        let permit = self.semaphore.acquire().await?;
        let result = self.evaluate(url, credentials, conflict_resolver).await;
        drop(permit);
        result
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
                return Err(OdlError::StdIoError { e });
            }
        }
    }

    /// Like `download`, but acquires a permit to respect `max_concurrent_downloads` before downloading.
    pub async fn download_queued<CR>(
        self: &Self,
        instruction: Download,
        conflict_resolver: &CR,
    ) -> Result<PathBuf, OdlError>
    where
        CR: ServerConflictResolver,
    {
        let permit = self.semaphore.acquire().await?;
        let result = self.download(instruction, conflict_resolver).await;
        drop(permit);
        result
    }

    /// Acquires a permit, evaluates the download, and immediately starts downloading.
    /// Returns the final file path if successful.
    pub async fn evaluate_and_download_queued<CR, SR>(
        &self,
        url: Url,
        credentials: Option<Credentials>,
        save_conflict_resolver: &CR,
        server_conflict_resolver: &SR,
    ) -> Result<PathBuf, OdlError>
    where
        CR: SaveConflictResolver,
        SR: ServerConflictResolver,
    {
        let permit = self.semaphore.acquire().await?;
        let instruction = self
            .evaluate(url, credentials, save_conflict_resolver)
            .await?;
        let result = self.download(instruction, server_conflict_resolver).await;
        drop(permit);
        result
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
        Ok(client.build()?)
    }

    /// Checks for common storage conflicts before the download begins
    async fn resolve_save_conflicts<CR>(
        mut instruction: Download,
        conflict_resolver: &CR,
    ) -> Result<Download, OdlError>
    where
        CR: SaveConflictResolver,
    {
        if tokio::fs::try_exists(instruction.download_dir())
            .await
            .unwrap_or(false)
        {
            let resolution: SameDownloadExistsResolution = SameDownloadExistsResolution::Resume;
            match resolution {
                SameDownloadExistsResolution::Abort => {
                    return Err(OdlError::Conflict(ConflictError::Save {
                        conflict: SaveConflict::SameDownloadExists,
                    }));
                }
                SameDownloadExistsResolution::AddNumberToNameAndContinue => {
                    let result = is_filename_unique(&instruction.download_dir()).await?;
                    if let IsUnique::SuggestedAlternative(filename) = result {
                        instruction.set_filename(filename);
                    }
                }
                SameDownloadExistsResolution::Resume => {
                    // do nothing and continue
                }
            }
        }

        let final_path = instruction.save_dir().join(instruction.filename());

        // This can still happen even if AddNumberToNameAndContinue solution is used previously
        // But it does not happen often in that case.
        if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
            match conflict_resolver.final_file_exists(&instruction).await {
                crate::conflict::FinalFileExistsResolution::Abort => {
                    return Err(OdlError::Conflict(ConflictError::Save {
                        conflict: SaveConflict::FinalFileExists,
                    }));
                }
                crate::conflict::FinalFileExistsResolution::ReplaceAndContinue => {
                    // We try to safely remove files, so just in case that
                    // the download_dir is not selected correctly, we don't end up
                    // deleting the wrong files.
                    let _ = tokio::fs::remove_file(instruction.metadata_path()).await;
                    let _ = tokio::fs::remove_file(instruction.metadata_temp_path()).await;
                    let mut entries = tokio::fs::read_dir(instruction.download_dir()).await?;
                    while let Some(entry) = entries.next_entry().await? {
                        let path = entry.path();
                        if let Some(ext) = path.extension() {
                            if ext == Download::PART_EXTENSION {
                                tokio::fs::remove_file(&path).await?;
                            }
                        }
                    }
                }
                crate::conflict::FinalFileExistsResolution::AddNumberToNameAndContinue => {
                    if let IsUnique::SuggestedAlternative(new_name) =
                        is_filename_unique(&final_path).await?
                    {
                        instruction.set_filename(new_name);
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

    async fn apply_restart_state_to_metadata(
        metadata: &mut DownloadMetadata,
        new_download: &Download,
        new_checksums: Vec<FileChecksum>,
    ) {
        metadata.last_etag = new_download.etag().to_owned();
        metadata.last_modified = new_download.last_modified();
        metadata.size = new_download.size();
        Self::remove_all_parts(new_download.download_dir()).await;
        metadata.parts = Download::determine_parts(metadata.size, metadata.max_connections);
        metadata.checksums = new_checksums;
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
                match conflict {
                    ServerConflict::FileChanged => {
                        match conflict_resolver.resolve_file_changed(&instruction).await {
                            FileChangedResolution::Abort => {
                                return Err(OdlError::Conflict(ConflictError::Server { conflict }));
                            }
                            FileChangedResolution::Restart => {
                                Self::apply_restart_state_to_metadata(
                                    &mut metadata,
                                    instruction,
                                    new_checksums,
                                )
                                .await
                            }
                        }
                    }
                    ServerConflict::NotResumable => {
                        match conflict_resolver.resolve_not_resumable(&instruction).await {
                            NotResumableResolution::Abort => {
                                return Err(OdlError::Conflict(ConflictError::Server { conflict }));
                            }
                            NotResumableResolution::Restart => {
                                Self::apply_restart_state_to_metadata(
                                    &mut metadata,
                                    instruction,
                                    new_checksums,
                                )
                                .await
                            }
                        }
                    }
                    ServerConflict::UrlBroken | ServerConflict::CredentialsInvalid => {
                        return Err(OdlError::Conflict(ConflictError::Server { conflict }));
                    }
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
        metadata: &DownloadMetadata,
        instruction: &Download,
    ) -> Result<PathBuf, OdlError> {
        let final_path = instruction.save_dir().join(&metadata.filename);
        let final_file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&final_path)
            .await?;
        let mut final_file = BufWriter::new(final_file);
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

        if !metadata.checksums.is_empty() {
            // Drop the mutable reference to final_file so we can re-open it for reading
            drop(final_file);
            for checksum in &metadata.checksums {
                let expected = HashDigest::try_from(checksum).map_err(|e| {
                    OdlError::MetadataError(MetadataError::Other {
                        message: format!("Invalid checksum in metadata: {}", e),
                    })
                })?;
                let file = tokio::fs::File::open(&final_path).await?;
                let reader = BufReader::new(file);
                let actual = HashDigest::from_reader(reader, &expected)
                    .await
                    .map_err(|e| OdlError::StdIoError { e })?;

                if actual != expected {
                    return Err(OdlError::Conflict(ConflictError::ChecksumMismatch {
                        expected: format!("{:?}", expected),
                        actual: format!("{:?}", actual),
                    }));
                }
            }
        }

        return Ok(final_path);
    }

    /// Attempts to download a list of parts
    async fn download_parts(
        &self,
        parts: Vec<PartDetails>,
        instruction: &Download,
        metadata: &Arc<Mutex<DownloadMetadata>>,
    ) -> Result<(), OdlError> {
        // reqwest is thread-safe, no need for mutex
        let client: Arc<Client> = Arc::new(self.get_client(Some(&instruction))?);
        let randomize_user_agent = if let Some(_) = self.user_agent {
            false
        } else {
            self.randomize_user_agent
        };
        // we will add permits once we confirm everything is okay on first download
        let semaphore = Arc::new(Semaphore::new(1));
        let mut first_iter = true;
        let first_permit = semaphore.acquire().await?;

        let mut futures = FuturesOrdered::new();

        for part in parts.into_iter() {
            let semaphore = semaphore.clone();
            let metadata = Arc::clone(&metadata);
            let url = instruction.url().clone();
            let ulid = part.ulid.clone();
            let part_path = instruction.part_path(&ulid);
            let part_details = part;
            let client: Arc<Client> = Arc::clone(&client);
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
                        let metadata = Arc::clone(&metadata);
                        let semaphore: Arc<Semaphore> = Arc::clone(&semaphore);
                        tokio::spawn(async move {
                            let mdata = metadata.lock().await;
                            if mdata.max_connections - 1 > 0 {
                                semaphore
                                    .add_permits(mdata.max_connections.try_into().unwrap_or(1));
                            }
                        });
                    }
                };
                let res = Self::download_part(
                    &client,
                    randomize_user_agent,
                    &url,
                    Some(&part_details),
                    &part_path,
                    started_callback,
                    progress_callback,
                )
                .await;

                // Mark part as finished and update metadata safely, but as soon as possible
                if res.is_ok() {
                    let mut mdata = metadata.lock().await;
                    if let Some(part) = mdata.parts.get_mut(&ulid) {
                        part.finished = true;
                    } else {
                        return Err(OdlError::MetadataError(MetadataError::Other {
                            message: format!("Part with ulid {} not found in metadata", ulid),
                        }));
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

        Ok(())
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

        Self::recover_metadata(&instruction).await?;

        let mut metadata = Self::resolve_server_conflicts(&instruction, conflict_resolver).await?;

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

            // Download all parts: first part serial, rest in parallel if first succeeds
            // Downloads count should be according to max_connections of metadata
            if !to_download.is_empty() {
                // Mutex for safe access across threads
                // We need this because downloads can happen in parallel and may finish at any point in time
                // we want to safely write the metadata in case any of them finish at the same time
                let metadata_mutex = Arc::new(Mutex::new(metadata)); // metadata is moved here until we put it back

                self.download_parts(to_download, &instruction, &metadata_mutex)
                    .await?;

                // Move metadata back from mutex to metadata variable
                let mut mdata = Arc::try_unwrap(metadata_mutex)
                    .map_err(|_| {
                        OdlError::MetadataError(MetadataError::Other {
                            message: "Failed to unwrap Arc for metadata".to_string(),
                        })
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

        let final_path = Self::assemble_final_file(&mut metadata, &instruction).await?;

        Self::remove_all_parts(&instruction.download_dir()).await;

        Ok(final_path)
    }

    /// Attempts to download a single part
    async fn download_part<S, F>(
        client: &Client,
        randomize_user_agent: bool,
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
        let current_size = match tokio::fs::metadata(part_path).await {
            Ok(meta) => meta.len(),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    0
                } else {
                    return Err(OdlError::StdIoError { e });
                }
            }
        };

        // we want to create the file anyway, it may be 0 bytes.
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(part_path)
            .await?;

        // If file already fully downloaded, skip
        if let Some(part) = part_details {
            if current_size >= part.size {
                return Ok(());
            }
        }

        let mut file = BufWriter::new(file);

        let mut req = client.get(url.clone());
        if let Some(part) = part_details {
            let range_header = format!(
                "bytes={}-{}",
                part.offset + current_size,
                part.offset + part.size - 1
            );
            req = req.header(
                RANGE,
                HeaderValue::from_str(&range_header).map_err(|e| OdlError::Other {
                    message: "Internal Error: Invalid header value was used at download_part"
                        .to_string(),
                    origin: Box::new(e),
                })?,
            );
        }
        if randomize_user_agent {
            req = req.header(USER_AGENT, random_user_agent())
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

    /// removes all .part files on disk
    async fn remove_all_parts(download_dir: &PathBuf) {
        // Remove all .part files in the download directory
        // Effectively resetting the download progress
        if let Ok(mut entries) = tokio::fs::read_dir(&download_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if ext == Download::PART_EXTENSION {
                        let _ = tokio::fs::remove_file(&path).await;
                    }
                }
            }
        }
    }
}

impl DownloadManagerBuilder {
    fn validate(&self) -> Result<(), DownloadManagerBuilderError> {
        if self.max_concurrent_downloads.is_none()
            || self
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
        Ok(())
    }

    pub fn build(&self) -> Result<DownloadManager, DownloadManagerBuilderError> {
        let result = self.private_build()?;
        result.semaphore.add_permits(
            self.max_concurrent_downloads
                .expect("max_concurrent_downloads cannot be None at this point"),
        );
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::DownloadBuilder;
    use crate::download_metadata::PartDetails;
    use async_trait::async_trait;
    use mockito::Matcher;
    use mockito::Server;
    use std::collections::HashMap;
    use tempfile::tempdir;
    use tokio::fs;

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

        // Prepare a temp file with already_downloaded bytes written
        let tmp_dir = tempdir()?;
        let part_path = tmp_dir.path().join("part1.part");
        {
            let mut f = tokio::fs::File::create(&part_path).await?;
            f.write_all(&file_content[..already_downloaded as usize])
                .await?;
        }

        // Build a minimal ClientWithMiddleware
        let client = reqwest::Client::builder().build()?;

        // Prepare PartDetails
        let part_details = PartDetails {
            ulid: "part1".to_string(),
            offset: part_offset,
            size: part_size,
            finished: false,
        };

        // Call download_part
        let url = Url::parse(&format!("{}/partialfile", url)).unwrap();
        let mut started_called = false;
        let mut progress_called = false;
        DownloadManager::download_part(
            &client,
            false,
            &url,
            Some(&part_details),
            &part_path,
            || {
                started_called = true;
            },
            |_downloaded| {
                progress_called = true;
            },
        )
        .await?;

        // Check file content: should be the full file_content
        let result = tokio::fs::read(&part_path).await?;
        assert_eq!(result, file_content);

        // Ensure mock was hit
        get_mock.assert_async().await;
        assert!(started_called, "started_callback should be called");
        assert!(progress_called, "progress_callback should be called");

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
