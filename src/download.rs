use crate::{
    credentials::Credentials,
    download_metadata::{DownloadMetadata, FileChecksum, PartDetails},
    error::DownloadParseError,
    fs_utils,
    hash::HashDigest,
    response_info::ResponseInfo,
};
use chrono::{DateTime, Utc};
use derive_builder::{Builder, UninitializedFieldError};
use reqwest::{Proxy, Url};
use std::{
    collections::HashMap,
    path::{self, PathBuf},
};
use thiserror::Error;
use tokio::sync::Semaphore;
use ulid::Ulid;

/// Represents a download instruction
#[derive(Builder, Debug, Clone)]
#[builder(build_fn(validate = "Self::validate", error = "DownloadBuilderError"))]
pub struct Download {
    /// Download directory used to store a small metadata file and parts
    download_dir: path::PathBuf,
    /// URL of the file to download.
    url: Url,
    /// Whether the server supports using range requests (i.e., resumable downloads).
    #[builder(default = false)]
    is_resumable: bool,
    /// Should we use the last-modified value server has sent us for the final file ?
    #[builder(default = false)]
    use_server_time: bool,
    /// The final file name to use to save on disk.
    filename: String,
    /// Where to save the final file
    save_dir: path::PathBuf,
    /// File size reported by the server in bytes. can be unknown until download is actually finished.
    #[builder(default = None)]
    size: Option<u64>,
    /// File size reported by the server in bytes. can be unknown until download is actually finished.
    #[builder(default = Vec::new())]
    checksums: Vec<HashDigest>,
    /// the e-tag the server has sent us, if any
    #[builder(default = None)]
    etag: Option<String>,
    /// the last-modified value the server has sent us, if any
    #[builder(default = None)]
    last_modified: Option<i64>,
    /// did the server ask us to authenticate?
    #[builder(default = false)]
    requires_auth: bool,
    /// did the server ask us to authenticate using basic auth?
    #[builder(default = false)]
    requires_basic_auth: bool,
    /// username and password to use, when requires_auth is true and this is provided
    #[builder(default = None)]
    credentials: Option<Credentials>,
    /// proxy to use, if provided
    #[builder(default = None)]
    proxy: Option<Proxy>,
    /// Preferred number of connections for this download.
    /// This will determine the initial number of parts, which will not be decreased after determination,
    /// even if the max_connections is decreased.
    #[builder(default = 6)]
    max_connections: u64,
    /// The parts determined based on max_connections and size. if size is unknown, this will only contain one element
    parts: HashMap<String, PartDetails>,
    /// Is the download finished?
    #[builder(default = false)]
    finished: bool,
}

impl Download {
    // Getters for Download fields

    pub fn download_dir(&self) -> &path::PathBuf {
        &self.download_dir
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn is_resumable(&self) -> bool {
        self.is_resumable
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn save_dir(&self) -> &path::PathBuf {
        &self.save_dir
    }

    pub fn size(&self) -> Option<u64> {
        self.size
    }

    pub fn etag(&self) -> &Option<String> {
        return &self.etag;
    }

    pub fn last_modified(&self) -> Option<i64> {
        self.last_modified
    }

    pub fn last_modified_as_date(&self) -> Option<DateTime<Utc>> {
        self.last_modified
            .and_then(|x| chrono::DateTime::from_timestamp(x, 0))
    }

    pub fn requires_auth(&self) -> bool {
        self.requires_auth
    }

    pub fn requires_basic_auth(&self) -> bool {
        self.requires_basic_auth
    }

    pub fn credentials(&self) -> &Option<Credentials> {
        &self.credentials
    }

    pub fn proxy(&self) -> &Option<Proxy> {
        &self.proxy
    }

    pub fn max_connections(&self) -> u64 {
        self.max_connections
    }

    pub fn parts(&self) -> &HashMap<String, PartDetails> {
        &self.parts
    }

    pub fn finished(&self) -> bool {
        self.finished
    }

    pub fn from_metadata(
        download_dir: path::PathBuf,
        metadata: DownloadMetadata,
    ) -> Result<Download, DownloadParseError> {
        let url = Url::parse(&metadata.url).map_err(|e| DownloadParseError::InvalidUrl {
            message: e.to_string(),
        })?;

        Ok(Self {
            download_dir,
            url,
            is_resumable: metadata.is_resumable,
            use_server_time: metadata.use_server_time,
            filename: metadata.filename, // is cleaned up before its stored as metadata, by from_response
            save_dir: PathBuf::from(metadata.save_dir),
            etag: metadata.last_etag,
            last_modified: metadata.last_modified,
            size: metadata.size,
            checksums: metadata
                .checksums
                .into_iter()
                .map(|c| c.try_into())
                .collect::<Result<Vec<HashDigest>, _>>()
                .unwrap_or_else(|_| Vec::new()),
            credentials: None,
            requires_auth: metadata.requires_auth,
            requires_basic_auth: metadata.requires_basic_auth,
            proxy: None,
            max_connections: metadata.max_connections,
            parts: metadata.parts,
            finished: metadata.finished,
        })
    }

    pub fn as_metadata(&self) -> DownloadMetadata {
        DownloadMetadata {
            url: self.url.to_string(),
            filename: self.filename.clone(),
            save_dir: self.save_dir.to_string_lossy().into_owned(),
            is_resumable: self.is_resumable,
            use_server_time: self.use_server_time,
            last_modified: self.last_modified,
            last_etag: self.etag.clone(),
            size: self.size,
            checksums: self
                .checksums
                .iter()
                .map(|h| h.clone().into())
                .collect::<Vec<FileChecksum>>(),
            requires_auth: self.requires_auth,
            requires_basic_auth: self.requires_basic_auth,
            max_connections: self.max_connections,
            parts: self.parts.clone(),
            finished: self.finished,
        }
    }

    pub fn from_response_info(
        download_dir: &path::PathBuf,
        save_dir: path::PathBuf,
        response_info: ResponseInfo,
        max_connections: u64,
        use_server_time: bool,
        credentials: Option<Credentials>,
        proxy: Option<Proxy>,
    ) -> Download {
        let filename = fs_utils::cleanup_filename(response_info.extract_filename().as_str());
        Self {
            download_dir: download_dir.join(&filename),
            url: response_info.url().clone(),
            is_resumable: response_info.is_resumable(),
            use_server_time,
            filename,
            save_dir,
            etag: response_info.etag(),
            last_modified: response_info.parse_last_modified(),
            size: response_info.total_length(),
            checksums: response_info.extract_hashes(),
            credentials,
            requires_auth: response_info.requires_auth(),
            requires_basic_auth: response_info.requires_basic_auth(),
            proxy,
            max_connections,
            parts: Download::determine_parts(
                response_info.total_length(),
                if response_info.is_resumable() {
                    max_connections
                } else {
                    1
                },
            ),
            finished: false,
        }
    }

    pub fn determine_parts(
        size: Option<u64>,
        max_connections: u64,
    ) -> HashMap<String, PartDetails> {
        let mut parts = HashMap::new();

        let max_connections = if max_connections > 0 {
            max_connections
        } else {
            1
        };

        let size = match size {
            Some(s) => s,
            None => 0,
        };

        const MIN_PART_SIZE: u64 = 300 * 1024; // 300 KB

        // Always return at least one part, even if size is 0
        if size <= MIN_PART_SIZE {
            let ulid = Ulid::new().to_string();
            parts.insert(
                ulid.clone(),
                PartDetails {
                    offset: 0,
                    size,
                    ulid,
                    finished: if size == 0 { true } else { false },
                },
            );
            return parts;
        }

        let mut actual_connections = max_connections;
        let min_connections = (size + MIN_PART_SIZE - 1) / MIN_PART_SIZE;
        if actual_connections > min_connections {
            actual_connections = min_connections;
        }

        let base_size = size / actual_connections;
        let remainder = size % actual_connections;
        let mut offset = 0;

        for i in 0..actual_connections {
            let part_size = if i == 0 {
                base_size + remainder
            } else {
                base_size
            };
            let ulid = Ulid::new().to_string();
            parts.insert(
                ulid.clone(),
                PartDetails {
                    offset,
                    size: part_size,
                    ulid,
                    finished: false,
                },
            );
            offset += part_size;
        }

        parts
    }
}

impl PartialEq for Download {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url
            && self.download_dir == other.download_dir
            && self.filename == other.filename
    }
}

impl DownloadBuilder {
    fn validate(&self) -> Result<(), DownloadBuilderError> {
        if self.download_dir.is_none() {
            return Err(DownloadBuilderError::MissingDownloadDir);
        }
        if self.url.is_none() {
            return Err(DownloadBuilderError::MissingUrl);
        }
        if self.filename.is_none() {
            return Err(DownloadBuilderError::MissingFilename);
        }
        if self
            .max_connections
            .is_none_or(|x| x <= 0 || x >= Semaphore::MAX_PERMITS.try_into().unwrap_or(1_000_000))
        {
            return Err(DownloadBuilderError::InvalidNumConnections);
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum DownloadBuilderError {
    #[error("download_dir is required")]
    MissingDownloadDir,
    #[error("url is required")]
    MissingUrl,
    #[error("filename is required")]
    MissingFilename,
    #[error("max_connections must be at least 1")]
    InvalidNumConnections,
    /// Uninitialized field
    #[error("uninitialized field: {0}")]
    UninitializedField(String),
    /// Custom validation error
    #[error("validation error: {0}")]
    ValidationError(String),
}

impl From<String> for DownloadBuilderError {
    fn from(s: String) -> Self {
        Self::ValidationError(s)
    }
}

impl From<UninitializedFieldError> for DownloadBuilderError {
    fn from(ufe: UninitializedFieldError) -> Self {
        Self::UninitializedField(ufe.to_string())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_determine_parts_zero_size() {
        let parts = Download::determine_parts(Some(0), 4);
        assert_eq!(parts.len(), 1);
        let part_vec: Vec<_> = parts.values().collect();
        let part = part_vec[0];
        assert_eq!(part.offset, 0);
        assert_eq!(part.size, 0);
        assert_eq!(part.finished, true);
    }

    #[test]
    fn test_determine_parts_zero_connections() {
        let parts = Download::determine_parts(Some(1024 * 1024), 0);
        assert_eq!(parts.len(), 1);
        let part_vec: Vec<_> = parts.values().collect();
        let part = part_vec[0];
        assert_eq!(part.offset, 0);
        assert_eq!(part.size, 1024 * 1024);
        assert_eq!(part.finished, false);
    }

    #[test]
    fn test_determine_parts_small_file() {
        // File smaller than MIN_PART_SIZE (300 KB)
        let size = 200 * 1024;
        let parts = Download::determine_parts(Some(size), 4);
        assert_eq!(parts.len(), 1);
        let part_vec: Vec<_> = parts.values().collect();
        let part = part_vec[0];
        assert_eq!(part.offset, 0);
        assert_eq!(part.size, size);
        assert_eq!(part.finished, false);
    }

    #[test]
    fn test_determine_parts_exact_min_part_size() {
        let size = 300 * 1024;
        let parts = Download::determine_parts(Some(size), 4);
        assert_eq!(parts.len(), 1);
        let part_vec: Vec<_> = parts.values().collect();
        let part = part_vec[0];
        assert_eq!(part.offset, 0);
        assert_eq!(part.size, size);
        assert_eq!(part.finished, false);
    }

    #[test]
    fn test_determine_parts_even_split() {
        // 1 MB file, 4 connections
        let size = 1024 * 1024;
        let max_connections = 4;
        let parts = Download::determine_parts(Some(size), max_connections);
        assert_eq!(parts.len(), max_connections as usize);
        let mut part_vec: Vec<_> = parts.values().collect();
        part_vec.sort_by_key(|p| p.offset);
        let total: u64 = part_vec.iter().map(|p| p.size).sum();
        assert_eq!(total, size);
        assert_eq!(part_vec[0].offset, 0);
        assert_eq!(part_vec[1].offset, part_vec[0].size);
        assert_eq!(part_vec[2].offset, part_vec[0].size + part_vec[1].size);
        assert_eq!(
            part_vec[3].offset,
            part_vec[0].size + part_vec[1].size + part_vec[2].size
        );
    }

    #[test]
    fn test_determine_parts_uneven_split() {
        // 1 MB + 123 bytes, 3 connections
        let size = 1024 * 1024 + 123;
        let max_connections = 3;
        let parts = Download::determine_parts(Some(size), max_connections);
        assert_eq!(parts.len(), max_connections as usize);
        let mut part_vec: Vec<_> = parts.values().collect();
        part_vec.sort_by_key(|p| p.offset);
        let total: u64 = part_vec.iter().map(|p| p.size).sum();
        assert_eq!(total, size);
        // The first part should get the remainder
        assert!(part_vec[0].size > part_vec[1].size);
    }

    #[test]
    fn test_determine_parts_too_many_connections() {
        // File size is such that min_connections < max_connections
        let size = 900 * 1024; // 900 KB
        let max_connections = 10;
        let parts = Download::determine_parts(Some(size), max_connections);
        // Should not exceed min_connections (3)
        assert_eq!(parts.len(), 3);
        let mut part_vec: Vec<_> = parts.values().collect();
        part_vec.sort_by_key(|p| p.offset);
        let total: u64 = part_vec.iter().map(|p| p.size).sum();
        assert_eq!(total, size);
    }

    #[test]
    fn test_determine_parts_800kb_file() {
        // 800 KB file, should be split into 3 parts (since MIN_PART_SIZE is 300 KB)
        let size = 800 * 1024;
        let max_connections = 10; // More than needed, should be capped by min_connections
        let parts = Download::determine_parts(Some(size), max_connections);
        // 800 KB / 300 KB = 2.66..., so should be 3 parts
        assert_eq!(parts.len(), 3);
        let mut part_vec: Vec<_> = parts.values().collect();
        part_vec.sort_by_key(|p| p.offset);
        let total: u64 = part_vec.iter().map(|p| p.size).sum();
        assert_eq!(total, size);

        // Check offsets are correct and contiguous
        assert_eq!(part_vec[0].offset, 0);
        assert_eq!(part_vec[1].offset, part_vec[0].offset + part_vec[0].size);
        assert_eq!(part_vec[2].offset, part_vec[1].offset + part_vec[1].size);

        // The first part should get the remainder
        assert!(part_vec[0].size >= part_vec[1].size);
        assert!(part_vec[1].size >= part_vec[2].size);
    }
}
