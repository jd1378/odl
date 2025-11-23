use async_trait::async_trait;

use crate::download::Download;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerConflict {
    /// File changed on server. Either of "size", "etag", "last-modified" headers have been changed
    FileChanged,
    /// File is the same probably but server does not support `range` requests
    NotResumable,
    /// The original url no longer works
    UrlBroken,
    /// The credentials provided are invalid
    CredentialsInvalid,
}

impl std::fmt::Display for ServerConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerConflict::FileChanged => write!(
                f,
                "file changed on server (size/etag/last-modified changed)"
            ),
            ServerConflict::NotResumable => {
                write!(f, "server does not support range requests (not resumable)")
            }
            ServerConflict::UrlBroken => write!(f, "original URL no longer works"),
            ServerConflict::CredentialsInvalid => write!(f, "credentials are missing or invalid"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileChangedResolution {
    Abort,
    Restart,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotResumableResolution {
    Abort,
    Restart,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaveConflict {
    /// Happens when a download is about to begin, but
    /// we find that we already have the same download structure
    /// already in place in download dir
    SameDownloadExists,
    /// Happens when a file already exists at the selected path for final concatenated file
    FinalFileExists,
}

impl std::fmt::Display for SaveConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaveConflict::SameDownloadExists => write!(
                f,
                "a download with the same metadata already exists in the download directory"
            ),
            SaveConflict::FinalFileExists => {
                write!(f, "a final file already exists at the target path")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SameDownloadExistsResolution {
    Abort,
    AddNumberToNameAndContinue,
    /// Try to resume the download.
    /// This can lead to a ServerConflict if file has been changed since the download was started.
    Resume,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinalFileExistsResolution {
    Abort,
    ReplaceAndContinue,
    AddNumberToNameAndContinue,
}

#[async_trait]
pub trait ServerConflictResolver: Send + Sync {
    async fn resolve_file_changed(&self, instruction: &Download) -> FileChangedResolution;
    async fn resolve_not_resumable(&self, instruction: &Download) -> NotResumableResolution;
}

#[async_trait]
pub trait SaveConflictResolver: Send + Sync {
    async fn same_download_exists(&self, instruction: &Download) -> SameDownloadExistsResolution;
    async fn final_file_exists(&self, instruction: &Download) -> FinalFileExistsResolution;
}
