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
