use reqwest::Url;

use crate::credentials::Credentials;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Conflict {
    /// File changed on server. Either of "size", "etag", "last-modified" headers have been changed
    ServerFileChanged,
    /// File is the same probably but server does not support `range` requests
    NotResumable,
    /// The original url no longer works
    UrlBroken,
    /// The credentials provided are invalid
    CredentialsInvalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Valid for all cases
    Abort,
    /// Valid for [Conflict::ServerFileChanged], [Conflict::NotResumable]
    Restart,
    /// Valid for [Conflict::FileNotFound]
    RestartWithUrl { url: Url },
    /// valid for [Conflict::CredentialsInvalid]
    RetryWithCredentials { credentials: Credentials },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaveConflict {
    /// Happens when a download is about to begin, but
    /// we find that we already have the same download structure
    /// already in place in download dir and Unfinished
    SameDownloadPending,
    /// Happens when a download is about to begin, but
    /// we find that we already have the same download structure
    /// already in place in download dir and It's finished
    SameDownloadFinished,
    /// Happens when a file already exists at the selected path for final concatenated file
    FinalFileExists,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaveConflictResolution {
    Abort,
    /// Valid for: [SaveConflict::SameDownloadPending], [SaveConflict::SameDownloadFinished], [SaveConflict::FinalFileExists]
    ReplaceAndContinue,
    /// Valid for: [SaveConflict::SameDownloadPending], [SaveConflict::SameDownloadFinished], [SaveConflict::FinalFileExists]
    AddNumberToNameAndContinue,
    /// Valid for: [SaveConflict::SameDownloadFinished]
    ProceedToFinish,
}
