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
pub enum ServerConflictResolution {
    /// Valid for all cases
    Abort,
    /// Valid for [ServerConflict::FileChanged], [ServerConflict::NotResumable]
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
pub enum SaveConflictResolution {
    /// Valid for all cases
    Abort,
    /// Valid for: [SaveConflict::SameDownloadExists], [SaveConflict::FinalFileExists]
    ReplaceAndContinue,
    /// Valid for: [SaveConflict::SameDownloadExists], [SaveConflict::FinalFileExists]
    AddNumberToNameAndContinue,
}

pub trait ServerConflictResolver: Send + Sync {
    fn resolve_server_conflict(&self, conflict: ServerConflict) -> ServerConflictResolution;
}

pub trait SaveConflictResolver: Send + Sync {
    fn resolve_save_conflict(&self, conflict: SaveConflict) -> SaveConflictResolution;
}
