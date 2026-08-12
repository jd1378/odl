use prost::DecodeError;
use std::error::Error;
use thiserror::Error;
use tokio::{sync::AcquireError, task::JoinError};

use crate::{
    config::{ConfigBuilderError, DownloadOptionsBuilderError},
    conflict::{SaveConflict, ServerConflict},
};

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Connection error")]
    Connect,
    #[error("DNS resolution failed{}: {message}", host.as_deref().map(|h| format!(" for `{h}`")).unwrap_or_default())]
    Dns {
        host: Option<String>,
        message: String,
    },
    #[error("Connection timeout")]
    Timeout,
    #[error("Response body error")]
    ResponseBody,
    #[error("HTTP {status_code}{}{}",
        reason.as_deref().map(|r| format!(" {r}")).unwrap_or_default(),
        url.as_deref().map(|u| format!(" (url: {u})")).unwrap_or_default())]
    Status {
        status_code: u16,
        reason: Option<String>,
        url: Option<String>,
    },
    #[error("Network error: {message}")]
    Other { message: String },
}

#[derive(Error, Debug)]
pub enum ConflictError {
    #[error("Save conflict: {conflict}")]
    Save { conflict: SaveConflict },
    #[error("Server conflict: {conflict}")]
    Server { conflict: ServerConflict },
    #[error("Checksum mismatch: expected `{expected}`, got `{actual}`")]
    ChecksumMismatch { expected: String, actual: String },
}

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("metadata lock is held by another process (lockfile in use)")]
    LockfileInUse,
    #[error("failed to decode metadata: {e}")]
    MetadataDecodeError { e: DecodeError },
    #[error("metadata error: {message}")]
    Other { message: String },
}

/// Failures specific to delegating a download to an external `yt-dlp`.
///
/// Kept separate from [`NetworkError`] because these describe the local
/// toolchain — a missing, unusable, or failing helper program — which calls
/// for a different remedy than a network fault.
#[derive(Error, Debug)]
pub enum YtdlpError {
    #[error("yt-dlp was not found{}", .searched_path.as_deref().map(|p| format!(" (looked for `{p}`)")).unwrap_or_default())]
    NotFound { searched_path: Option<String> },
    #[error("yt-dlp at `{path}` is not usable: {message}")]
    NotUsable { path: String, message: String },
    #[error("yt-dlp at `{path}` is version {found}, but {required} or newer is required")]
    TooOld {
        path: String,
        found: String,
        required: String,
    },
    #[error("yt-dlp exited with {}{}",
        code.map(|c| format!("code {c}")).unwrap_or_else(|| "a signal".to_owned()),
        stderr.as_deref().map(|s| format!(": {s}")).unwrap_or_default())]
    ProcessFailed {
        code: Option<i32>,
        stderr: Option<String>,
    },
    #[error("yt-dlp does not support this URL")]
    UnsupportedUrl,
    #[error(
        "the site refused the request as too frequent (HTTP 429){}. Nothing is wrong with the download itself — wait and try again, or pass credentials with `cookies_from_browser` in the config. Some endpoints, subtitles especially, are limited far more tightly than the media itself, so a video may still download while its transcript will not",
        .detail.as_deref().map(|d| format!(": {d}")).unwrap_or_default()
    )]
    RateLimited { detail: Option<String> },
    #[error(
        "format {format_id} is no longer offered for this URL; the partial download was discarded, so running the same command again will pick a currently available format"
    )]
    FormatUnavailable { format_id: String },
    #[error("yt-dlp error: {message}")]
    Other { message: String },
}

#[derive(Error, Debug)]
// New engines bring new failure modes; leaving this open means adding one
// does not break every downstream `match`.
#[non_exhaustive]
pub enum OdlError {
    #[error(transparent)]
    Network(#[from] NetworkError),
    #[error(transparent)]
    Ytdlp(#[from] YtdlpError),
    #[error(transparent)]
    Conflict(#[from] ConflictError),
    #[error("The input file is empty")]
    EmptyInputFile,
    #[error("URL decode error: {message}")]
    UrlDecodeError { message: String },
    #[error("I/O error: {e} {extra_info:?}")]
    StdIoError {
        e: std::io::Error,
        extra_info: Option<String>,
    },
    #[error("CLI error: {message}")]
    CliError { message: String },
    #[error(
        "`{url}` has not been evaluated yet, so there is nothing to download; call `evaluate` first"
    )]
    NotEvaluated { url: String },
    /// A request that cannot be honoured as written.
    ///
    /// Distinct from [`Self::CliError`], which names a mistake on the command
    /// line: this one is reachable from the library API, where "CLI error"
    /// would be a confusing thing to be told.
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    #[error("download cancelled")]
    Cancelled,
    #[error(transparent)]
    ConfigBuilderError(#[from] ConfigBuilderError),
    #[error(transparent)]
    DownloadOptionsBuilderError(#[from] DownloadOptionsBuilderError),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("{message}")]
    Other {
        message: String,
        origin: Box<dyn std::error::Error + Send + Sync>,
    },
}

fn find_dns_message(e: &(dyn Error + 'static)) -> Option<String> {
    let mut matched = false;
    let mut leaf_msg: Option<String> = None;
    let mut src: Option<&(dyn Error + 'static)> = Some(e);
    while let Some(s) = src {
        let msg = s.to_string();
        let lower = msg.to_ascii_lowercase();
        let is_dns = lower.contains("dns error")
            || lower.contains("failed to lookup address")
            || lower.contains("name or service not known")
            || lower.contains("no such host")
            || lower.contains("nodename nor servname")
            || lower.contains("temporary failure in name resolution");
        if is_dns {
            matched = true;
        }
        // Track the deepest informative message (skip generic wrappers like
        // "dns error" with no further detail).
        if lower != "dns error" && !msg.is_empty() {
            leaf_msg = Some(msg);
        }
        src = s.source();
    }
    if matched {
        leaf_msg.or(Some("DNS lookup failed".to_string()))
    } else {
        None
    }
}

impl OdlError {
    /// Classify a transport failure into the network error it describes.
    ///
    /// Deliberately an inherent method rather than a `From` impl: a trait impl
    /// naming `reqwest::Error` would make reqwest part of odl's public API,
    /// binding every consumer to the same major version of it and turning any
    /// future reqwest upgrade into a breaking release of odl. Which HTTP
    /// client odl uses is nobody's business but odl's.
    pub(crate) fn from_reqwest(e: reqwest::Error) -> Self {
        if let Some(status) = e.status()
            && !status.is_success()
        {
            return Self::Network(NetworkError::Status {
                status_code: status.as_u16(),
                reason: status.canonical_reason().map(|s| s.to_string()),
                url: e.url().map(|u| u.to_string()),
            });
        }
        if e.is_timeout() {
            return OdlError::Network(NetworkError::Timeout);
        }

        if e.is_body() {
            return OdlError::Network(NetworkError::ResponseBody);
        }

        if e.is_connect() {
            if let Some(message) = find_dns_message(&e) {
                return OdlError::Network(NetworkError::Dns {
                    host: e.url().and_then(|u| u.host_str().map(|h| h.to_string())),
                    message,
                });
            }
            return OdlError::Network(NetworkError::Connect);
        }

        if let Some(io_err) = e.source().and_then(|s| s.downcast_ref::<std::io::Error>())
            && io_err.kind() == std::io::ErrorKind::TimedOut
        {
            return OdlError::Network(NetworkError::Timeout);
        }

        if let Some(message) = find_dns_message(&e) {
            return OdlError::Network(NetworkError::Dns {
                host: e.url().and_then(|u| u.host_str().map(|h| h.to_string())),
                message,
            });
        }

        Self::Network(NetworkError::Other {
            message: e.to_string(),
        })
    }
}

impl From<std::io::Error> for OdlError {
    fn from(e: std::io::Error) -> Self {
        Self::StdIoError {
            e,
            extra_info: None,
        }
    }
}

impl From<JoinError> for OdlError {
    fn from(e: JoinError) -> Self {
        Self::Other {
            message: e.to_string(),
            origin: Box::new(e),
        }
    }
}

impl From<crate::download::DownloadBuilderError> for OdlError {
    fn from(e: crate::download::DownloadBuilderError) -> Self {
        Self::Other {
            message: e.to_string(),
            origin: Box::new(e),
        }
    }
}

impl From<prost::DecodeError> for OdlError {
    fn from(e: prost::DecodeError) -> Self {
        OdlError::MetadataError(MetadataError::MetadataDecodeError { e })
    }
}

impl From<AcquireError> for OdlError {
    fn from(e: AcquireError) -> Self {
        OdlError::Other {
            message: "failed to acquire semaphore permit".to_string(),
            origin: Box::new(e),
        }
    }
}

#[derive(Error, Debug)]
pub enum DownloadParseError {
    #[error("Failed to parse URL: {message}")]
    InvalidUrl { message: String },
    #[error("Invalid timestamp")]
    InvalidTimestamp,
}
