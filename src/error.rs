use prost::DecodeError;
use std::error::Error;
use thiserror::Error;
use tokio::{sync::AcquireError, task::JoinError};

use crate::conflict::{SaveConflict, ServerConflict};

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Connect error")]
    Connect,
    #[error("Connection timeout")]
    Timeout,
    #[error("Response body error")]
    ResponseBody,
    #[error("Response status not success: {status_code:?}")]
    Status { status_code: u16 },
    #[error("Network error: {msg:?}")]
    Other { msg: String },
}

#[derive(Error, Debug)]
pub enum OdlError {
    #[error(transparent)]
    Network(#[from] NetworkError),
    #[error("The input file is empty")]
    EmptyInputFile,
    #[error("URL decode error: {message:?}")]
    UrlDecodeError { message: String },
    #[error("Standard I/O error: {e}")]
    StdIoError { e: std::io::Error },
    #[error("Task error: {e}")]
    TaskError { e: JoinError },
    #[error("Channel error: {e}")]
    ChannelError { e: async_channel::RecvError },
    #[error("CLI argument error: {message:?}")]
    CliArgumentError { message: String },
    #[error("CLI argument error: {e}")]
    ClapError { e: clap::Error },
    #[error("Program interrupted")]
    ProgramInterrupted,
    #[error("Unexpected error in odl related metadata: {message:?}")]
    MetadataError { message: String },
    #[error("Error while decoding metadata: {e}")]
    MetadataDecodeError { e: DecodeError },
    #[error("Error while acquiring lock for metadata")]
    LockfileInUse,
    #[error("Download aborted due to conflict: {conflict:?}")]
    DownloadAbortedDuetoConflict { conflict: ServerConflict },
    #[error("Download save aborted due to conflict: {conflict:?}")]
    DownloadSaveAbortedDuetoConflict { conflict: SaveConflict },
    #[error("Checksum mismatch: expected `{expected}`, got `{actual}`")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("Other error: {message:?}")]
    Other {
        message: String,
        origin: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl From<reqwest::Error> for OdlError {
    fn from(e: reqwest::Error) -> Self {
        if let Some(status) = e.status() {
            if !status.is_success() {
                return Self::Network(NetworkError::Status {
                    status_code: status.as_u16(),
                });
            }
        }
        if e.is_timeout() {
            return OdlError::Network(NetworkError::Timeout);
        }

        if e.is_body() {
            return OdlError::Network(NetworkError::ResponseBody);
        }

        if e.is_connect() {
            return OdlError::Network(NetworkError::Connect);
        }

        if let Some(io_err) = e.source().and_then(|s| s.downcast_ref::<std::io::Error>()) {
            if io_err.kind() == std::io::ErrorKind::TimedOut {
                return OdlError::Network(NetworkError::Timeout);
            }
        }

        Self::Network(NetworkError::Other { msg: e.to_string() })
    }
}

impl From<std::io::Error> for OdlError {
    fn from(e: std::io::Error) -> Self {
        Self::StdIoError { e }
    }
}

impl From<JoinError> for OdlError {
    fn from(e: JoinError) -> Self {
        Self::TaskError { e }
    }
}

impl From<async_channel::RecvError> for OdlError {
    fn from(e: async_channel::RecvError) -> Self {
        Self::ChannelError { e }
    }
}

impl From<clap::Error> for OdlError {
    fn from(e: clap::Error) -> Self {
        Self::ClapError { e }
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

impl From<reqwest_middleware::Error> for OdlError {
    fn from(value: reqwest_middleware::Error) -> Self {
        match value {
            reqwest_middleware::Error::Middleware(error) => Self::Other {
                message: error.to_string(),
                origin: error.into_boxed_dyn_error(),
            },
            reqwest_middleware::Error::Reqwest(error) => OdlError::from(error),
        }
    }
}

impl From<prost::DecodeError> for OdlError {
    fn from(e: prost::DecodeError) -> Self {
        OdlError::MetadataDecodeError { e }
    }
}

impl From<AcquireError> for OdlError {
    fn from(e: AcquireError) -> Self {
        OdlError::Other {
            message: "Failed to acquire permit from semaphore, this should not happen.".to_string(),
            origin: Box::new(e),
        }
    }
}

impl From<keyring::Error> for OdlError {
    fn from(e: keyring::Error) -> Self {
        OdlError::Other {
            message: e.to_string(),
            origin: Box::new(e),
        }
    }
}

#[derive(Error, Debug)]
pub enum DownloadParseError {
    #[error("Failed to parse url: {message:?}")]
    InvalidUrl { message: String },
    #[error("Failed to parse timestamp")]
    InvalidTimestamp,
}
