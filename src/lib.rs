//! ODL — Open-source Download Library and CLI
//!
//! This crate provides a flexible, resumable, and configurable download manager
//! with a small CLI and library API. Intended for use as both a library and a
//! standalone binary. Public types and modules expose the high-level API used
//! by applications:
//!
//! - `Download` — primary download instruction type (create via `from_response_info` or
//!   `from_metadata`).
//! - `download_manager` — higher-level operations to evaluate and run downloads.
//! - `config` — persistent configuration for the manager.
//!
//! Example (library usage):
//!
//! ```no_run
//! use odl::{Download, download_manager::DownloadManager, config::Config};
//! // create a `DownloadManager` with default `Config` and call `evaluate`/`download`.
//! ```

pub mod config;
pub mod conflict;
pub mod credentials;
mod download;
pub mod download_manager;
pub mod error;
mod fs_utils;
mod hash;
mod response_info;
pub mod user_agents;

pub mod proto {
    pub mod download_metadata {
        include!(concat!(env!("OUT_DIR"), "/odl.download_metadata.rs"));
    }
    mod download_metadata_ext;
}

pub use download::Download;
pub use proto::download_metadata;
