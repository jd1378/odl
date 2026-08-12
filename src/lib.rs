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
//!
//! # Feature flags and process spawning
//!
//! The default feature set targets the `odl` binary: it pulls in the CLI and
//! the `ytdlp` engine, which delegates known media hosts to an externally
//! installed `yt-dlp`. That makes [`download_manager::DownloadManager::evaluate`]
//! able to **fork and exec a helper process**, which matters if you embed odl
//! somewhere that cannot or should not do that — a sandboxed desktop app
//! (macOS App Sandbox, Flatpak), a hardened server, or anywhere `evaluate`
//! is expected to cost one HTTP round-trip rather than a full extraction.
//!
//! Library consumers should therefore opt in deliberately:
//!
//! ```toml
//! # Pure library: no CLI dependencies, no engine that spawns anything.
//! odl = { version = "3", default-features = false }
//!
//! # Library plus media-site support.
//! odl = { version = "3", default-features = false, features = ["ytdlp"] }
//! ```
//!
//! Two runtime switches exist as well, for builds that do include the feature:
//! set `enabled = false` on [`config::YtdlpOptions`], or pass
//! [`engine::EnginePreference::Engine`] with the HTTP engine on an individual
//! request. Read the security notes on [`config::YtdlpOptions`] before
//! accepting a `Config` from anywhere but your own code.

pub mod config;
pub mod conflict;
pub mod credentials;
mod download;
pub mod download_manager;
pub mod engine;
pub mod error;
pub mod format;
mod fs_utils;
pub mod hash;
#[cfg(any(feature = "ytdlp", feature = "self-update"))]
mod http;
pub mod progress;
mod response_info;
mod retry_policies;
#[cfg(feature = "self-update")]
pub mod self_update;
pub mod user_agents;
#[cfg(feature = "ytdlp")]
pub mod ytdlp;

pub mod proto {
    // prost names a `oneof`'s module after the message that holds it, which
    // for `DownloadMetadata` repeats this module's own name.
    #[allow(clippy::module_inception)]
    pub mod download_metadata {
        include!(concat!(env!("OUT_DIR"), "/odl.download_metadata.rs"));

        // prost nests a `oneof`'s enum in a module named after its message,
        // giving `download_metadata::download_metadata::EngineDetails`.
        // Re-exported so the doubled path stays an implementation detail.
        pub use self::download_metadata::EngineDetails;
    }
    mod download_metadata_ext;
}

pub use download::{Download, YtdlpSpec};
pub use proto::download_metadata;
