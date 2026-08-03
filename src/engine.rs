//! Engine selection and the capability surface that distinguishes engines.
//!
//! A download's *engine* decides who moves the bytes. `http_multipart` is
//! odl's own downloader; `ytdlp` delegates the whole transfer to an
//! externally installed `yt-dlp`. Engines differ in what they can report —
//! yt-dlp never exposes HTTP response headers, for instance — so consumers
//! branch on [`EngineCapabilities`] rather than on the engine identity
//! itself. That keeps presentation code from growing a new arm every time
//! an engine is added.

use crate::download_metadata::DownloadEngine;
use std::fmt;
use std::str::FromStr;

/// What a given engine is able to report about a download.
///
/// Used to decide which fields are meaningful to display or persist. A
/// `false` here means "this engine cannot know", not "unknown for this
/// particular download" — so a UI can omit the field outright instead of
/// rendering an empty value that looks like a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct EngineCapabilities {
    /// Transfers the file over several concurrent connections, so
    /// `max_connections` and the per-part progress events are meaningful.
    pub multipart: bool,
    /// Can surface server-provided checksums for final-file verification.
    pub server_checksums: bool,
    /// Can surface the response headers observed while probing.
    pub response_headers: bool,
    /// Reports an exact byte size up front rather than an estimate.
    pub exact_size: bool,
    /// Produces more than one output file per download.
    pub multi_file: bool,
}

/// Extension methods on the persisted engine discriminant.
pub trait DownloadEngineExt {
    fn capabilities(&self) -> EngineCapabilities;
    /// Stable lowercase identifier, matching the proto enum value names.
    fn as_str(&self) -> &'static str;
}

impl DownloadEngineExt for DownloadEngine {
    fn capabilities(&self) -> EngineCapabilities {
        match self {
            DownloadEngine::HttpMultipart => EngineCapabilities {
                multipart: true,
                server_checksums: true,
                response_headers: true,
                exact_size: true,
                multi_file: false,
            },
            // yt-dlp owns the transfer end to end: it uses one connection per
            // format, never surfaces the underlying HTTP exchange, and reports
            // an estimated size for adaptive formats.
            DownloadEngine::Ytdlp => EngineCapabilities {
                multipart: false,
                server_checksums: false,
                response_headers: false,
                exact_size: false,
                multi_file: false,
            },
            // Nothing is known until the URL is evaluated, so nothing can be
            // promised. A UI should render such a row as pending rather than
            // as a download missing its details.
            DownloadEngine::Unresolved => EngineCapabilities {
                multipart: false,
                server_checksums: false,
                response_headers: false,
                exact_size: false,
                multi_file: false,
            },
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            DownloadEngine::HttpMultipart => "http_multipart",
            DownloadEngine::Ytdlp => "ytdlp",
            DownloadEngine::Unresolved => "unresolved",
        }
    }
}

/// Which engine a caller wants for a download.
///
/// [`EnginePreference::Auto`] is the default and defers to the delegation
/// rules (a curated host list plus tool availability). The explicit variants
/// let a caller force an engine and get a hard error instead of a silent
/// fallback when it is unusable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnginePreference {
    #[default]
    Auto,
    Engine(DownloadEngine),
}

impl EnginePreference {
    /// Engine explicitly requested by the caller, if any.
    pub fn forced(&self) -> Option<DownloadEngine> {
        match self {
            EnginePreference::Auto => None,
            EnginePreference::Engine(e) => Some(*e),
        }
    }
}

impl fmt::Display for EnginePreference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EnginePreference::Auto => f.write_str("auto"),
            EnginePreference::Engine(e) => f.write_str(e.as_str()),
        }
    }
}

impl FromStr for EnginePreference {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(EnginePreference::Auto),
            // `http` is accepted as the obvious short spelling of the default
            // engine; the canonical name stays `http_multipart`.
            "http" | "http_multipart" => {
                Ok(EnginePreference::Engine(DownloadEngine::HttpMultipart))
            }
            "ytdlp" | "yt-dlp" => Ok(EnginePreference::Engine(DownloadEngine::Ytdlp)),
            other => Err(format!(
                "unknown engine {other:?} (expected one of: auto, http, ytdlp)"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_proto_value_is_the_legacy_engine() {
        // Metadata written before the field existed decodes as 0. If this ever
        // changes, every pre-existing download on disk is misread.
        assert_eq!(
            DownloadEngine::try_from(0).unwrap(),
            DownloadEngine::HttpMultipart
        );
    }

    #[test]
    fn preference_parses_accepted_spellings() {
        assert_eq!(
            "auto".parse::<EnginePreference>().unwrap(),
            EnginePreference::Auto
        );
        assert_eq!(
            " HTTP ".parse::<EnginePreference>().unwrap(),
            EnginePreference::Engine(DownloadEngine::HttpMultipart)
        );
        assert_eq!(
            "yt-dlp".parse::<EnginePreference>().unwrap(),
            EnginePreference::Engine(DownloadEngine::Ytdlp)
        );
        assert!("bittorrent".parse::<EnginePreference>().is_err());
    }

    #[test]
    fn preference_round_trips_through_display() {
        for s in ["auto", "http_multipart", "ytdlp"] {
            let p: EnginePreference = s.parse().unwrap();
            assert_eq!(p.to_string(), s);
        }
    }

    #[test]
    fn delegated_engine_cannot_report_server_metadata() {
        let caps = DownloadEngine::Ytdlp.capabilities();
        assert!(!caps.server_checksums);
        assert!(!caps.response_headers);
        assert!(!caps.multipart);
        assert!(DownloadEngine::HttpMultipart.capabilities().multipart);
    }
}
