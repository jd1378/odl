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

/// Which engine moves a download's bytes.
///
/// Deliberately distinct from the persisted discriminant in
/// [`crate::download_metadata::DownloadEngine`], which is generated code and
/// therefore cannot be `non_exhaustive`. Exposing that type directly would
/// make every future engine a breaking change for anyone matching on it —
/// including exactly the consumers this enum exists to serve. Metadata still
/// stores the generated value; the conversion happens at that boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Engine {
    /// odl's own multipart HTTP downloader.
    HttpMultipart,
    /// The whole transfer delegated to an external `yt-dlp`.
    Ytdlp,
    /// Not evaluated yet, so which engine applies is still unknown.
    Unresolved,
}

impl From<Engine> for DownloadEngine {
    fn from(engine: Engine) -> Self {
        match engine {
            Engine::HttpMultipart => DownloadEngine::HttpMultipart,
            Engine::Ytdlp => DownloadEngine::Ytdlp,
            Engine::Unresolved => DownloadEngine::Unresolved,
        }
    }
}

impl From<DownloadEngine> for Engine {
    fn from(engine: DownloadEngine) -> Self {
        match engine {
            DownloadEngine::HttpMultipart => Engine::HttpMultipart,
            DownloadEngine::Ytdlp => Engine::Ytdlp,
            DownloadEngine::Unresolved => Engine::Unresolved,
        }
    }
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Engine {
    fn capabilities_of(&self) -> EngineCapabilities {
        match self {
            Engine::HttpMultipart => EngineCapabilities {
                multipart: true,
                server_checksums: true,
                response_headers: true,
                exact_size: true,
                multi_file: false,
            },
            // yt-dlp owns the transfer end to end: it uses one connection per
            // format, never surfaces the underlying HTTP exchange, and reports
            // an estimated size for adaptive formats.
            Engine::Ytdlp => EngineCapabilities {
                multipart: false,
                server_checksums: false,
                response_headers: false,
                exact_size: false,
                multi_file: false,
            },
            // Nothing is known until the URL is evaluated, so nothing can be
            // promised. A UI should render such a row as pending rather than
            // as a download missing its details.
            Engine::Unresolved => EngineCapabilities {
                multipart: false,
                server_checksums: false,
                response_headers: false,
                exact_size: false,
                multi_file: false,
            },
        }
    }

    /// What this engine is able to report about a download.
    pub fn capabilities(&self) -> EngineCapabilities {
        self.capabilities_of()
    }

    /// Stable lowercase identifier, matching the persisted enum's value names.
    pub fn as_str(&self) -> &'static str {
        match self {
            Engine::HttpMultipart => "http_multipart",
            Engine::Ytdlp => "ytdlp",
            Engine::Unresolved => "unresolved",
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
#[non_exhaustive]
pub enum EnginePreference {
    #[default]
    Auto,
    Engine(Engine),
}

impl EnginePreference {
    /// Engine explicitly requested by the caller, if any.
    pub fn forced(&self) -> Option<Engine> {
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
            "http" | "http_multipart" => Ok(EnginePreference::Engine(Engine::HttpMultipart)),
            "ytdlp" | "yt-dlp" => Ok(EnginePreference::Engine(Engine::Ytdlp)),
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
            EnginePreference::Engine(Engine::HttpMultipart)
        );
        assert_eq!(
            "yt-dlp".parse::<EnginePreference>().unwrap(),
            EnginePreference::Engine(Engine::Ytdlp)
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
        let caps = Engine::Ytdlp.capabilities();
        assert!(!caps.server_checksums);
        assert!(!caps.response_headers);
        assert!(!caps.multipart);
        assert!(Engine::HttpMultipart.capabilities().multipart);
    }

    #[test]
    fn the_public_engine_round_trips_through_the_persisted_one() {
        // Storage keeps the generated discriminant; the API keeps an enum we
        // can extend. Neither is useful if they disagree.
        for engine in [Engine::HttpMultipart, Engine::Ytdlp, Engine::Unresolved] {
            let stored: DownloadEngine = engine.into();
            assert_eq!(Engine::from(stored), engine);
            assert_eq!(stored.as_str_name(), engine.as_str());
        }
    }
}
