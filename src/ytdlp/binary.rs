//! Locating and vetting the external `yt-dlp` and `ffmpeg` executables.
//!
//! Lookup is done explicitly rather than by handing a bare program name to
//! [`std::process::Command`]. Windows resolves a bare name against the current
//! directory before `PATH`, so a `yt-dlp.exe` dropped into whatever directory
//! odl happens to be run from would win — resolving to an absolute path first
//! removes that. Empty `PATH` entries, which mean "current directory" on both
//! platforms, are skipped for the same reason.

use crate::config::YtdlpOptions;
use crate::error::YtdlpError;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use tokio::process::Command;

/// Oldest `yt-dlp` accepted.
///
/// `--progress-delta` landed in 2024.05.27; requiring a little more than that
/// keeps the machine-readable progress plumbing on ground we can rely on
/// rather than probing feature by feature.
const MIN_VERSION: Version = Version {
    year: 2024,
    month: 7,
    day: 1,
};

/// A `yt-dlp` release version, which is a date (`2025.06.09`). Nightly builds
/// append a build number that carries no ordering we need, so it is ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub year: u32,
    pub month: u32,
    pub day: u32,
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}.{:02}.{:02}", self.year, self.month, self.day)
    }
}

impl std::str::FromStr for Version {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.trim().split('.');
        let mut next = || parts.next().and_then(|p| p.parse::<u32>().ok()).ok_or(());
        let year = next()?;
        let month = next()?;
        let day = next()?;
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return Err(());
        }
        Ok(Version { year, month, day })
    }
}

/// Executables needed to run a delegated download.
#[derive(Debug, Clone)]
pub struct Tools {
    /// Absolute path to `yt-dlp`.
    pub ytdlp: PathBuf,
    /// Absolute path to `ffmpeg`, when present. Without it only formats that
    /// need no muxing can be downloaded.
    pub ffmpeg: Option<PathBuf>,
}

/// Whether `path` is a file we could execute.
fn is_executable_file(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// Candidate file names for `program` on this platform.
#[cfg(windows)]
fn candidate_names(program: &str) -> Vec<String> {
    // PATHEXT drives what the shell would consider executable. Fall back to
    // the usual set when it is unset or empty.
    let pathext = std::env::var("PATHEXT").unwrap_or_default();
    let mut exts: Vec<String> = pathext
        .split(';')
        .map(|e| e.trim().to_ascii_lowercase())
        .filter(|e| e.starts_with('.'))
        .collect();
    if exts.is_empty() {
        exts = vec![".exe".into(), ".cmd".into(), ".bat".into()];
    }
    exts.iter().map(|e| format!("{program}{e}")).collect()
}

#[cfg(not(windows))]
fn candidate_names(program: &str) -> Vec<String> {
    vec![program.to_owned()]
}

/// Resolve `program` to an absolute path by walking `PATH`.
///
/// Returns `None` rather than falling back to a bare name: an unresolved name
/// handed to `Command` is exactly the current-directory lookup this avoids.
fn which(program: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    let names = candidate_names(program);
    for dir in std::env::split_paths(&path) {
        // An empty entry means "current directory"; never search it.
        if dir.as_os_str().is_empty() {
            continue;
        }
        for name in &names {
            let candidate = dir.join(name);
            if is_executable_file(&candidate) {
                return std::fs::canonicalize(&candidate).ok().or(Some(candidate));
            }
        }
    }
    None
}

/// Resolve a user-configured executable path.
///
/// An explicitly configured path is used as given (after canonicalization) and
/// never falls back to a `PATH` lookup — silently running a different binary
/// than the one named would be worse than failing.
fn resolve_configured(configured: Option<&Path>, program: &str) -> Option<PathBuf> {
    match configured {
        Some(p) => is_executable_file(p)
            .then(|| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())),
        None => which(program),
    }
}

/// Identity of a particular build of a tool.
///
/// Path alone is not enough to memo against: a tool gets upgraded, replaced by
/// a different build, or swapped for something else entirely between runs, and
/// a cache keyed only on where it lives would keep reporting the old answer.
/// Size and modification time change on any of those.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ToolIdentity {
    path: PathBuf,
    len: u64,
    modified: Option<std::time::SystemTime>,
}

impl ToolIdentity {
    fn of(path: &Path) -> Option<Self> {
        let meta = std::fs::metadata(path).ok()?;
        Some(Self {
            path: path.to_path_buf(),
            len: meta.len(),
            modified: meta.modified().ok(),
        })
    }
}

/// Process-lifetime memo of `--version` results.
///
/// A single odl run can evaluate many URLs; spawning `yt-dlp --version` for
/// each would be pure overhead. Existence is *not* memoized — every lookup
/// stats the file — so a tool that disappears is noticed immediately.
fn version_cache() -> &'static Mutex<HashMap<ToolIdentity, Result<Version, String>>> {
    static CACHE: OnceLock<Mutex<HashMap<ToolIdentity, Result<Version, String>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Run `yt-dlp --version` and parse the result.
async fn query_version(ytdlp: &Path) -> Result<Version, String> {
    let output = Command::new(ytdlp)
        .arg("--version")
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!("`--version` exited with {}", output.status));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .next()
        .unwrap_or_default()
        .parse::<Version>()
        .map_err(|_| format!("unrecognised version output {:?}", stdout.trim()))
}

/// Version of the `yt-dlp` at `path`, memoized per build of that file.
async fn cached_version(ytdlp: &Path) -> Result<Version, String> {
    // No identity means the file vanished between resolving and here; let the
    // spawn fail and report that rather than serving a stale answer.
    let identity = ToolIdentity::of(ytdlp);

    if let Some(id) = &identity
        && let Some(hit) = version_cache().lock().ok().and_then(|c| c.get(id).cloned())
    {
        return hit;
    }
    let result = query_version(ytdlp).await;
    if let Some(id) = identity
        && let Ok(mut cache) = version_cache().lock()
    {
        cache.insert(id, result.clone());
    }
    result
}

/// Locate `yt-dlp` (and `ffmpeg`, if present) and verify the version.
///
/// Returns [`YtdlpError::NotFound`] when the tool is absent, which callers in
/// `auto` mode treat as "use the built-in engine" rather than as a failure.
pub async fn discover(opts: &YtdlpOptions) -> Result<Tools, YtdlpError> {
    let ytdlp =
        resolve_configured(opts.binary_path(), "yt-dlp").ok_or_else(|| YtdlpError::NotFound {
            searched_path: opts.binary_path().map(|p| p.display().to_string()),
        })?;

    let version = cached_version(&ytdlp)
        .await
        .map_err(|message| YtdlpError::NotUsable {
            path: ytdlp.display().to_string(),
            message,
        })?;
    if version < MIN_VERSION {
        return Err(YtdlpError::TooOld {
            path: ytdlp.display().to_string(),
            found: version.to_string(),
            required: MIN_VERSION.to_string(),
        });
    }

    Ok(Tools {
        ffmpeg: resolve_configured(opts.ffmpeg_path(), "ffmpeg"),
        ytdlp,
    })
}

/// Whether `program` resolves to something runnable, without spawning it.
/// Used for cheap availability checks such as reporting ffmpeg's absence.
pub fn is_available(configured: Option<&Path>, program: &str) -> bool {
    resolve_configured(configured, program).is_some()
}

/// Convenience for building a command against an already-resolved tool.
pub fn command<S: AsRef<OsStr>>(program: S) -> Command {
    let mut cmd = Command::new(program);
    // yt-dlp is a helper: nothing it writes should reach odl's own stdio, and
    // both streams are parsed rather than displayed.
    cmd.stdin(std::process::Stdio::null());
    cmd.kill_on_drop(true);
    cmd
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_release_and_nightly_versions() {
        assert_eq!(
            "2025.06.09".parse::<Version>().unwrap(),
            Version {
                year: 2025,
                month: 6,
                day: 9
            }
        );
        // Nightly builds append a build stamp; the date prefix is what orders.
        assert_eq!(
            "2025.06.09.232703".parse::<Version>().unwrap(),
            Version {
                year: 2025,
                month: 6,
                day: 9
            }
        );
        assert!("2025.06".parse::<Version>().is_err());
        assert!("not-a-version".parse::<Version>().is_err());
        assert!("2025.13.01".parse::<Version>().is_err());
    }

    #[test]
    fn versions_order_by_date() {
        let older: Version = "2024.01.02".parse().unwrap();
        let newer: Version = "2024.02.01".parse().unwrap();
        assert!(older < newer);
        assert!(older < MIN_VERSION);
    }

    #[test]
    fn configured_path_is_not_looked_up_on_path() {
        // A configured-but-missing binary must fail rather than quietly
        // resolving to some other `yt-dlp` found on PATH.
        let missing = Path::new("/nonexistent/odl-test/yt-dlp");
        assert!(resolve_configured(Some(missing), "yt-dlp").is_none());
    }

    #[test]
    fn resolves_an_executable_from_path() {
        let dir = tempfile::tempdir().unwrap();
        let name = if cfg!(windows) {
            "odl-fake-tool.exe"
        } else {
            "odl-fake-tool"
        };
        let path = dir.path().join(name);
        std::fs::write(&path, b"#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let found = resolve_configured(Some(&path), "odl-fake-tool");
        assert!(found.is_some(), "configured path should resolve");
    }

    #[test]
    fn non_executable_file_is_rejected_on_unix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not-executable");
        std::fs::write(&path, b"data").unwrap();
        #[cfg(unix)]
        assert!(!is_executable_file(&path));
        assert!(!is_executable_file(dir.path()), "a directory is not a tool");
    }

    /// Runs against whatever is installed. Both outcomes are valid — CI may
    /// have no yt-dlp — so this asserts the shape of the answer rather than
    /// which answer it is: a discovered tool must be an absolute path to a
    /// real file, and absence must be reported as `NotFound` rather than as
    /// some other failure.
    #[tokio::test]
    async fn discovery_reports_a_usable_tool_or_says_it_is_missing() {
        let opts = YtdlpOptions::default();
        match discover(&opts).await {
            Ok(tools) => {
                assert!(tools.ytdlp.is_absolute(), "resolved path must be absolute");
                assert!(is_executable_file(&tools.ytdlp));
                if let Some(ffmpeg) = &tools.ffmpeg {
                    assert!(is_executable_file(ffmpeg));
                }
            }
            Err(YtdlpError::NotFound { .. }) => {}
            Err(YtdlpError::TooOld { .. }) => {}
            Err(e) => panic!("unexpected discovery failure: {e}"),
        }
    }

    #[test]
    fn tool_identity_changes_when_the_file_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tool");
        std::fs::write(&path, b"one").unwrap();
        let first = ToolIdentity::of(&path).unwrap();

        // An upgrade in place: same path, different build. A cache keyed on
        // path alone would keep reporting the old version.
        std::fs::write(&path, b"a different build").unwrap();
        let second = ToolIdentity::of(&path).unwrap();
        assert_ne!(first, second);

        std::fs::remove_file(&path).unwrap();
        assert!(
            ToolIdentity::of(&path).is_none(),
            "a deleted tool has no identity"
        );
    }

    #[tokio::test]
    async fn missing_configured_binary_is_not_found_not_a_silent_fallback() {
        let opts = crate::config::YtdlpOptionsBuilder::default()
            .binary_path(Some(PathBuf::from("/nonexistent/odl-test/yt-dlp")))
            .build()
            .unwrap();
        assert!(matches!(
            discover(&opts).await,
            Err(YtdlpError::NotFound { .. })
        ));
    }
}
