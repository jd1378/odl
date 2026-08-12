//! Fetching `yt-dlp` and `ffmpeg` into odl's own data directory.
//!
//! Offered as a convenience for people who would otherwise have to find,
//! download and configure two helper programs by hand. Nothing here happens
//! without an explicit yes: odl asks, states where the files come from, and
//! says how to do it manually instead.
//!
//! # What is and is not guaranteed
//!
//! Every download is checked against a SHA-256 published in the same release,
//! and an install that cannot be verified is refused rather than completed
//! with a warning. That protects against a corrupted transfer or a tampered
//! mirror — it does **not** protect against a compromised upstream repository,
//! since the checksums come from the same place as the files. Verifying the
//! maintainers' GPG signatures would close that gap and needs a trusted key
//! distributed with odl, which is a decision beyond this module.
//!
//! Versions are never pinned: whatever upstream currently calls latest is what
//! gets installed. Extractors break as sites change, so an old yt-dlp is a
//! liability rather than a stable base.
//!
//! The transfer itself is odl's own: this module only says *what* to fetch
//! and what its checksum must be ([`plan`]), then turns the verified file
//! into an installed tool ([`finish`]). Fetching a forty-megabyte binary over
//! a bad connection is exactly the problem odl exists to solve, and a
//! second-rate loop here would fail at it the moment a link dropped.
//!
//! Unpacking the ffmpeg archive is the one memory-hungry step: its `xz`
//! dictionary costs the system decompressor several hundred megabytes,
//! identically whether odl or the user runs `tar`, because the figure is set
//! by how the archive was compressed rather than by who reads it.

use crate::{config::DownloadOptions, error::YtdlpError};
use reqwest::Client;
use std::path::{Path, PathBuf};

/// Release that publishes the `yt-dlp` binaries.
const YTDLP_RELEASE_API: &str = "https://api.github.com/repos/yt-dlp/yt-dlp/releases/latest";

/// ffmpeg builds maintained by the yt-dlp project itself, which is why they
/// are preferred over the many unaffiliated build sites.
const FFMPEG_RELEASE_API: &str =
    "https://api.github.com/repos/yt-dlp/FFmpeg-Builds/releases/latest";

/// A checksum listing is a few kilobytes; anything approaching this is not one.
const MAX_LISTING_BYTES: u64 = 1024 * 1024;

/// Which helper to install.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tool {
    Ytdlp,
    Ffmpeg,
}

impl Tool {
    pub fn as_str(&self) -> &'static str {
        match self {
            Tool::Ytdlp => "yt-dlp",
            Tool::Ffmpeg => "ffmpeg",
        }
    }

    /// Where the binary comes from, for the question odl asks before
    /// downloading anything.
    pub fn source_description(&self) -> &'static str {
        match self {
            Tool::Ytdlp => "the official yt-dlp GitHub releases (github.com/yt-dlp/yt-dlp)",
            Tool::Ffmpeg => {
                "the ffmpeg builds maintained by the yt-dlp project (github.com/yt-dlp/FFmpeg-Builds)"
            }
        }
    }

    /// Why odl wants it, in terms of what the user gets or loses.
    pub fn purpose(&self) -> &'static str {
        match self {
            Tool::Ytdlp => {
                "yt-dlp is what turns a media page link into a downloadable video. \
                 Without it, such a link downloads the web page instead of the video."
            }
            Tool::Ffmpeg => {
                "ffmpeg joins the separate video and audio streams that sites serve for \
                 higher qualities. Without it, downloads are limited to the qualities that \
                 come as a single file — often 720p or below."
            }
        }
    }

    /// The command that installs it system-wide, which is always a valid
    /// alternative to letting odl fetch it.
    pub fn manual_instructions(&self) -> String {
        match std::env::consts::OS {
            "macos" => format!("brew install {}", self.as_str()),
            "windows" => format!("winget install {}", self.as_str()),
            // No single command covers every distribution, so name the tool
            // rather than guess at a package manager that may not be there.
            _ => format!("apt/dnf/pacman install {}", self.as_str()),
        }
    }

    /// Config key that points odl at this tool.
    pub fn config_key(&self) -> &'static str {
        match self {
            Tool::Ytdlp => "ytdlp.binary_path",
            Tool::Ffmpeg => "ytdlp.ffmpeg_path",
        }
    }
}

/// Whether odl can fetch `tool` for the platform it is running on.
///
/// The yt-dlp project publishes ffmpeg builds for Linux and Windows only, and
/// the macOS build sites either target Intel alone or publish no checksum we
/// could verify — so on macOS ffmpeg is left to the user rather than fetched
/// from somewhere odl cannot vouch for.
pub fn can_install(tool: Tool) -> bool {
    match tool {
        Tool::Ytdlp => asset_name_for_ytdlp().is_some(),
        Tool::Ffmpeg => !cfg!(target_os = "macos") && asset_fragment_for_ffmpeg().is_some(),
    }
}

/// Release asset holding the standalone `yt-dlp` for this platform.
///
/// The standalone builds bundle their own Python; the small `yt-dlp` script
/// asset would need a system Python and is deliberately not used.
fn asset_name_for_ytdlp() -> Option<&'static str> {
    Some(match (std::env::consts::OS, std::env::consts::ARCH) {
        // The glibc build. A musl-only system needs the `_musllinux`
        // asset, which odl cannot detect reliably, so such a user installs
        // yt-dlp themselves.
        ("linux", "x86_64") => "yt-dlp_linux",
        ("linux", "aarch64") => "yt-dlp_linux_aarch64",
        ("macos", _) => "yt-dlp_macos",
        ("windows", "x86_64") => "yt-dlp.exe",
        ("windows", "aarch64") => "yt-dlp_arm64.exe",
        ("windows", "x86") => "yt-dlp_x86.exe",
        _ => return None,
    })
}

/// Distinguishing part of the ffmpeg asset name for this platform.
fn asset_fragment_for_ffmpeg() -> Option<&'static str> {
    Some(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "linux64-gpl",
        ("linux", "aarch64") => "linuxarm64-gpl",
        ("windows", "x86_64") => "win64-gpl",
        ("windows", "aarch64") => "winarm64-gpl",
        ("windows", "x86") => "win32-gpl",
        _ => return None,
    })
}

/// Directory odl installs helpers into.
pub fn tools_dir() -> PathBuf {
    crate::fs_utils::get_odl_dir().join("tools")
}

#[derive(Debug, serde::Deserialize)]
struct Release {
    #[serde(default)]
    tag_name: String,
    #[serde(default)]
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug, serde::Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
    #[serde(default)]
    size: u64,
}

fn other(message: impl Into<String>) -> YtdlpError {
    YtdlpError::Other {
        message: message.into(),
    }
}

async fn fetch_release(client: &Client, api: &str) -> Result<Release, YtdlpError> {
    let response = client
        .get(api)
        // GitHub rejects requests without one, and naming ourselves is more
        // honest than borrowing a browser's identity.
        .header(reqwest::header::USER_AGENT, "odl")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .map_err(|e| other(format!("could not reach {api}: {e}")))?
        .error_for_status()
        .map_err(|e| other(format!("{api} returned {e}")))?;
    let body = response
        .bytes()
        .await
        .map_err(|e| other(format!("could not read the release listing: {e}")))?;
    serde_json::from_slice::<Release>(&body)
        .map_err(|e| other(format!("could not parse the release listing: {e}")))
}

/// Fetch a small text asset — a checksum listing — into memory.
async fn fetch_text(client: &Client, url: &str, declared_size: u64) -> Result<String, YtdlpError> {
    if declared_size > MAX_LISTING_BYTES {
        return Err(other(format!("{url} is larger than a checksum listing")));
    }
    let bytes = client
        .get(url)
        .header(reqwest::header::USER_AGENT, "odl")
        .send()
        .await
        .map_err(|e| other(format!("could not download {url}: {e}")))?
        .error_for_status()
        .map_err(|e| other(format!("{url} returned {e}")))?
        .bytes()
        .await
        .map_err(|e| other(format!("could not read {url}: {e}")))?;
    if bytes.len() as u64 > MAX_LISTING_BYTES {
        return Err(other(format!("{url} sent more data than odl will accept")));
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// Find `name`'s expected digest in a `sha256  filename` listing.
///
/// Both `SHA2-256SUMS` and `checksums.sha256` use this shape.
fn expected_digest(listing: &str, name: &str) -> Option<String> {
    listing.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        let digest = parts.next()?;
        let file = parts.next()?.trim_start_matches('*');
        (file == name && digest.len() == 64).then(|| digest.to_ascii_lowercase())
    })
}

/// Where a partly-downloaded asset waits between runs.
///
/// Deliberately stable rather than a temporary directory: an interrupted
/// install is resumed from what is already there, which is the whole point of
/// downloading these through odl instead of a bespoke fetch loop.
pub fn staging_dir() -> PathBuf {
    tools_dir().join("staging")
}

/// One asset to fetch, and how to know it arrived intact.
#[derive(Debug, Clone)]
pub struct AssetPlan {
    /// Name the asset is published under; also its filename while staged.
    pub name: String,
    pub url: String,
    /// Expected SHA-256, lowercase hex, as published in the same release.
    pub sha256: String,
    /// Size the release listing claims. Advisory only.
    pub size: u64,
}

/// Work out what to download for `tool`, without downloading it.
///
/// The listing is fetched with `net`'s proxy, certificate and timeout
/// settings, so reaching the release host obeys the same network rules as
/// reaching anything else odl downloads.
pub async fn plan(net: &DownloadOptions, tool: Tool) -> Result<AssetPlan, YtdlpError> {
    let client = crate::http::client_for(net).map_err(|e| YtdlpError::Other {
        message: e.to_string(),
    })?;
    match tool {
        Tool::Ytdlp => plan_ytdlp(&client).await,
        Tool::Ffmpeg => plan_ffmpeg(&client).await,
    }
}

/// Turn a downloaded, checksum-verified asset into an installed tool.
///
/// Verification is the caller's job — odl's own downloader does it — so this
/// only unpacks when needed and puts the binary in place.
pub async fn finish(tool: Tool, downloaded: &Path, dir: &Path) -> Result<PathBuf, YtdlpError> {
    match tool {
        Tool::Ytdlp => {
            let name = if cfg!(windows) {
                "yt-dlp.exe"
            } else {
                "yt-dlp"
            };
            let path = dir.join(name);
            install_file(downloaded, &path).await?;
            Ok(path)
        }
        Tool::Ffmpeg => {
            let staging = tempfile::tempdir()
                .map_err(|e| other(format!("could not create a temporary directory: {e}")))?;
            extract_ffmpeg(downloaded, staging.path(), dir).await
        }
    }
}

async fn plan_ytdlp(client: &Client) -> Result<AssetPlan, YtdlpError> {
    let wanted = asset_name_for_ytdlp()
        .ok_or_else(|| other("yt-dlp publishes no build for this platform"))?;
    let release = fetch_release(client, YTDLP_RELEASE_API).await?;

    let asset = release
        .assets
        .iter()
        .find(|a| a.name == wanted)
        .ok_or_else(|| other(format!("release {} has no {wanted}", release.tag_name)))?;
    let sums = release
        .assets
        .iter()
        .find(|a| a.name == "SHA2-256SUMS")
        .ok_or_else(|| other("release publishes no checksums; refusing to install"))?;

    let listing = fetch_text(client, &sums.browser_download_url, sums.size).await?;
    let sha256 = expected_digest(&listing, wanted)
        .ok_or_else(|| other(format!("no checksum published for {wanted}")))?;

    Ok(AssetPlan {
        name: wanted.to_owned(),
        url: asset.browser_download_url.clone(),
        sha256,
        size: asset.size,
    })
}

async fn plan_ffmpeg(client: &Client) -> Result<AssetPlan, YtdlpError> {
    if !can_install(Tool::Ffmpeg) {
        return Err(other(
            "odl has no verified ffmpeg build for this platform; install it yourself",
        ));
    }
    let fragment = asset_fragment_for_ffmpeg().expect("checked by can_install");
    let release = fetch_release(client, FFMPEG_RELEASE_API).await?;

    // The `-shared` builds need their libraries alongside them; the plain
    // ones are self-contained, which is what a single extracted binary needs.
    let asset = release
        .assets
        .iter()
        .find(|a| {
            a.name.contains(fragment)
                && !a.name.contains("-shared")
                && (a.name.ends_with(".tar.xz") || a.name.ends_with(".zip"))
        })
        .ok_or_else(|| other(format!("release publishes no {fragment} build")))?;
    let sums = release
        .assets
        .iter()
        .find(|a| a.name == "checksums.sha256")
        .ok_or_else(|| other("release publishes no checksums; refusing to install"))?;

    let listing = fetch_text(client, &sums.browser_download_url, sums.size).await?;
    let sha256 = expected_digest(&listing, &asset.name)
        .ok_or_else(|| other(format!("no checksum published for {}", asset.name)))?;

    Ok(AssetPlan {
        name: asset.name.clone(),
        url: asset.browser_download_url.clone(),
        sha256,
        size: asset.size,
    })
}

/// Unpack the one file we want out of an ffmpeg archive.
///
/// Extraction shells out to the system `tar`, which handles both `.tar.xz` and
/// `.zip` and ships with Linux, macOS, and Windows 10 1803 or newer. Linking a
/// decompressor into odl for a step this rare would cost every user binary
/// size to save one user a dependency they already have.
async fn extract_ffmpeg(
    archive_path: &Path,
    staging: &Path,
    dir: &Path,
) -> Result<PathBuf, YtdlpError> {
    let status = tokio::process::Command::new("tar")
        .arg("-xf")
        .arg(archive_path)
        .current_dir(staging)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .status()
        .await
        .map_err(|e| {
            other(format!(
                "could not run `tar` to unpack ffmpeg ({e}); install ffmpeg yourself instead"
            ))
        })?;
    if !status.success() {
        return Err(other("`tar` could not unpack the ffmpeg archive"));
    }

    let binary = if cfg!(windows) {
        "ffmpeg.exe"
    } else {
        "ffmpeg"
    };
    let found = find_file(staging, binary, 4)
        .ok_or_else(|| other("the ffmpeg archive contained no ffmpeg binary"))?;

    let path = dir.join(binary);
    install_file(&found, &path).await?;
    Ok(path)
}

/// Depth-limited search for `name` under `root`.
fn find_file(root: &Path, name: &str, depth: usize) -> Option<PathBuf> {
    if depth == 0 {
        return None;
    }
    let entries = std::fs::read_dir(root).ok()?;
    let mut dirs = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            dirs.push(path);
        } else if path.file_name().is_some_and(|f| f == name) {
            return Some(path);
        }
    }
    dirs.into_iter()
        .find_map(|d| find_file(&d, name, depth - 1))
}

/// Move a verified file into place and make it runnable.
///
/// Staged next to its destination and renamed, so a failed or interrupted
/// install never leaves a half-written binary that would be executed on the
/// next run. A rename cannot cross filesystems, and the staging directory is
/// in the system temp area, so the bytes are copied there first.
async fn install_file(from: &Path, path: &Path) -> Result<(), YtdlpError> {
    let dir = path
        .parent()
        .ok_or_else(|| other("install path has no parent directory"))?;
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| other(format!("could not create {}: {e}", dir.display())))?;

    let temp = path.with_extension("partial");
    tokio::fs::copy(from, &temp)
        .await
        .map_err(|e| other(format!("could not write {}: {e}", temp.display())))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Owner-only: a tool odl will execute should not be writable by
        // anyone else on the machine.
        tokio::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|e| other(format!("could not make {} executable: {e}", temp.display())))?;
    }

    tokio::fs::rename(&temp, path)
        .await
        .map_err(|e| other(format!("could not install to {}: {e}", path.display())))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_read_from_a_checksum_listing() {
        let listing = "\
aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  yt-dlp
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb  yt-dlp_linux
cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc *yt-dlp.exe
";
        assert_eq!(
            expected_digest(listing, "yt-dlp_linux").unwrap(),
            "b".repeat(64)
        );
        // A leading `*` marks binary mode in the sha256sum format.
        assert_eq!(
            expected_digest(listing, "yt-dlp.exe").unwrap(),
            "c".repeat(64)
        );
        assert!(expected_digest(listing, "yt-dlp_macos").is_none());
    }

    #[test]
    fn a_truncated_or_malformed_digest_is_not_accepted() {
        // Anything but a full-length hex digest must not pass as one, or a
        // mangled listing would silently weaken verification.
        let listing = "abc  yt-dlp_linux\n";
        assert!(expected_digest(listing, "yt-dlp_linux").is_none());
        assert!(expected_digest("", "yt-dlp_linux").is_none());
    }

    #[test]
    fn a_yt_dlp_build_is_offered_exactly_where_upstream_has_one() {
        // Not every target odl ships for is covered upstream: yt-dlp
        // publishes no standalone build for 32-bit Linux or 32-bit ARM, and
        // odl ships binaries for both. `can_install` answering false there is
        // correct — the install offer is simply not made, and the link still
        // downloads over HTTP. Asserting a build exists everywhere failed the
        // release on those targets over a premise that was never true.
        let upstream_builds_for_this_platform = cfg!(target_os = "macos")
            || cfg!(target_os = "windows")
            || (cfg!(target_os = "linux")
                && (cfg!(target_arch = "x86_64") || cfg!(target_arch = "aarch64")));

        if upstream_builds_for_this_platform {
            assert!(
                can_install(Tool::Ytdlp),
                "the asset table lost a platform upstream still builds for"
            );
        } else {
            assert!(
                !can_install(Tool::Ytdlp),
                "an asset name was invented for a platform yt-dlp does not build for"
            );
        }
    }

    #[test]
    fn ffmpeg_is_not_offered_where_no_verified_build_exists() {
        if cfg!(target_os = "macos") {
            assert!(
                !can_install(Tool::Ffmpeg),
                "macOS has no checksummed build odl can vouch for"
            );
        }
    }

    #[tokio::test]
    async fn an_installed_tool_is_executable_and_replaces_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let staged = dir.path().join("staged");
        std::fs::write(&staged, b"#!/bin/sh\nexit 0\n").unwrap();
        let path = dir.path().join("tool");
        install_file(&staged, &path).await.unwrap();

        assert!(path.exists());
        assert!(
            !path.with_extension("partial").exists(),
            "the staging file must not survive a successful install"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "tools odl executes stay owner-only");
        }
    }

    #[test]
    fn a_nested_binary_is_found_within_the_depth_limit() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir
            .path()
            .join("ffmpeg-master-latest-linux64-gpl")
            .join("bin");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("ffmpeg"), b"x").unwrap();

        assert!(find_file(dir.path(), "ffmpeg", 4).is_some());
        assert!(
            find_file(dir.path(), "ffmpeg", 1).is_none(),
            "the search must stay bounded"
        );
    }
}
