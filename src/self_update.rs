//! Replacing odl's own binary with the latest GitHub release.
//!
//! # What this will and will not touch
//!
//! Only an installation odl put there itself. The install scripts leave a
//! receipt naming the directory they wrote to; when it matches the running
//! binary, that binary may be replaced. Without a receipt — an older script
//! install, or a copy made by hand — the binary still qualifies if it sits in
//! the directory the scripts default to and the user can write it. Anything
//! else is refused with the command that owns it instead: `cargo install`,
//! Homebrew, a distribution package and Nix all keep their own record of what
//! they installed, and writing over that record's file is how a machine ends
//! up with a package manager reporting a version nothing on disk has.
//!
//! # What is verified
//!
//! Every release archive is checked against the SHA-256 published beside it in
//! the same release, and an update that cannot be verified is refused rather
//! than completed with a warning. As with [`crate::ytdlp::install`], that
//! protects against a corrupted transfer or a tampered mirror — not against a
//! compromised upstream repository, since the sums come from the same place as
//! the files.
//!
//! The transfer itself is odl's own: this module says *what* to fetch and what
//! its checksum must be ([`plan`]), and turns the verified archive into the
//! installed binary ([`finish`]). Everything in between is the download
//! manager, with the user's proxy, retries and connection settings.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::OdlError;

/// Releases that publish odl's binaries.
const RELEASE_API: &str = "https://api.github.com/repos/jd1378/odl/releases/latest";

/// Target triple this binary was built for, recorded by `build.rs`. It names
/// the release asset: guessing from `std::env::consts` cannot tell gnu from
/// musl, and installing the wrong one produces a binary that will not start.
pub const BUILD_TARGET: &str = env!("ODL_BUILD_TARGET");

/// A checksum file is under a hundred bytes; anything approaching this is not
/// one, and is not worth reading into memory to find out.
const MAX_CHECKSUM_BYTES: usize = 64 * 1024;

/// Written by the install scripts, read here to know odl installed itself.
///
/// Kept in odl's own data directory rather than beside the binary: the install
/// directory may need privileges to write (`/usr/local/bin`), and a receipt
/// that cannot be written is a receipt that does not exist. TOML rather than
/// JSON because a `sh` script has to write it without a JSON encoder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallReceipt {
    /// Directory the script installed into.
    pub install_dir: PathBuf,
    /// Release tag installed, e.g. `v2.1.0`. Advisory: the binary's own
    /// `--version` is what an update compares against.
    #[serde(default)]
    pub tag: Option<String>,
    /// Which script wrote this — `install.sh` or `install.ps1`.
    #[serde(default)]
    pub installer: Option<String>,
}

impl InstallReceipt {
    /// Where the install scripts write the receipt.
    pub fn path() -> PathBuf {
        crate::fs_utils::get_odl_dir().join("install-receipt.toml")
    }

    pub async fn load() -> Option<Self> {
        let raw = tokio::fs::read_to_string(Self::path()).await.ok()?;
        toml::from_str(&raw).ok()
    }
}

/// Why an update is not allowed to proceed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ineligible {
    /// Another program installed this binary and still tracks it.
    ManagedBy(&'static str),
    /// A plausible location, but the file cannot be replaced by this user.
    NotWritable(PathBuf),
    /// Neither a receipt nor a location odl's installers use.
    UnknownInstall(PathBuf),
}

impl Ineligible {
    /// What to tell the user, including the command that *can* update them.
    pub fn explain(&self) -> String {
        match self {
            Ineligible::ManagedBy(manager) => format!(
                "this odl was installed by {manager}, which tracks its own copy — update it there \
                 rather than writing over the file it installed"
            ),
            Ineligible::NotWritable(path) => format!(
                "{} is not writable by this user; re-run with the privileges that installed it, \
                 or re-install with the script into a directory you own",
                path.display()
            ),
            Ineligible::UnknownInstall(path) => format!(
                "{} was not installed by odl's install script, so odl will not replace it. \
                 Install with the script to enable updates: \
                 https://github.com/jd1378/odl#installation",
                path.display()
            ),
        }
    }
}

/// Roots owned by a package manager. A binary under one of these is that
/// manager's file, whatever its permissions happen to say.
fn managed_by(path: &Path) -> Option<&'static str> {
    let text = path.to_string_lossy().replace('\\', "/");
    // Nix store paths are read-only anyway; naming it produces a better error
    // than "permission denied" on a path the user cannot fix.
    if text.starts_with("/nix/store/") {
        return Some("Nix");
    }
    if text.contains("/homebrew/") || text.contains("/Cellar/") {
        return Some("Homebrew");
    }
    if text.contains("/.cargo/bin/") {
        return Some("cargo install");
    }
    // Distribution packages. `/usr/local` is deliberately absent: it is where
    // `install.sh --dir /usr/local/bin` puts things, and no distribution owns
    // it.
    if text.starts_with("/usr/bin/") || text.starts_with("/usr/sbin/") || text.starts_with("/bin/")
    {
        return Some("a system package");
    }
    None
}

/// Where the install scripts put odl when told nothing else.
fn default_install_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if cfg!(windows) {
        // install.ps1: %LOCALAPPDATA%\Programs\odl
        if let Some(local) = dirs::data_local_dir() {
            dirs.push(local.join("Programs").join("odl"));
        }
    } else if let Some(home) = dirs::home_dir() {
        // install.sh: $HOME/.local/bin
        dirs.push(home.join(".local").join("bin"));
    }
    dirs
}

/// Can this file be replaced? Checked by writing, because every cheaper answer
/// — mode bits, ownership, `access(2)` — is wrong somewhere: ACLs, read-only
/// mounts, and containers all disagree with the permission bits.
async fn is_replaceable(exe: &Path) -> bool {
    let Some(dir) = exe.parent() else {
        return false;
    };
    // A rename into place needs a writable *directory*, not a writable file.
    let probe = dir.join(format!(".odl-update-probe-{}", std::process::id()));
    match tokio::fs::write(&probe, b"").await {
        Ok(()) => {
            let _ = tokio::fs::remove_file(&probe).await;
            true
        }
        Err(_) => false,
    }
}

/// Decide whether this binary may be replaced in place.
///
/// `exe` is the running binary, with symlinks resolved by the caller: updating
/// through a symlink would replace the link and orphan its target.
pub async fn eligibility(exe: &Path) -> Result<(), Ineligible> {
    let receipt = InstallReceipt::load().await;
    let claimed = decide(exe, receipt.as_ref(), &default_install_dirs())?;
    // Left until last because it costs a write: the answers above are free and
    // one of them usually settles it.
    if !is_replaceable(claimed).await {
        return Err(Ineligible::NotWritable(exe.to_path_buf()));
    }
    Ok(())
}

/// Everything about eligibility that does not need to touch the disk.
///
/// Returns the binary it agreed to replace, so the writability probe and the
/// decision cannot drift apart.
fn decide<'a>(
    exe: &'a Path,
    receipt: Option<&InstallReceipt>,
    default_dirs: &[PathBuf],
) -> Result<&'a Path, Ineligible> {
    if let Some(manager) = managed_by(exe) {
        return Err(Ineligible::ManagedBy(manager));
    }
    let dir = exe.parent();
    // A receipt for some other directory says nothing about this binary:
    // someone can keep a script install and also build one from source.
    let receipt_matches = receipt.is_some_and(|r| dir == Some(r.install_dir.as_path()));
    let in_default_dir = dir.is_some_and(|d| default_dirs.iter().any(|known| known == d));

    if receipt_matches || in_default_dir {
        Ok(exe)
    } else {
        Err(Ineligible::UnknownInstall(exe.to_path_buf()))
    }
}

#[derive(Debug, Deserialize)]
struct Release {
    tag_name: String,
    #[serde(default)]
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
    #[serde(default)]
    size: u64,
}

/// One release archive to fetch, and how to know it arrived intact.
#[derive(Debug, Clone)]
pub struct UpdatePlan {
    /// Release tag, e.g. `v2.1.1`.
    pub tag: String,
    /// Version the tag names, with the leading `v` removed.
    pub version: String,
    /// Asset name; also its filename while staged.
    pub name: String,
    pub url: String,
    /// Expected SHA-256, lowercase hex, published beside the asset.
    pub sha256: String,
    /// Size the release listing claims. Advisory only.
    pub size: u64,
}

fn other(message: impl Into<String>) -> OdlError {
    OdlError::Other {
        message: message.into(),
        origin: Box::new(std::io::Error::other("self-update")),
    }
}

/// Archive extension for this platform, matching what the release workflow
/// publishes.
fn asset_extension() -> &'static str {
    if cfg!(windows) { "zip" } else { "tar.gz" }
}

/// Is `candidate` newer than what is running?
///
/// Compared field by field rather than as text, so `2.10.0` beats `2.9.0`. A
/// version that will not parse is treated as *not* newer: an update is a
/// destructive act and a number nobody can order is not a reason for one.
pub fn is_newer(candidate: &str, current: &str) -> bool {
    fn parts(v: &str) -> Option<(u64, u64, u64)> {
        let core = v.trim().trim_start_matches('v');
        // Ignore any pre-release / build suffix for ordering purposes.
        let core = core.split(['-', '+']).next().unwrap_or(core);
        let mut it = core.split('.');
        let major = it.next()?.parse().ok()?;
        let minor = it.next().unwrap_or("0").parse().ok()?;
        let patch = it.next().unwrap_or("0").parse().ok()?;
        Some((major, minor, patch))
    }
    match (parts(candidate), parts(current)) {
        (Some(new), Some(now)) => new > now,
        _ => false,
    }
}

/// Read the SHA-256 out of a `sha256sum`-style line: `<hex>  <filename>`.
fn digest_from_checksum_file(body: &str, asset: &str) -> Option<String> {
    body.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let digest = fields.next()?;
        // A bare digest with no filename is accepted — the file is published
        // per-asset, so there is nothing to confuse it with. When a name is
        // present it must be the asset's.
        match fields.next() {
            Some(name) if name.trim_start_matches('*') != asset => None,
            _ => {
                let digest = digest.trim().to_ascii_lowercase();
                (digest.len() == 64 && digest.chars().all(|c| c.is_ascii_hexdigit()))
                    .then_some(digest)
            }
        }
    })
}

/// Work out what to download, without downloading it.
///
/// The listing is fetched with `net`'s proxy, certificate and timeout
/// settings, so reaching GitHub obeys the same network rules as reaching
/// anything else odl downloads.
///
/// Returns `Ok(None)` when the published release is not newer than `current`.
pub async fn plan(
    net: &crate::config::DownloadOptions,
    current: &str,
) -> Result<Option<UpdatePlan>, OdlError> {
    let client = crate::http::client_for(net)?;
    plan_from(&client, RELEASE_API, BUILD_TARGET, current).await
}

/// [`plan`] against a given listing and target, so the decisions can be tested
/// without a network or this machine's own triple.
async fn plan_from(
    client: &reqwest::Client,
    api: &str,
    target: &str,
    current: &str,
) -> Result<Option<UpdatePlan>, OdlError> {
    let release = client
        .get(api)
        // GitHub rejects requests without one, and naming ourselves is more
        // honest than borrowing a browser's identity.
        .header(reqwest::header::USER_AGENT, "odl")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
        .map_err(|e| other(format!("could not reach the release listing: {e}")))?
        .error_for_status()
        .map_err(|e| other(format!("the release listing was refused: {e}")))?
        .bytes()
        .await
        .map_err(|e| other(format!("the release listing could not be read: {e}")))?;
    let release: Release = serde_json::from_slice(&release)
        .map_err(|e| other(format!("the release listing could not be parsed: {e}")))?;

    if !is_newer(&release.tag_name, current) {
        return Ok(None);
    }

    let wanted = format!("odl-{}-{}.{}", release.tag_name, target, asset_extension());
    let asset = release
        .assets
        .iter()
        .find(|a| a.name == wanted)
        .ok_or_else(|| {
            other(format!(
                "release {} publishes no build for {target}; install the one you want from \
                 https://github.com/jd1378/odl/releases",
                release.tag_name
            ))
        })?;
    let sums = release
        .assets
        .iter()
        .find(|a| a.name == format!("{wanted}.sha256"))
        .ok_or_else(|| {
            other(format!(
                "release {} publishes no checksum for {wanted}, so the download cannot be \
                 verified; update by hand if you trust it",
                release.tag_name
            ))
        })?;

    if sums.size as usize > MAX_CHECKSUM_BYTES {
        return Err(other("the published checksum file is implausibly large"));
    }
    let body = client
        .get(&sums.browser_download_url)
        .header(reqwest::header::USER_AGENT, "odl")
        .send()
        .await
        .map_err(|e| other(format!("could not fetch the checksum: {e}")))?
        .error_for_status()
        .map_err(|e| other(format!("the checksum was refused: {e}")))?
        .text()
        .await
        .map_err(|e| other(format!("the checksum could not be read: {e}")))?;
    if body.len() > MAX_CHECKSUM_BYTES {
        return Err(other("the published checksum file is implausibly large"));
    }
    let sha256 = digest_from_checksum_file(&body, &asset.name).ok_or_else(|| {
        other(format!(
            "no usable SHA-256 for {} was published",
            asset.name
        ))
    })?;

    Ok(Some(UpdatePlan {
        version: release.tag_name.trim_start_matches('v').to_string(),
        tag: release.tag_name,
        name: asset.name.clone(),
        url: asset.browser_download_url.clone(),
        sha256,
        size: asset.size,
    }))
}

/// Where a downloaded archive is staged before it becomes the binary.
pub fn staging_dir() -> PathBuf {
    std::env::temp_dir().join("odl-update")
}

/// Turn a downloaded, checksum-verified archive into the running binary.
///
/// Verification is the caller's job — odl's own downloader does it — so this
/// only unpacks and puts the binary in place. Returns the path replaced.
pub async fn finish(archive: &Path, exe: &Path) -> Result<PathBuf, OdlError> {
    let staging = tempfile::tempdir()?;
    // `tar` reads zip as well as tar.gz on every platform odl publishes for
    // (bsdtar ships with Windows 10 and later), so one call covers both.
    let status = tokio::process::Command::new("tar")
        .arg("-xf")
        .arg(archive)
        .current_dir(staging.path())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .status()
        .await
        .map_err(|e| other(format!("could not run `tar` to unpack the release ({e})")))?;
    if !status.success() {
        return Err(other("`tar` could not unpack the release archive"));
    }

    let binary_name = if cfg!(windows) { "odl.exe" } else { "odl" };
    let new_binary = find_file(staging.path(), binary_name, 4)
        .ok_or_else(|| other("the release archive contained no odl binary"))?;

    replace_binary(&new_binary, exe).await?;
    Ok(exe.to_path_buf())
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

/// Put `new_binary` where `exe` is, atomically as far as the filesystem allows.
///
/// The new file is staged in the destination directory — a rename cannot cross
/// filesystems, and the download landed in the temp area — then renamed over
/// the old one. On Windows the running image cannot be overwritten, so the old
/// binary is renamed aside first; it is deleted on the next run, since it is
/// still mapped until this process exits.
async fn replace_binary(new_binary: &Path, exe: &Path) -> Result<(), OdlError> {
    let dir = exe
        .parent()
        .ok_or_else(|| other("the running binary has no parent directory"))?;
    let staged = dir.join(".odl-update-staged");
    tokio::fs::copy(new_binary, &staged).await.map_err(|e| {
        other(format!(
            "could not write the new binary to {}: {e}",
            staged.display()
        ))
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Match what the install script sets, rather than whatever the archive
        // carried: an update should not quietly change who can run odl.
        let mode = match tokio::fs::metadata(exe).await {
            Ok(meta) => meta.permissions().mode(),
            Err(_) => 0o755,
        };
        if let Err(e) =
            tokio::fs::set_permissions(&staged, std::fs::Permissions::from_mode(mode)).await
        {
            let _ = tokio::fs::remove_file(&staged).await;
            return Err(other(format!(
                "could not set permissions on the update: {e}"
            )));
        }
    }

    if cfg!(windows) {
        let aside = exe.with_extension("old");
        // A leftover from a previous update: this process no longer maps it.
        let _ = tokio::fs::remove_file(&aside).await;
        if let Err(e) = tokio::fs::rename(exe, &aside).await {
            let _ = tokio::fs::remove_file(&staged).await;
            return Err(other(format!(
                "could not move the running binary aside: {e}"
            )));
        }
        if let Err(e) = tokio::fs::rename(&staged, exe).await {
            // Put the old binary back rather than leaving the user with none.
            let _ = tokio::fs::rename(&aside, exe).await;
            let _ = tokio::fs::remove_file(&staged).await;
            return Err(other(format!("could not install the update: {e}")));
        }
    } else if let Err(e) = tokio::fs::rename(&staged, exe).await {
        let _ = tokio::fs::remove_file(&staged).await;
        return Err(other(format!("could not install the update: {e}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_package_managers_binary_is_left_alone() {
        assert_eq!(
            managed_by(Path::new("/home/u/.cargo/bin/odl")),
            Some("cargo install")
        );
        assert_eq!(
            managed_by(Path::new("/usr/bin/odl")),
            Some("a system package")
        );
        assert_eq!(
            managed_by(Path::new("/nix/store/abc123-odl-2.0.0/bin/odl")),
            Some("Nix")
        );
        assert_eq!(
            managed_by(Path::new("/opt/homebrew/bin/odl")),
            Some("Homebrew")
        );
    }

    /// `/usr/local/bin` is what `install.sh --dir` is most often pointed at,
    /// so it must not be mistaken for a distribution's own directory.
    #[test]
    fn a_script_install_outside_the_default_dir_is_not_package_managed() {
        assert_eq!(managed_by(Path::new("/usr/local/bin/odl")), None);
        assert_eq!(managed_by(Path::new("/home/u/.local/bin/odl")), None);
    }

    #[test]
    fn versions_are_ordered_as_numbers_not_text() {
        assert!(is_newer("v2.10.0", "2.9.0"));
        assert!(is_newer("2.1.0", "2.0.9"));
        assert!(!is_newer("v2.1.0", "2.1.0"));
        assert!(!is_newer("v2.0.9", "2.1.0"));
    }

    /// An update overwrites a working program. A version string nobody can
    /// order is not grounds for that.
    #[test]
    fn an_unparseable_version_is_never_newer() {
        assert!(!is_newer("nightly", "2.1.0"));
        assert!(!is_newer("v2.1.0", "custom-build"));
    }

    #[test]
    fn a_pre_release_suffix_does_not_break_the_comparison() {
        assert!(is_newer("v2.2.0-rc1", "2.1.0"));
        assert!(!is_newer("v2.1.0-rc1", "2.1.0"));
    }

    #[test]
    fn the_digest_is_read_from_the_published_checksum_file() {
        let hex = "a".repeat(64);
        let body = format!("{hex}  odl-v2.1.1-x86_64-unknown-linux-gnu.tar.gz\n");
        assert_eq!(
            digest_from_checksum_file(&body, "odl-v2.1.1-x86_64-unknown-linux-gnu.tar.gz"),
            Some(hex.clone())
        );
        // Bare digest, no filename.
        assert_eq!(digest_from_checksum_file(&hex, "anything"), Some(hex));
    }

    #[test]
    fn a_digest_for_another_file_is_not_accepted() {
        let hex = "b".repeat(64);
        let body = format!("{hex}  odl-v2.1.1-aarch64-apple-darwin.tar.gz\n");
        assert_eq!(
            digest_from_checksum_file(&body, "odl-v2.1.1-x86_64-unknown-linux-gnu.tar.gz"),
            None
        );
    }

    #[test]
    fn a_truncated_or_malformed_digest_is_not_accepted() {
        assert_eq!(
            digest_from_checksum_file("abc123  odl.tar.gz", "odl.tar.gz"),
            None
        );
        assert_eq!(
            digest_from_checksum_file(&format!("{}  odl.tar.gz", "z".repeat(64)), "odl.tar.gz"),
            None
        );
    }

    fn receipt_for(dir: &Path) -> InstallReceipt {
        InstallReceipt {
            install_dir: dir.to_path_buf(),
            tag: Some("v2.1.0".to_string()),
            installer: Some("install.sh".to_string()),
        }
    }

    #[test]
    fn a_binary_nobody_claims_is_refused() {
        let exe = Path::new("/home/u/build/odl");
        assert_eq!(
            decide(exe, None, &[]),
            Err(Ineligible::UnknownInstall(exe.to_path_buf()))
        );
    }

    #[test]
    fn the_receipts_own_install_is_replaceable() {
        let exe = Path::new("/opt/odl-bin/odl");
        let receipt = receipt_for(Path::new("/opt/odl-bin"));
        assert_eq!(decide(exe, Some(&receipt), &[]), Ok(exe));
    }

    /// Someone can keep a script install and also build odl from source. The
    /// receipt describes the first; it must not license replacing the second.
    #[test]
    fn a_receipt_for_another_directory_licenses_nothing() {
        let exe = Path::new("/home/u/src/odl/target/release/odl");
        let receipt = receipt_for(Path::new("/home/u/.local/bin"));
        assert!(matches!(
            decide(exe, Some(&receipt), &[]),
            Err(Ineligible::UnknownInstall(_))
        ));
    }

    /// Installs that predate the receipt are the common case on first upgrade.
    #[test]
    fn the_installers_default_directory_needs_no_receipt() {
        let default = PathBuf::from("/home/u/.local/bin");
        let exe = default.join("odl");
        assert_eq!(
            decide(&exe, None, std::slice::from_ref(&default)),
            Ok(exe.as_path())
        );
    }

    /// A receipt cannot license writing over a package manager's file.
    #[test]
    fn a_receipt_does_not_override_a_package_manager() {
        let exe = Path::new("/home/u/.cargo/bin/odl");
        let receipt = receipt_for(Path::new("/home/u/.cargo/bin"));
        assert_eq!(
            decide(exe, Some(&receipt), &[]),
            Err(Ineligible::ManagedBy("cargo install"))
        );
    }

    /// The shape the install scripts write, parsed by the code that reads it.
    #[test]
    fn the_receipt_the_scripts_write_is_readable() {
        let receipt: InstallReceipt = toml::from_str(
            "install_dir = \"/home/u/.local/bin\"\ntag = \"v2.1.0\"\ninstaller = \"install.sh\"\n",
        )
        .expect("the installers' receipt must parse");
        assert_eq!(receipt.install_dir, PathBuf::from("/home/u/.local/bin"));
        assert_eq!(receipt.tag.as_deref(), Some("v2.1.0"));
    }

    /// An older receipt, or one a script wrote before these fields existed.
    #[test]
    fn a_receipt_with_only_the_directory_still_works() {
        let receipt: InstallReceipt =
            toml::from_str("install_dir = \"/home/u/.local/bin\"\n").expect("must parse");
        assert!(receipt.tag.is_none());
    }

    #[tokio::test]
    async fn an_unwritable_install_is_refused_before_anything_is_downloaded() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("odl");
        tokio::fs::write(&exe, b"binary").await.unwrap();
        // The probe writes into the *directory*, so make that unwritable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500))
                .await
                .unwrap();
            assert!(!is_replaceable(&exe).await);
            tokio::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .await
                .unwrap();
        }
        assert!(is_replaceable(&exe).await);
    }

    /// A release listing shaped like GitHub's, for `plan_from`.
    fn listing(server: &mockito::Server, tag: &str, assets: &[&str]) -> String {
        let assets: Vec<String> = assets
            .iter()
            .map(|name| {
                format!(
                    r#"{{"name":"{name}","browser_download_url":"{}/{name}","size":10}}"#,
                    server.url()
                )
            })
            .collect();
        format!(r#"{{"tag_name":"{tag}","assets":[{}]}}"#, assets.join(","))
    }

    const TARGET: &str = "x86_64-unknown-linux-gnu";

    fn asset_for(tag: &str) -> String {
        format!("odl-{tag}-{TARGET}.{}", asset_extension())
    }

    #[tokio::test]
    async fn a_newer_release_is_planned_with_its_published_digest() {
        let mut server = mockito::Server::new_async().await;
        let asset = asset_for("v9.9.9");
        let hex = "c".repeat(64);
        let _list = server
            .mock("GET", "/releases/latest")
            .with_body(listing(
                &server,
                "v9.9.9",
                &[&asset, &format!("{asset}.sha256")],
            ))
            .create_async()
            .await;
        let _sum = server
            .mock("GET", format!("/{asset}.sha256").as_str())
            .with_body(format!("{hex}  {asset}\n"))
            .create_async()
            .await;

        let plan = plan_from(
            &reqwest::Client::new(),
            &format!("{}/releases/latest", server.url()),
            TARGET,
            "2.1.0",
        )
        .await
        .expect("a well-formed release must plan")
        .expect("9.9.9 is newer than 2.1.0");
        assert_eq!(plan.version, "9.9.9");
        assert_eq!(plan.name, asset);
        assert_eq!(plan.sha256, hex);
    }

    /// The whole point of the checksum: without one there is nothing to check
    /// the download against, so there is no update.
    #[tokio::test]
    async fn a_release_without_a_checksum_is_refused() {
        let mut server = mockito::Server::new_async().await;
        let asset = asset_for("v9.9.9");
        let _list = server
            .mock("GET", "/releases/latest")
            .with_body(listing(&server, "v9.9.9", &[&asset]))
            .create_async()
            .await;

        let err = plan_from(
            &reqwest::Client::new(),
            &format!("{}/releases/latest", server.url()),
            TARGET,
            "2.1.0",
        )
        .await
        .expect_err("an unverifiable release must not be planned");
        assert!(
            err.to_string().contains("no checksum"),
            "the reason must name the missing checksum: {err}"
        );
    }

    #[tokio::test]
    async fn a_release_without_a_build_for_this_machine_is_refused() {
        let mut server = mockito::Server::new_async().await;
        let other = "odl-v9.9.9-aarch64-apple-darwin.tar.gz";
        let _list = server
            .mock("GET", "/releases/latest")
            .with_body(listing(
                &server,
                "v9.9.9",
                &[other, &format!("{other}.sha256")],
            ))
            .create_async()
            .await;

        let err = plan_from(
            &reqwest::Client::new(),
            &format!("{}/releases/latest", server.url()),
            TARGET,
            "2.1.0",
        )
        .await
        .expect_err("there is nothing here this machine can run");
        assert!(err.to_string().contains(TARGET), "got: {err}");
    }

    #[tokio::test]
    async fn the_release_in_use_plans_nothing() {
        let mut server = mockito::Server::new_async().await;
        let asset = asset_for("v2.1.0");
        let _list = server
            .mock("GET", "/releases/latest")
            .with_body(listing(
                &server,
                "v2.1.0",
                &[&asset, &format!("{asset}.sha256")],
            ))
            .create_async()
            .await;

        let plan = plan_from(
            &reqwest::Client::new(),
            &format!("{}/releases/latest", server.url()),
            TARGET,
            "2.1.0",
        )
        .await
        .expect("an up-to-date check is not an error");
        assert!(plan.is_none());
    }

    #[tokio::test]
    async fn a_replaced_binary_keeps_its_path_and_contents() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("odl");
        tokio::fs::write(&exe, b"old").await.unwrap();
        let fresh = dir.path().join("fresh");
        tokio::fs::write(&fresh, b"new").await.unwrap();

        replace_binary(&fresh, &exe).await.unwrap();
        assert_eq!(tokio::fs::read(&exe).await.unwrap(), b"new");
        // No staging file left behind for the next run to trip over.
        assert!(!dir.path().join(".odl-update-staged").exists());
    }
}
