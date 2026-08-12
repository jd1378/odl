use crate::{
    config::DownloadOptions,
    credentials::Credentials,
    download_metadata::{
        DownloadEngine, DownloadMetadata, EngineDetails, FileChecksum, PartDetails, ResponseHeader,
        YtdlpDetails,
    },
    engine::Engine,
    error::MetadataError,
    format::Quality,
    fs_utils,
    hash::HashDigest,
    response_info::ResponseInfo,
};
use chrono::{DateTime, Utc};
use derive_builder::{Builder, UninitializedFieldError};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use std::{
    collections::HashMap,
    path::{self, PathBuf},
    str::FromStr,
};
use thiserror::Error;
use tokio::sync::Semaphore;
use ulid::Ulid;
use url::Url;

/// Represents a download instruction.
///
/// `Download` models all information needed to perform a single file download:
/// the remote `url`, the local `download_dir` used to store part files and
/// metadata, the `save_dir` and `filename` for the final assembled file, and
/// per-download options such as `max_connections` and server-provided
/// attributes (ETag, last-modified, size, hashes).
///
/// Use `Download::from_response_info` to construct a new instruction from a
/// `ResponseInfo` (returned after probing the remote URL), or
/// `Download::from_metadata` to recreate an instruction from persisted
/// metadata on disk.
///
/// Examples
///
/// The canonical way to obtain a `Download` is to probe the remote URL and
/// construct it from the HTTP response information. Consumers typically use
/// `DownloadManager::evaluate` which performs the probe and returns a
/// `Download` instruction ready for `DownloadManager::download`.
///
/// The example below is illustrative and intentionally ignored for doctests
/// because the required `ResponseInfo` type is internal to the crate.
///
/// ```ignore
/// // pseudo-code:
/// // let instr = manager
/// //     .evaluate(EvaluateRequest::new(url, save_dir, &save_resolver))
/// //     .await?;
/// // manager.download(DownloadRequest::new(instr, &server_resolver)).await?;
/// ```
#[derive(Builder, Debug, Clone)]
#[builder(build_fn(validate = "Self::validate", error = "DownloadBuilderError"))]
pub struct Download {
    /// Download directory used to store a small metadata file and parts
    download_dir: path::PathBuf,
    /// URL of the file to download.
    url: Url,
    /// Whether the server supports using range requests (i.e., resumable downloads).
    #[builder(default = false)]
    is_resumable: bool,
    /// Should we use the last-modified value server has sent us for the final file ?
    #[builder(default = false)]
    use_server_time: bool,
    /// The final file name to use to save on disk.
    filename: String,
    /// Where to save the final file
    save_dir: path::PathBuf,
    /// File size reported by the server in bytes. can be unknown until download is actually finished.
    #[builder(default = None)]
    size: Option<u64>,
    /// File size reported by the server in bytes. can be unknown until download is actually finished.
    #[builder(default = Vec::new())]
    checksums: Vec<HashDigest>,
    /// the e-tag the server has sent us, if any
    #[builder(default = None)]
    etag: Option<String>,
    /// the last-modified value the server has sent us, if any
    #[builder(default = None)]
    last_modified: Option<i64>,
    /// did the server ask us to authenticate?
    #[builder(default = false)]
    requires_auth: bool,
    /// did the server ask us to authenticate using basic auth?
    #[builder(default = false)]
    requires_basic_auth: bool,
    /// username and password to use, when requires_auth is true and this is provided
    #[builder(default = None)]
    credentials: Option<Credentials>,
    #[builder(default = None)]
    headers: Option<HeaderMap>,
    /// Headers the server sent on the evaluate probe. Instructions rebuilt
    /// from disk carry the filtered subset that was persisted (see
    /// [`Download::stored_response_headers`]).
    #[builder(default = None)]
    response_headers: Option<HeaderMap>,
    /// When `response_headers` were observed, in unix seconds.
    #[builder(default = None)]
    response_headers_probed_at: Option<i64>,
    /// Preferred number of connections for this download.
    /// This will determine the initial number of parts, which will not be decreased after determination,
    /// even if the max_connections is decreased.
    #[builder(default = 6)]
    max_connections: u64,
    /// The parts determined based on max_connections and size. if size is unknown, this will only contain one element
    parts: HashMap<String, PartDetails>,
    /// Is the download finished?
    #[builder(default = false)]
    finished: bool,
    /// Which engine moves the bytes for this download.
    #[builder(default = Engine::HttpMultipart)]
    engine: Engine,
    /// Engine-specific state. `HttpMultipart` keeps none — the fields above
    /// already are its state.
    #[builder(default = None)]
    engine_details: Option<EngineDetails>,
}

/// Response headers never persisted to metadata: they carry session
/// material and are useless in a properties dialog anyway.
const RESPONSE_HEADER_DENYLIST: &[&str] = &[
    "set-cookie",
    "set-cookie2",
    "www-authenticate",
    "proxy-authenticate",
    "authentication-info",
    "proxy-authentication-info",
    "authorization",
    "proxy-authorization",
];

/// Substrings that mark a header as credential-bearing regardless of the
/// exact name. Vendor headers are open-ended (`x-amz-security-token`,
/// `x-api-key`, `x-goog-signature`, …), so this fails closed: an unknown
/// header matching one of these is dropped rather than written to disk.
const RESPONSE_HEADER_SECRET_MARKERS: &[&str] = &[
    "auth",
    "cookie",
    "credential",
    "key",
    "password",
    "secret",
    "session",
    "signature",
    "token",
];

/// Cap on the total persisted header bytes (names + values). Servers cap
/// their own header block around 8 KB (nginx and Apache both default
/// there), so this bounds the tail — long `link` preload lists, CDN debug
/// headers — without touching any realistic response.
const MAX_STORED_RESPONSE_HEADERS_BYTES: usize = 8 * 1024;

/// Cap on a single persisted header value. One pathological value should
/// not consume the whole budget and crowd out the rest.
const MAX_STORED_RESPONSE_HEADER_VALUE_BYTES: usize = 1024;

/// Whether a response header may carry credentials and must stay off disk.
/// `name` is expected lowercase, as [`HeaderName`] guarantees.
fn is_secret_response_header(name: &str) -> bool {
    RESPONSE_HEADER_DENYLIST.contains(&name)
        || RESPONSE_HEADER_SECRET_MARKERS
            .iter()
            .any(|marker| name.contains(marker))
}

/// Inputs for [`Download::from_ytdlp`], gathered by the delegating engine
/// during extraction.
#[derive(Debug, Clone)]
pub struct YtdlpSpec {
    /// Page URL. Also becomes the download's `url`, because it is what a
    /// resume re-extracts from.
    pub source_url: Url,
    pub title: String,
    pub extractor: String,
    /// Extractor's id for the media, when it reports one. Identifies the item
    /// across the different URLs that point at it.
    pub video_id: Option<String>,
    /// Concrete format the engine must request on every run, including
    /// resumes.
    pub format_id: String,
    /// Container the chosen format produces.
    pub ext: String,
    pub size: Option<u64>,
    pub size_is_approx: bool,
    /// What the chosen format offers, kept so the download can still be
    /// described to a person once the format list is gone.
    pub quality: Quality,
    pub use_server_time: bool,
    /// Transliterate the title to ASCII before it becomes a path component.
    pub ascii_filenames: bool,
    pub headers: Option<HeaderMap>,
}

/// Result of [`Download::compute_split`]: how to resize an existing
/// part (`new_left_size`) and where the new right-hand part begins
/// (`new_right_offset` / `new_right_size`).
#[derive(Debug, Clone, Copy)]
pub struct PartSplit {
    pub new_left_size: u64,
    pub new_right_offset: u64,
    pub new_right_size: u64,
}

impl Download {
    // Getters for Download fields
    const METADATA_FILENAME: &'static str = "metadata.pb";
    const METADATA_TEMP_FILENAME: &'static str = "metadata.pb.temp";
    const LOCK_FILENAME: &'static str = "odl.lock";
    pub const PART_EXTENSION: &'static str = "part";
    pub const MIN_PART_SIZE: u64 = 300 * 1024; // 300 KB
    /// Sentinel value stored in `PartDetails.size` when the server did not
    /// report a total length (no `Content-Length`, no `Content-Range`).
    /// The downloader treats such a part as "stream until EOF" and never
    /// sends a `Range` header. Resumption / dynamic-split / grow_parts all
    /// skip parts carrying this sentinel.
    pub const UNKNOWN_PART_SIZE: u64 = u64::MAX;
    /// Assumed filesystem cluster size used to keep part boundaries aligned
    /// so the assembler can reflink parts into the final file.
    ///
    /// 4 KiB matches the page/cluster size on btrfs, xfs, ext4 and ReFS.
    /// On filesystems with larger clusters (e.g. ZFS recordsize 128 KiB)
    /// reflink fails the alignment check and the assembler falls back to
    /// a buffered copy — correct, just no CoW share.
    ///
    /// Three call sites depend on this constant; keep them in sync:
    ///   * `Download::split_parts` — initial split offsets/sizes
    ///   * `Downloader::try_split_dynamic` — mid-flight split boundary
    ///   * `download_manager::io::assemble_blocking` — reflink alignment check
    pub const ASSEMBLY_CLUSTER_SIZE: u64 = 4096;

    // Split logic assumes the minimum part size is at least one cluster,
    // otherwise the base-size round-down could produce zero and emit
    // empty leading parts.
    const _ASSERT_MIN_PART_GE_CLUSTER: () =
        assert!(Self::MIN_PART_SIZE >= Self::ASSEMBLY_CLUSTER_SIZE);

    pub fn download_dir(&self) -> &path::PathBuf {
        &self.download_dir
    }

    /// Whether a file in a download directory is odl's own bookkeeping rather
    /// than downloaded data.
    ///
    /// Engines that write files of their own choosing need this to tell their
    /// output from odl's: counting the metadata as progress would overstate
    /// it, and deleting the lockfile would break the exclusion it provides.
    pub fn is_bookkeeping_filename(name: &str) -> bool {
        matches!(
            name,
            Self::METADATA_FILENAME | Self::METADATA_TEMP_FILENAME | Self::LOCK_FILENAME
        )
    }

    pub fn part_path(&self, ulid: &str) -> path::PathBuf {
        self.download_dir
            .join(format!("{}.{}", ulid, Self::PART_EXTENSION))
    }

    pub fn set_download_dir(&mut self, path: PathBuf) {
        self.download_dir = path
    }

    pub fn lockfile_path(&self) -> path::PathBuf {
        self.download_dir.join(Self::LOCK_FILENAME)
    }

    pub fn metadata_path(&self) -> path::PathBuf {
        self.download_dir.join(Self::METADATA_FILENAME)
    }

    pub fn metadata_temp_path(&self) -> path::PathBuf {
        self.download_dir.join(Self::METADATA_TEMP_FILENAME)
    }

    pub fn final_file_path(&self) -> path::PathBuf {
        self.save_dir.join(&self.filename)
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn is_resumable(&self) -> bool {
        self.is_resumable
    }

    pub fn use_server_time(&self) -> bool {
        self.use_server_time
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn set_filename(&mut self, filename: String) {
        self.filename = filename;
    }

    /// Checksums this download will be verified against.
    ///
    /// Populated from whatever the server advertised during evaluation, plus
    /// anything [`Self::add_checksums`] contributed.
    pub fn checksums(&self) -> &[HashDigest] {
        &self.checksums
    }

    /// Stop odl from verifying this download's contents.
    ///
    /// Hashing a large file costs real time, and a caller that wants to do it
    /// on its own schedule — after the download returns, off the critical
    /// path, with its own progress — should not pay for it twice. Read
    /// [`Self::checksums`] first to keep what the server advertised, then
    /// verify with [`crate::hash::HashDigest::verify_file`] when it suits.
    ///
    /// The final file's *size* is still checked: that costs one `stat` and
    /// catches a truncated download, which is worth keeping whatever the
    /// caller intends to do about contents.
    pub fn clear_checksums(&mut self) {
        self.checksums.clear();
    }

    /// Merge additional expected checksums (e.g. user-supplied via CLI)
    /// into the instruction, skipping any already present. These are
    /// persisted to metadata and verified against the assembled file
    /// alongside any server-advertised checksums.
    pub fn add_checksums(&mut self, extra: impl IntoIterator<Item = HashDigest>) {
        for c in extra {
            if !self.checksums.contains(&c) {
                self.checksums.push(c);
            }
        }
    }

    pub fn save_dir(&self) -> &path::PathBuf {
        &self.save_dir
    }

    pub fn set_save_dir(&mut self, path: PathBuf) {
        self.save_dir = path
    }

    pub fn size(&self) -> Option<u64> {
        self.size
    }

    pub fn etag(&self) -> &Option<String> {
        &self.etag
    }

    pub fn last_modified(&self) -> Option<i64> {
        self.last_modified
    }

    pub fn last_modified_as_date(&self) -> Option<DateTime<Utc>> {
        self.last_modified
            .and_then(|x| chrono::DateTime::from_timestamp(x, 0))
    }

    pub fn requires_auth(&self) -> bool {
        self.requires_auth
    }

    pub fn requires_basic_auth(&self) -> bool {
        self.requires_basic_auth
    }

    pub fn credentials(&self) -> &Option<Credentials> {
        &self.credentials
    }

    pub fn headers(&self) -> &Option<HeaderMap> {
        &self.headers
    }

    /// Headers returned by the server during [`crate::download_manager::DownloadManager::evaluate`].
    ///
    /// `None` when no probe happened (`quick_evaluate`) and no probe was
    /// ever persisted. For an instruction rebuilt from metadata these are
    /// the filtered, capped subset that was stored — see
    /// [`Self::stored_response_headers`] — and describe the probe at
    /// [`Self::response_headers_probed_at`], which may be long past.
    pub fn response_headers(&self) -> Option<&HeaderMap> {
        self.response_headers.as_ref()
    }

    /// When [`Self::response_headers`] were observed, in unix seconds.
    ///
    /// Consumers displaying the headers should show this alongside them:
    /// the values describe one past probe, not the server's current state.
    pub fn response_headers_probed_at(&self) -> Option<i64> {
        self.response_headers_probed_at
    }

    pub fn response_headers_probed_at_as_date(&self) -> Option<DateTime<Utc>> {
        self.response_headers_probed_at
            .and_then(|x| DateTime::from_timestamp(x, 0))
    }

    pub fn max_connections(&self) -> u64 {
        self.max_connections
    }

    pub fn parts(&self) -> &HashMap<String, PartDetails> {
        &self.parts
    }

    pub fn finished(&self) -> bool {
        self.finished
    }

    /// Which engine moves the bytes for this download.
    pub fn engine(&self) -> Engine {
        self.engine
    }

    /// Engine-specific state, if the engine keeps any.
    pub fn engine_details(&self) -> Option<&EngineDetails> {
        self.engine_details.as_ref()
    }

    /// yt-dlp state, or `None` when this download uses another engine.
    pub fn ytdlp_details(&self) -> Option<&YtdlpDetails> {
        match self.engine_details.as_ref()? {
            EngineDetails::YtdlpDetails(d) => Some(d),
        }
    }

    /// Whether [`Self::size`] is an estimate rather than an exact figure.
    /// Only ever true for engines that cannot know the size up front.
    pub fn size_is_approx(&self) -> bool {
        self.ytdlp_details().is_some_and(|d| d.size_is_approx)
    }

    /// What the chosen format offers, for engines that pick between formats.
    ///
    /// `None` for engines that download whatever the URL points at, where
    /// there was no choice to describe.
    pub fn quality(&self) -> Option<Quality> {
        let d = self.ytdlp_details()?;
        // A transcript is recoverable from the pinned id alone.
        if let Some((lang, automatic)) = crate::format::parse_subtitle_format_id(&d.format_id) {
            return Some(Quality::Subtitles {
                lang: lang.to_owned(),
                automatic,
            });
        }
        Some(if let Some(height) = d.height {
            Quality::Video { height, fps: d.fps }
        } else if d.audio_only {
            Quality::Audio {
                bitrate_kbps: d.bitrate_kbps.map(|b| b.round() as u32),
            }
        } else {
            Quality::Unknown { note: None }
        })
    }

    /// The response headers as persisted: credential-bearing ones dropped,
    /// oversized values skipped, total capped at
    /// [`MAX_STORED_RESPONSE_HEADERS_BYTES`]. Server order and repeated
    /// header names are preserved.
    ///
    /// Prefer this over [`Self::response_headers`] when showing headers to a
    /// user: a live probe's map still holds `set-cookie`, bearer tokens and
    /// signed-URL material, and this applies the same filter the on-disk copy
    /// went through — so a download displays identically before and after a
    /// restart.
    pub fn stored_response_headers(&self) -> Vec<ResponseHeader> {
        let Some(headers) = &self.response_headers else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let mut budget = MAX_STORED_RESPONSE_HEADERS_BYTES;
        for (name, value) in headers.iter() {
            let name = name.as_str();
            if is_secret_response_header(name) {
                continue;
            }
            // Non-UTF8 values are rare (and undisplayable anyway); drop them
            // rather than lossily mangling what the server sent.
            let Ok(value) = value.to_str() else {
                continue;
            };
            if value.len() > MAX_STORED_RESPONSE_HEADER_VALUE_BYTES {
                continue;
            }
            let cost = name.len() + value.len();
            if cost > budget {
                break;
            }
            budget -= cost;
            out.push(ResponseHeader {
                name: name.to_string(),
                value: value.to_string(),
            });
        }
        out
    }

    pub fn from_metadata(
        download_dir: path::PathBuf,
        metadata: DownloadMetadata,
    ) -> Result<Download, MetadataError> {
        let url = Url::parse(&metadata.url).map_err(|e| MetadataError::Other {
            message: e.to_string(),
        })?;

        Ok(Self {
            download_dir,
            url,
            is_resumable: metadata.is_resumable,
            use_server_time: metadata.use_server_time,
            filename: metadata.filename, // is cleaned up before its stored as metadata, by from_response
            save_dir: PathBuf::from(metadata.save_dir),
            etag: metadata.last_etag,
            last_modified: metadata.last_modified,
            size: metadata.size,
            checksums: metadata
                .checksums
                .into_iter()
                .map(|c| c.try_into())
                .collect::<Result<Vec<HashDigest>, _>>()
                .unwrap_or_default(),
            credentials: None,
            requires_auth: metadata.requires_auth,
            requires_basic_auth: metadata.requires_basic_auth,
            headers: if metadata.headers.is_empty() {
                None
            } else {
                let mut map = HeaderMap::new();
                for (k, v) in metadata.headers {
                    if let (Ok(header_name), Ok(header_value)) =
                        (HeaderName::from_str(&k), HeaderValue::from_str(&v))
                    {
                        map.insert(header_name, header_value);
                    }
                }
                Some(map)
            },
            response_headers: if metadata.response_headers.is_empty() {
                None
            } else {
                let mut map = HeaderMap::new();
                for h in metadata.response_headers {
                    if let (Ok(name), Ok(value)) = (
                        HeaderName::from_str(&h.name),
                        HeaderValue::from_str(&h.value),
                    ) {
                        // `append`, not `insert`: a server may send the same
                        // header more than once and both are worth showing.
                        map.append(name, value);
                    }
                }
                Some(map)
            },
            response_headers_probed_at: metadata.response_headers_probed_at,
            max_connections: metadata.max_connections,
            parts: metadata.parts,
            finished: metadata.finished,
            // An unrecognised discriminant means the file was written by a
            // newer odl. Falling back to the default engine would hand a
            // foreign download to the HTTP downloader, so refuse instead.
            engine: DownloadEngine::try_from(metadata.engine).map(Engine::from).map_err(|_| {
                MetadataError::Other {
                    message: format!(
                        "unknown download engine {} in metadata; it was likely written by a newer version of odl",
                        metadata.engine
                    ),
                }
            })?,
            engine_details: metadata.engine_details,
        })
    }

    pub fn as_metadata(&self) -> DownloadMetadata {
        DownloadMetadata {
            url: self.url.to_string(),
            filename: self.filename.clone(),
            save_dir: self.save_dir.to_string_lossy().into_owned(),
            is_resumable: self.is_resumable,
            use_server_time: self.use_server_time,
            last_modified: self.last_modified,
            last_etag: self.etag.clone(),
            size: self.size,
            checksums: self
                .checksums
                .iter()
                .map(|h| h.clone().into())
                .collect::<Vec<FileChecksum>>(),
            requires_auth: self.requires_auth,
            requires_basic_auth: self.requires_basic_auth,
            headers: self
                .headers
                .as_ref()
                .map(|h| {
                    h.iter()
                        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                        .collect()
                })
                .unwrap_or_default(),
            response_headers: self.stored_response_headers(),
            response_headers_probed_at: self.response_headers_probed_at,
            max_connections: self.max_connections,
            parts: self.parts.clone(),
            finished: self.finished,
            engine: DownloadEngine::from(self.engine).into(),
            engine_details: self.engine_details.clone(),
        }
    }

    /// Build a download that a delegating engine will perform.
    ///
    /// The download directory is keyed on the *title* rather than the final
    /// filename: the container depends on which format was chosen, and a
    /// directory that moved when the user picked a different quality would
    /// orphan the partial data it holds.
    pub fn from_ytdlp(
        download_root: &std::path::Path,
        save_dir: path::PathBuf,
        spec: YtdlpSpec,
    ) -> Download {
        let YtdlpSpec {
            source_url,
            title,
            extractor,
            video_id,
            format_id,
            ext,
            size,
            size_is_approx,
            quality,
            use_server_time,
            ascii_filenames,
            headers,
        } = spec;

        let (height, fps, bitrate_kbps, audio_only) = match &quality {
            Quality::Video { height, fps } => (Some(*height), *fps, None, false),
            Quality::Audio { bitrate_kbps } => (None, None, bitrate_kbps.map(f64::from), true),
            // A subtitle choice is fully described by its pinned id, which
            // encodes the language and whether it is machine-generated, so
            // there is nothing extra to persist.
            Quality::Subtitles { .. } | Quality::Unknown { .. } => (None, None, None, false),
        };

        let dir_name = fs_utils::cleanup_filename(&title, ascii_filenames);
        let filename = fs_utils::cleanup_filename(&format!("{title}.{ext}"), ascii_filenames);

        Self {
            download_dir: download_root.join(&dir_name),
            url: source_url.clone(),
            // yt-dlp continues an interrupted transfer, and fragmented
            // formats keep their own resume state alongside the output.
            is_resumable: true,
            use_server_time,
            filename,
            save_dir,
            etag: None,
            last_modified: None,
            size,
            checksums: Vec::new(),
            credentials: None,
            requires_auth: false,
            requires_basic_auth: false,
            headers,
            // The engine never exposes the underlying HTTP exchange.
            response_headers: None,
            response_headers_probed_at: None,
            // The transfer is one opaque unit: there is no part table to
            // schedule, and the single entry exists so status reporting has
            // something to count.
            max_connections: 1,
            parts: Download::determine_parts(size, 1),
            finished: false,
            engine: Engine::Ytdlp,
            engine_details: Some(EngineDetails::YtdlpDetails(YtdlpDetails {
                source_url: source_url.to_string(),
                format_id,
                extractor,
                video_id: video_id.unwrap_or_default(),
                title,
                size_is_approx,
                height,
                fps,
                bitrate_kbps,
                audio_only,
            })),
        }
    }

    /// Mark this instruction as not yet evaluated.
    ///
    /// Used by [`crate::download_manager::DownloadManager::quick_evaluate`],
    /// whose whole point is to skip the probe: what it returns is a
    /// placeholder good enough to show in a queue, not something that can be
    /// downloaded. Evaluating the URL later produces a real instruction.
    pub(crate) fn mark_unresolved(&mut self) {
        self.engine = Engine::Unresolved;
        self.engine_details = None;
    }

    /// Everything the transfer needs beyond the probe result is taken from
    /// `opts` rather than passed alongside it: the settings travel together,
    /// and a positional list of same-typed knobs is a swap waiting to happen.
    pub fn from_response_info(
        download_dir: &std::path::Path,
        save_dir: path::PathBuf,
        response_info: ResponseInfo,
        credentials: Option<Credentials>,
        opts: &DownloadOptions,
    ) -> Download {
        let max_connections = opts.max_connections();
        let use_server_time = opts.use_server_time();
        let headers = Some(HeaderMap::from(opts));
        let filename = fs_utils::cleanup_filename(
            response_info.extract_filename().as_str(),
            opts.ascii_filenames(),
        );
        // Empty means no probe was made (`quick_evaluate`), not "server sent
        // nothing" — keep that distinguishable for downstream consumers.
        let response_headers = {
            let h = response_info.response_headers();
            (!h.is_empty()).then(|| h.clone())
        };
        Self {
            download_dir: download_dir.join(&filename),
            url: response_info.url().clone(),
            is_resumable: response_info.is_resumable(),
            use_server_time,
            filename,
            save_dir,
            etag: response_info.etag(),
            last_modified: response_info.parse_last_modified(),
            size: response_info.total_length(),
            checksums: response_info.extract_hashes(),
            credentials,
            requires_auth: response_info.requires_auth(),
            requires_basic_auth: response_info.requires_basic_auth(),
            headers,
            response_headers_probed_at: response_headers.is_some().then(|| Utc::now().timestamp()),
            response_headers,
            max_connections,
            parts: Download::determine_parts(
                response_info.total_length(),
                if response_info.is_resumable() {
                    max_connections
                } else {
                    1
                },
            ),
            finished: false,
            engine: Engine::HttpMultipart,
            engine_details: None,
        }
    }

    /// Compute a cluster-aligned split point for a part with `offset`,
    /// `size`, and `already_consumed` bytes already written/scheduled.
    /// The split favours the right half (new part) absorbing roughly the
    /// remaining bytes / 2; the left half (current part) keeps every
    /// byte up to `already_consumed`, so no progress is invalidated.
    ///
    /// Returns `None` when the resulting halves wouldn't both clear
    /// `min_part_size`, the new boundary wouldn't move past
    /// `already_consumed`, or the input `offset` is not on a cluster
    /// boundary (which would break reflink-based assembly).
    ///
    /// Reflink invariant kept:
    /// - `new_left_size` is rounded down to a multiple of
    ///   `ASSEMBLY_CLUSTER_SIZE` so the left half ends on a cluster
    ///   boundary → its reflink range stays aligned.
    /// - `new_right_offset = offset + new_left_size` stays cluster-
    ///   aligned because `offset` is required to be aligned on entry
    ///   and `new_left_size` is a cluster multiple.
    /// - `new_right_size = size - new_left_size` inherits any tail
    ///   unalignment from `size`. The original tail-unaligned part is
    ///   always the LAST in absolute-offset order, so its split right
    ///   child remains last too — Linux's `ficlonerange` allows an
    ///   unaligned tail on the final reflink range (Windows falls back
    ///   to a byte copy, same as before).
    ///
    /// Both callers — mid-flight dynamic splits in
    /// `Downloader::split_task` and the static resume-time grow in
    /// `download_manager::grow_parts` — share this geometry; only the
    /// minimum-size threshold differs.
    pub fn compute_split(
        offset: u64,
        size: u64,
        already_consumed: u64,
        min_part_size: u64,
    ) -> Option<PartSplit> {
        if !offset.is_multiple_of(Self::ASSEMBLY_CLUSTER_SIZE) {
            // Caller bug: a part whose absolute offset isn't cluster-
            // aligned can't be assembled via reflink, so refuse to split
            // it (preserving the current bad state is strictly better
            // than producing two bad parts).
            debug_assert!(
                false,
                "compute_split: offset {offset:#x} not cluster-aligned",
            );
            return None;
        }
        if already_consumed >= size {
            return None;
        }
        let remaining = size - already_consumed;
        if remaining < min_part_size * 2 {
            return None;
        }
        let candidate = already_consumed + remaining / 2;
        // Round down to a multiple of ASSEMBLY_CLUSTER_SIZE so the new
        // boundary lands on a cluster edge (reflink requirement).
        let new_left_size = candidate - candidate % Self::ASSEMBLY_CLUSTER_SIZE;
        if new_left_size <= already_consumed {
            return None;
        }
        let new_right_size = size - new_left_size;
        if new_right_size < min_part_size || new_left_size - already_consumed < min_part_size {
            return None;
        }
        Some(PartSplit {
            new_left_size,
            new_right_offset: offset + new_left_size,
            new_right_size,
        })
    }

    pub fn determine_parts(
        size: Option<u64>,
        max_connections: u64,
    ) -> HashMap<String, PartDetails> {
        let mut parts = HashMap::new();

        let max_connections = if max_connections > 0 {
            max_connections
        } else {
            1
        };

        // Unknown total length (no Content-Length / Content-Range from the
        // server). Emit a single "stream until EOF" part marked with the
        // UNKNOWN_PART_SIZE sentinel. The downloader skips Range, drains
        // the body, and rewrites part.size to the actual byte count when
        // it completes.
        if size.is_none() {
            let ulid = Ulid::generate().to_string();
            parts.insert(
                ulid.clone(),
                PartDetails {
                    offset: 0,
                    size: Self::UNKNOWN_PART_SIZE,
                    ulid,
                    finished: false,
                },
            );
            return parts;
        }

        let size = size.unwrap_or(0);

        // Always return at least one part, even if size is 0
        // If the size is small (<= MIN_PART_SIZE) we keep a single part to
        // avoid fragmenting the download into many very small requests.
        if size <= Self::MIN_PART_SIZE {
            let ulid = Ulid::generate().to_string();
            parts.insert(
                ulid.clone(),
                PartDetails {
                    offset: 0,
                    size,
                    ulid,
                    finished: size == 0,
                },
            );
            return parts;
        }

        let mut actual_connections = max_connections;
        let min_connections = size.div_ceil(Self::MIN_PART_SIZE);
        if actual_connections > min_connections {
            actual_connections = min_connections;
        }

        // Round each middle part's size down to a cluster multiple so
        // the assembler can reflink it at its absolute offset.
        let raw_base = size / actual_connections;
        let base_size = raw_base - raw_base % Self::ASSEMBLY_CLUSTER_SIZE;
        let mut offset = 0;

        // Cluster-aligned base size lets the assembler reflink each part at its
        // offset. Remainder + alignment slack go on the last part, whose tail
        // is allowed to be unaligned (the trailing copy handles it).
        for i in 0..actual_connections {
            let part_size = if i == actual_connections - 1 {
                size - offset
            } else {
                base_size
            };
            let ulid = Ulid::generate().to_string();
            parts.insert(
                ulid.clone(),
                PartDetails {
                    offset,
                    size: part_size,
                    ulid,
                    finished: false,
                },
            );
            offset += part_size;
        }

        parts
    }
}

impl PartialEq for Download {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url
            && self.download_dir == other.download_dir
            && self.filename == other.filename
    }
}

impl DownloadBuilder {
    fn validate(&self) -> Result<(), DownloadBuilderError> {
        if self.download_dir.is_none() {
            return Err(DownloadBuilderError::MissingDownloadDir);
        }
        if self.save_dir.is_none() {
            return Err(DownloadBuilderError::MissingSaveDir);
        }
        if self.url.is_none() {
            return Err(DownloadBuilderError::MissingUrl);
        }
        if self.filename.is_none() {
            return Err(DownloadBuilderError::MissingFilename);
        }
        if self
            .max_connections
            .is_none_or(|x| x == 0 || x >= Semaphore::MAX_PERMITS.try_into().unwrap_or(1_000_000))
        {
            return Err(DownloadBuilderError::InvalidNumConnections);
        }
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum DownloadBuilderError {
    #[error("download_dir is required")]
    MissingDownloadDir,
    #[error("save_dir is required")]
    MissingSaveDir,
    #[error("url is required")]
    MissingUrl,
    #[error("filename is required")]
    MissingFilename,
    #[error("max_connections must be at least 1")]
    InvalidNumConnections,
    /// Uninitialized field
    #[error("uninitialized field: {0}")]
    UninitializedField(String),
    /// Custom validation error
    #[error("validation error: {0}")]
    ValidationError(String),
}

impl From<String> for DownloadBuilderError {
    fn from(s: String) -> Self {
        Self::ValidationError(s)
    }
}

impl From<UninitializedFieldError> for DownloadBuilderError {
    fn from(ufe: UninitializedFieldError) -> Self {
        Self::UninitializedField(ufe.to_string())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn test_download(checksums: Vec<HashDigest>) -> Download {
        DownloadBuilder::default()
            .download_dir(PathBuf::from("/tmp/dl"))
            .save_dir(PathBuf::from("/tmp/save"))
            .url(Url::parse("https://example.com/file").unwrap())
            .filename("file".to_string())
            .max_connections(1)
            .checksums(checksums)
            .parts(Download::determine_parts(Some(0), 1))
            .build()
            .unwrap()
    }

    fn download_with_response_headers(headers: Vec<(&str, &str)>) -> Download {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.append(
                HeaderName::from_str(name).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        let mut dl = test_download(vec![]);
        dl.response_headers = Some(map);
        dl.response_headers_probed_at = Some(1_700_000_000);
        dl
    }

    #[test]
    fn stored_response_headers_drops_credential_bearing_ones() {
        let dl = download_with_response_headers(vec![
            ("content-type", "application/zip"),
            ("set-cookie", "session=secret"),
            ("www-authenticate", "Basic realm=\"x\""),
            // Vendor headers no denylist can enumerate — caught by marker.
            ("x-amz-security-token", "AQoDYXdz"),
            ("x-api-key", "k-123"),
            ("x-goog-signature", "deadbeef"),
            ("x-cache", "HIT"),
        ]);

        let stored: Vec<String> = dl
            .stored_response_headers()
            .into_iter()
            .map(|h| h.name)
            .collect();

        assert_eq!(stored, ["content-type", "x-cache"]);
    }

    #[test]
    fn stored_response_headers_respects_caps() {
        let huge = "v".repeat(MAX_STORED_RESPONSE_HEADER_VALUE_BYTES + 1);
        let dl = download_with_response_headers(vec![
            ("x-huge", huge.as_str()),
            ("content-type", "application/zip"),
        ]);
        let stored = dl.stored_response_headers();
        assert_eq!(stored.len(), 1, "oversized value must be skipped");
        assert_eq!(stored[0].name, "content-type");

        // Many mid-sized headers: accumulation stops at the total budget.
        let value = "v".repeat(512);
        let names: Vec<String> = (0..40).map(|i| format!("x-pad-{i}")).collect();
        let dl = download_with_response_headers(
            names.iter().map(|n| (n.as_str(), value.as_str())).collect(),
        );
        let stored = dl.stored_response_headers();
        let total: usize = stored.iter().map(|h| h.name.len() + h.value.len()).sum();
        assert!(total <= MAX_STORED_RESPONSE_HEADERS_BYTES, "total {total}");
        assert!(!stored.is_empty(), "budget must fit at least some headers");
        assert!(stored.len() < names.len(), "budget must actually bind");
    }

    #[test]
    fn response_headers_round_trip_through_metadata() {
        let dl = download_with_response_headers(vec![
            ("content-type", "application/zip"),
            ("x-trace", "first"),
            ("x-trace", "second"),
            ("set-cookie", "session=secret"),
        ]);

        let metadata = dl.as_metadata();
        assert!(
            !metadata
                .response_headers
                .iter()
                .any(|h| h.name == "set-cookie"),
            "filtering must apply on the way to disk"
        );
        assert_eq!(metadata.response_headers_probed_at, Some(1_700_000_000));

        let restored = Download::from_metadata(PathBuf::from("/tmp/dl"), metadata).unwrap();
        let headers = restored.response_headers().expect("headers survive resume");
        assert_eq!(headers.get("content-type").unwrap(), "application/zip");
        let traces: Vec<&str> = headers
            .get_all("x-trace")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(traces, vec!["first", "second"], "duplicates preserved");
        assert!(headers.get("set-cookie").is_none());
        assert_eq!(restored.response_headers_probed_at(), Some(1_700_000_000));
    }

    #[test]
    fn add_checksums_merges_and_dedups() {
        use crate::hash::HashEncoding;
        // Instruction already carries a server-advertised SHA256.
        let server = HashDigest::SHA256("aa".repeat(32), HashEncoding::Hex);
        let mut dl = test_download(vec![server.clone()]);

        // User supplies the identical SHA256 again (must dedup against the
        // seeded one) plus a brand-new MD5 (must be kept).
        let user_new = HashDigest::MD5("bb".repeat(16), HashEncoding::Hex);
        dl.add_checksums(vec![server.clone(), user_new.clone()]);

        assert_eq!(dl.checksums, vec![server, user_new]);
    }

    #[test]
    fn add_checksums_into_empty() {
        use crate::hash::HashEncoding;
        let mut dl = test_download(vec![]);
        let c = HashDigest::SHA512("cc".repeat(64), HashEncoding::Hex);
        dl.add_checksums(vec![c.clone()]);
        assert_eq!(dl.checksums, vec![c]);
    }

    #[test]
    fn test_determine_parts_unknown_size_streams_until_eof() {
        // No Content-Length / Content-Range from the server (typical for
        // chunked HTML, gzipped responses where reqwest strips the length,
        // etc.) must yield a single unfinished part flagged with the
        // UNKNOWN_PART_SIZE sentinel so the downloader streams to EOF
        // instead of treating it as a zero-byte file already complete.
        let parts = Download::determine_parts(None, 4);
        assert_eq!(parts.len(), 1);
        let part = parts.values().next().unwrap();
        assert_eq!(part.offset, 0);
        assert_eq!(part.size, Download::UNKNOWN_PART_SIZE);
        assert!(!part.finished);
    }

    #[test]
    fn test_determine_parts_zero_size() {
        let parts = Download::determine_parts(Some(0), 4);
        assert_eq!(parts.len(), 1);
        let part_vec: Vec<_> = parts.values().collect();
        let part = part_vec[0];
        assert_eq!(part.offset, 0);
        assert_eq!(part.size, 0);
        assert!(part.finished);
    }

    #[test]
    fn test_determine_parts_zero_connections() {
        let parts = Download::determine_parts(Some(1024 * 1024), 0);
        assert_eq!(parts.len(), 1);
        let part_vec: Vec<_> = parts.values().collect();
        let part = part_vec[0];
        assert_eq!(part.offset, 0);
        assert_eq!(part.size, 1024 * 1024);
        assert!(!part.finished);
    }

    #[test]
    fn test_determine_parts_small_file() {
        // File smaller than MIN_PART_SIZE (300 KB)
        let size = 200 * 1024;
        let parts = Download::determine_parts(Some(size), 4);
        assert_eq!(parts.len(), 1);
        let part_vec: Vec<_> = parts.values().collect();
        let part = part_vec[0];
        assert_eq!(part.offset, 0);
        assert_eq!(part.size, size);
        assert!(!part.finished);
    }

    #[test]
    fn test_determine_parts_exact_min_part_size() {
        let size = 300 * 1024;
        let parts = Download::determine_parts(Some(size), 4);
        assert_eq!(parts.len(), 1);
        let part_vec: Vec<_> = parts.values().collect();
        let part = part_vec[0];
        assert_eq!(part.offset, 0);
        assert_eq!(part.size, size);
        assert!(!part.finished);
    }

    #[test]
    fn test_determine_parts_even_split() {
        // 1 MB file, 4 connections
        let size = 1024 * 1024;
        let max_connections = 4;
        let parts = Download::determine_parts(Some(size), max_connections);
        assert_eq!(parts.len(), max_connections as usize);
        let mut part_vec: Vec<_> = parts.values().collect();
        part_vec.sort_by_key(|p| p.offset);
        let total: u64 = part_vec.iter().map(|p| p.size).sum();
        assert_eq!(total, size);
        assert_eq!(part_vec[0].offset, 0);
        assert_eq!(part_vec[1].offset, part_vec[0].size);
        assert_eq!(part_vec[2].offset, part_vec[0].size + part_vec[1].size);
        assert_eq!(
            part_vec[3].offset,
            part_vec[0].size + part_vec[1].size + part_vec[2].size
        );
    }

    #[test]
    fn test_determine_parts_uneven_split() {
        // 1 MB + 123 bytes, 3 connections
        let size = 1024 * 1024 + 123;
        let max_connections = 3;
        let parts = Download::determine_parts(Some(size), max_connections);
        assert_eq!(parts.len(), max_connections as usize);
        let mut part_vec: Vec<_> = parts.values().collect();
        part_vec.sort_by_key(|p| p.offset);
        let total: u64 = part_vec.iter().map(|p| p.size).sum();
        assert_eq!(total, size);
        // The last part absorbs the remainder so middle offsets stay
        // cluster-aligned for reflink-based assembly.
        assert!(part_vec[2].size >= part_vec[1].size);
        assert_eq!(part_vec[0].size, part_vec[1].size);
    }

    #[test]
    fn test_determine_parts_too_many_connections() {
        // File size is such that min_connections < max_connections
        let size = 900 * 1024; // 900 KB
        let max_connections = 10;
        let parts = Download::determine_parts(Some(size), max_connections);
        // Should not exceed min_connections (3)
        assert_eq!(parts.len(), 3);
        let mut part_vec: Vec<_> = parts.values().collect();
        part_vec.sort_by_key(|p| p.offset);
        let total: u64 = part_vec.iter().map(|p| p.size).sum();
        assert_eq!(total, size);
    }

    #[test]
    fn test_determine_parts_800kb_file() {
        // 800 KB file, should be split into 3 parts (since MIN_PART_SIZE is 300 KB)
        let size = 800 * 1024;
        let max_connections = 10; // More than needed, should be capped by min_connections
        let parts = Download::determine_parts(Some(size), max_connections);
        // 800 KB / 300 KB = 2.66..., so should be 3 parts
        assert_eq!(parts.len(), 3);
        let mut part_vec: Vec<_> = parts.values().collect();
        part_vec.sort_by_key(|p| p.offset);
        let total: u64 = part_vec.iter().map(|p| p.size).sum();
        assert_eq!(total, size);

        // Check offsets are correct and contiguous
        assert_eq!(part_vec[0].offset, 0);
        assert_eq!(part_vec[1].offset, part_vec[0].offset + part_vec[0].size);
        assert_eq!(part_vec[2].offset, part_vec[1].offset + part_vec[1].size);

        // The last part absorbs the remainder; preceding parts share the
        // cluster-aligned base size.
        assert_eq!(part_vec[0].size, part_vec[1].size);
        assert!(part_vec[2].size >= part_vec[1].size);
        assert_eq!(part_vec[0].offset % Download::ASSEMBLY_CLUSTER_SIZE, 0);
        assert_eq!(part_vec[1].offset % Download::ASSEMBLY_CLUSTER_SIZE, 0);
        assert_eq!(part_vec[2].offset % Download::ASSEMBLY_CLUSTER_SIZE, 0);
    }

    #[test]
    fn compute_split_returns_none_when_remaining_below_double_min() {
        // remaining = size - already = MIN_PART_SIZE * 2 - 1
        let size = Download::MIN_PART_SIZE * 2 - 1;
        assert!(Download::compute_split(0, size, 0, Download::MIN_PART_SIZE).is_none());
    }

    #[test]
    fn compute_split_aligns_boundary_and_preserves_total() {
        let size = Download::MIN_PART_SIZE * 8;
        let split = Download::compute_split(1024 * 1024, size, 0, Download::MIN_PART_SIZE)
            .expect("split expected");
        // Left half aligned to cluster boundary
        assert_eq!(split.new_left_size % Download::ASSEMBLY_CLUSTER_SIZE, 0);
        // Total bytes preserved
        assert_eq!(split.new_left_size + split.new_right_size, size);
        // New offset = base offset + left size
        assert_eq!(split.new_right_offset, 1024 * 1024 + split.new_left_size);
        // Both halves above min
        assert!(split.new_left_size >= Download::MIN_PART_SIZE);
        assert!(split.new_right_size >= Download::MIN_PART_SIZE);
    }

    #[test]
    fn compute_split_keeps_offsets_cluster_aligned_for_reflink() {
        // Start from a determine_parts result (which guarantees all
        // middle offsets are cluster-aligned) and recursively split the
        // largest unfinished candidate. Every produced offset must stay
        // cluster-aligned so the assembler can reflink.
        let size = 50 * 1024 * 1024 + 1234; // 50 MiB + unaligned tail
        let mut parts = Download::determine_parts(Some(size), 4);

        for _ in 0..8 {
            let candidate = parts
                .values()
                .filter_map(|p| {
                    Download::compute_split(p.offset, p.size, 0, Download::MIN_PART_SIZE)
                        .map(|s| (p.ulid.clone(), p.offset, p.size, s))
                })
                .max_by_key(|(_, _, _, s)| s.new_right_size);
            let Some((ulid, _, _, split)) = candidate else {
                break;
            };
            // Update left
            if let Some(p) = parts.get_mut(&ulid) {
                p.size = split.new_left_size;
            }
            // Insert right
            let new_ulid = ulid::Ulid::generate().to_string();
            parts.insert(
                new_ulid.clone(),
                crate::download_metadata::PartDetails {
                    offset: split.new_right_offset,
                    size: split.new_right_size,
                    ulid: new_ulid,
                    finished: false,
                },
            );
        }

        // All offsets must be cluster-aligned for reflink.
        for p in parts.values() {
            assert_eq!(
                p.offset % Download::ASSEMBLY_CLUSTER_SIZE,
                0,
                "offset {} broke cluster alignment after split",
                p.offset
            );
        }
        // Coverage preserved.
        let total: u64 = parts.values().map(|p| p.size).sum();
        assert_eq!(total, size);

        // Among the parts, the last-by-offset is the only one allowed
        // to have unaligned size. Every other must be cluster-aligned
        // size to keep its reflink range fully aligned.
        let mut sorted: Vec<_> = parts.values().collect();
        sorted.sort_by_key(|p| p.offset);
        for p in &sorted[..sorted.len() - 1] {
            assert_eq!(
                p.size % Download::ASSEMBLY_CLUSTER_SIZE,
                0,
                "non-last part size {} broke cluster alignment",
                p.size
            );
        }
    }

    #[test]
    fn compute_split_respects_already_consumed_floor() {
        // Already consumed half; remainder must still be splittable.
        let size = Download::MIN_PART_SIZE * 8;
        let consumed = Download::MIN_PART_SIZE * 4;
        let split = Download::compute_split(0, size, consumed, Download::MIN_PART_SIZE)
            .expect("split expected");
        assert!(
            split.new_left_size > consumed,
            "boundary must move past already-consumed prefix"
        );
        assert_eq!(split.new_left_size + split.new_right_size, size);
    }

    /// Part filenames are generated identifiers that end up on disk and in
    /// persisted metadata, so their shape is a compatibility contract: a
    /// download interrupted by one version has to be resumable by the next.
    /// This pins it against the identifier crate changing underneath us.
    #[test]
    fn part_names_keep_the_shape_already_written_to_disk() {
        const CROCKFORD: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

        let parts = Download::determine_parts(Some(Download::MIN_PART_SIZE * 8), 4);
        assert!(!parts.is_empty());
        for name in parts.keys() {
            assert_eq!(name.len(), 26, "part name {name} is not 26 characters");
            assert!(
                name.chars().all(|c| CROCKFORD.contains(c)),
                "part name {name} leaves Crockford base32"
            );
        }

        // And a name written by an older version still addresses its file, so
        // an in-flight download survives the upgrade.
        let download = test_download(vec![]);
        let legacy = "01D39ZY06FGSCTVN4T2V9PKHFZ";
        assert!(
            download
                .part_path(legacy)
                .to_string_lossy()
                .ends_with("01D39ZY06FGSCTVN4T2V9PKHFZ.part")
        );
    }

    /// Metadata written before the engine field existed must keep loading as
    /// an ordinary HTTP download. Every download already on a user's disk
    /// depends on this.
    #[test]
    fn metadata_without_engine_fields_loads_as_http_multipart() {
        use prost::Message;

        let mut legacy = test_download(vec![]).as_metadata();
        // Simulate a file written by the previous version: the fields simply
        // were not there, which on the wire is indistinguishable from their
        // defaults.
        legacy.engine = 0;
        legacy.engine_details = None;
        let encoded = legacy.encode_to_vec();

        let decoded = DownloadMetadata::decode(encoded.as_slice()).expect("decode");
        let download = Download::from_metadata(PathBuf::from("/tmp/dl"), decoded).expect("load");

        assert_eq!(download.engine(), Engine::HttpMultipart);
        assert!(download.engine_details().is_none());
        assert!(!download.size_is_approx());
    }

    #[test]
    fn engine_details_round_trip_through_metadata() {
        let mut download = test_download(vec![]);
        download.engine = Engine::Ytdlp;
        download.engine_details = Some(EngineDetails::YtdlpDetails(YtdlpDetails {
            source_url: "https://www.youtube.com/watch?v=x".to_owned(),
            format_id: "137+251".to_owned(),
            extractor: "youtube".to_owned(),
            video_id: "x".to_owned(),
            title: "A Title".to_owned(),
            size_is_approx: true,
            height: Some(1080),
            fps: Some(60.0),
            bitrate_kbps: None,
            audio_only: false,
        }));

        let restored = Download::from_metadata(PathBuf::from("/tmp/dl"), download.as_metadata())
            .expect("load");

        assert_eq!(restored.engine(), Engine::Ytdlp);
        let details = restored.ytdlp_details().expect("ytdlp details");
        // The pinned format is what makes a resume safe; losing it in a
        // round-trip would let a resume append bytes of a different format.
        assert_eq!(details.format_id, "137+251");
        assert_eq!(details.source_url, "https://www.youtube.com/watch?v=x");
        assert_eq!(details.video_id, "x");
        assert!(restored.size_is_approx());
    }

    #[test]
    fn unknown_engine_is_rejected_rather_than_defaulted() {
        // A file from a future odl using an engine we cannot drive. Silently
        // treating it as HTTP would hand a foreign download to the wrong
        // downloader.
        let mut metadata = test_download(vec![]).as_metadata();
        metadata.engine = 9999;
        assert!(Download::from_metadata(PathBuf::from("/tmp/dl"), metadata).is_err());
    }

    #[tokio::test]
    async fn checksums_can_be_taken_over_by_the_caller() {
        use crate::hash::HashDigest;

        let mut download = test_download(vec![
            HashDigest::parse_cli(&format!("sha256:{}", "ab".repeat(32))).unwrap(),
        ]);
        // Readable, so a caller can keep what the server advertised before
        // taking responsibility for checking it.
        assert_eq!(download.checksums().len(), 1);

        download.clear_checksums();
        assert!(download.checksums().is_empty());
        // And gone from what gets persisted, so a resume does not reinstate
        // verification the caller opted out of.
        assert!(download.as_metadata().checksums.is_empty());
    }
}
