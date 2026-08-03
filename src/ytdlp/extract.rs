//! Turning a page URL into everything needed to plan a download.
//!
//! One `yt-dlp -J` run does the whole job: it reports the media's title, the
//! full format list for a selector to choose from, and — because a format
//! selector is passed in the same invocation — the *resolved* choice with its
//! final container and size already worked out. Predicting the muxed
//! container ourselves is then only needed when a caller overrides the
//! default.

use crate::config::YtdlpOptions;
use crate::error::YtdlpError;
use crate::format::{FormatOffer, MediaFormat, Quality, SubtitleTrack};
use crate::ytdlp::binary::{self, Tools};
use reqwest::Url;
use serde::Deserialize;
use std::path::Path;

/// Cap on captured stderr, so a pathological failure cannot buffer without
/// bound before we turn it into an error message.
const MAX_STDERR_BYTES: usize = 16 * 1024;

/// What extraction learned about a single media item.
#[derive(Debug, Clone)]
pub struct ExtractedInfo {
    /// Page URL this came from. Persisted, because a resume re-extracts from
    /// it rather than reusing an expired media URL.
    pub source_url: Url,
    pub title: String,
    pub extractor: String,
    /// Format id yt-dlp resolved for the selector we passed.
    pub default_format_id: Option<String>,
    /// Container of the resolved selection, after any muxing.
    pub default_ext: Option<String>,
    /// Size of the resolved selection.
    pub size: Option<u64>,
    pub size_is_approx: bool,
    pub formats: Vec<MediaFormat>,
    /// Cover image, for a queue row or a details pane.
    pub thumbnail: Option<String>,
    /// Runtime in seconds, when the extractor reports one.
    pub duration_seconds: Option<f64>,
    /// Channel / account that published the media.
    pub uploader: Option<String>,
    /// Publication date as `YYYYMMDD`, the form extractors emit.
    pub upload_date: Option<String>,
    /// Transcript tracks, author-supplied ones first.
    pub subtitles: Vec<SubtitleTrack>,
}

impl ExtractedInfo {
    /// Present this to a [`crate::format::FormatSelector`].
    pub fn offer(&self, can_merge: bool) -> FormatOffer {
        FormatOffer {
            source_url: self.source_url.clone(),
            title: self.title.clone(),
            formats: self.formats.clone(),
            default_id: self.default_format_id.clone(),
            default_ext: self.default_ext.clone(),
            subtitles: self.subtitles.clone(),
            can_merge,
        }
    }

    /// Container that will result from downloading `format_id`.
    ///
    /// Uses yt-dlp's own answer when the id is the one it resolved, and
    /// otherwise derives it from the parts being muxed.
    pub fn ext_for(&self, format_id: &str) -> String {
        if let Some((lang, automatic)) = crate::format::parse_subtitle_format_id(format_id) {
            return self
                .subtitles
                .iter()
                .find(|t| t.lang == lang && t.automatic == automatic)
                .map(|t| t.ext.clone())
                .unwrap_or_else(|| "vtt".to_owned());
        }
        if self.default_format_id.as_deref() == Some(format_id)
            && let Some(ext) = &self.default_ext
        {
            return ext.clone();
        }
        let parts = crate::format::split_id(format_id);
        let exts: Vec<&str> = parts
            .iter()
            .filter_map(|id| self.formats.iter().find(|f| f.id == *id))
            .map(|f| f.ext.as_str())
            .collect();
        if exts.len() != parts.len() {
            // At least one part is an id we have no entry for. Guessing from
            // the parts we do know would name a container that cannot hold
            // the rest, so fall back to the one that accepts anything.
            return "mkv".to_owned();
        }
        crate::format::merged_ext(&exts)
    }

    /// What `format_id` offers, in the terms a person judges it by.
    ///
    /// For a compound id the picture comes from whichever part carries video,
    /// since that is the half a viewer is choosing between.
    pub fn quality_for(&self, format_id: &str) -> Quality {
        if let Some((lang, automatic)) = crate::format::parse_subtitle_format_id(format_id) {
            return Quality::Subtitles {
                lang: lang.to_owned(),
                automatic,
            };
        }
        let parts: Vec<&MediaFormat> = crate::format::split_id(format_id)
            .iter()
            .filter_map(|id| self.formats.iter().find(|f| f.id == *id))
            .collect();
        if let Some(video) = parts.iter().find(|f| f.has_video) {
            return video.quality();
        }
        match parts.first() {
            Some(f) => f.quality(),
            None => crate::format::Quality::Unknown {
                note: Some(format_id.to_owned()),
            },
        }
    }

    /// Size of `format_id`, summing the parts of a compound id.
    ///
    /// Returns `(size, is_approx)`; `is_approx` is true when any part was an
    /// estimate, since the sum is then no better than its worst term.
    pub fn size_for(&self, format_id: &str) -> (Option<u64>, bool) {
        // Extractors report no size for a transcript, and it is small enough
        // that an invented figure would only mislead a progress bar.
        if crate::format::parse_subtitle_format_id(format_id).is_some() {
            return (None, false);
        }
        if self.default_format_id.as_deref() == Some(format_id) && self.size.is_some() {
            return (self.size, self.size_is_approx);
        }
        let mut total: u64 = 0;
        let mut approx = false;
        let mut known = false;
        for id in crate::format::split_id(format_id) {
            let Some(f) = self.formats.iter().find(|f| f.id == id) else {
                continue;
            };
            match f.size {
                Some(s) => {
                    total = total.saturating_add(s);
                    approx |= f.size_is_approx;
                    known = true;
                }
                // A missing size makes the whole sum a lower bound, which is
                // worse than admitting we do not know.
                None => return (None, true),
            }
        }
        if known {
            (Some(total), approx)
        } else {
            (None, true)
        }
    }
}

/// Machine-generated caption languages are capped: sites publish translated
/// captions into a hundred languages, and listing them all would bury the
/// handful of real choices.
const MAX_AUTOMATIC_CAPTION_LANGS: usize = 3;

/// Turn the extractor's language maps into a flat, ordered track list.
///
/// Author-supplied tracks come first and are kept in full; automatic captions
/// are a fallback and are capped, since a menu is only useful while it stays
/// readable. A specific language beyond the cap is still reachable by naming
/// its id directly.
fn collect_subtitles(
    manual: std::collections::HashMap<String, Vec<RawSubtitle>>,
    automatic: std::collections::HashMap<String, Vec<RawSubtitle>>,
) -> Vec<SubtitleTrack> {
    fn ext_of(shapes: &[RawSubtitle]) -> Option<String> {
        // Prefer the widely supported text formats over site-specific ones
        // such as YouTube's `json3` / `srv1`, which few players read.
        for wanted in ["srt", "vtt", "ass"] {
            if shapes.iter().any(|s| s.ext.as_deref() == Some(wanted)) {
                return Some(wanted.to_owned());
            }
        }
        shapes.first().and_then(|s| s.ext.clone())
    }

    let mut out = Vec::new();
    let mut langs: Vec<String> = manual.keys().cloned().collect();
    langs.sort();
    for lang in langs {
        // yt-dlp lists "live_chat" alongside subtitles; it is a chat replay,
        // not a transcript.
        if lang == "live_chat" {
            continue;
        }
        if let Some(ext) = ext_of(&manual[&lang]) {
            out.push(SubtitleTrack {
                lang,
                ext,
                automatic: false,
            });
        }
    }

    let mut auto_langs: Vec<String> = automatic.keys().cloned().collect();
    auto_langs.sort();
    for lang in auto_langs.into_iter().take(MAX_AUTOMATIC_CAPTION_LANGS) {
        if out.iter().any(|t| t.lang == lang) {
            continue;
        }
        if let Some(ext) = ext_of(&automatic[&lang]) {
            out.push(SubtitleTrack {
                lang,
                ext,
                automatic: true,
            });
        }
    }
    out
}

/// Format selector handed to yt-dlp when the caller configured none.
///
/// With a muxer available, the best video and audio are combined, falling
/// back to the best self-contained format. Without one, only self-contained
/// formats are requestable at all.
pub fn default_selector(can_merge: bool) -> &'static str {
    if can_merge { "bv*+ba/b" } else { "b" }
}

#[derive(Debug, Deserialize)]
struct RawInfo {
    #[serde(rename = "_type", default)]
    kind: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    extractor: Option<String>,
    #[serde(default)]
    extractor_key: Option<String>,
    #[serde(default)]
    ext: Option<String>,
    #[serde(default)]
    format_id: Option<String>,
    #[serde(default)]
    filesize: Option<f64>,
    #[serde(default)]
    filesize_approx: Option<f64>,
    #[serde(default)]
    formats: Vec<RawFormat>,
    #[serde(default)]
    entries: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    thumbnail: Option<String>,
    #[serde(default)]
    duration: Option<f64>,
    #[serde(default)]
    uploader: Option<String>,
    #[serde(default)]
    upload_date: Option<String>,
    /// Language tag -> the shapes that language is served in.
    #[serde(default)]
    subtitles: std::collections::HashMap<String, Vec<RawSubtitle>>,
    #[serde(default)]
    automatic_captions: std::collections::HashMap<String, Vec<RawSubtitle>>,
}

#[derive(Debug, Deserialize)]
struct RawSubtitle {
    #[serde(default)]
    ext: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawFormat {
    format_id: String,
    #[serde(default)]
    ext: Option<String>,
    #[serde(default)]
    vcodec: Option<String>,
    #[serde(default)]
    acodec: Option<String>,
    // Numeric fields are declared as f64 because extractors are inconsistent
    // about emitting integers vs floats for the same field.
    #[serde(default)]
    height: Option<f64>,
    #[serde(default)]
    fps: Option<f64>,
    #[serde(default)]
    tbr: Option<f64>,
    #[serde(default)]
    filesize: Option<f64>,
    #[serde(default)]
    filesize_approx: Option<f64>,
    #[serde(default)]
    format_note: Option<String>,
}

/// yt-dlp writes `"none"` rather than omitting the codec when a stream is
/// absent.
fn has_stream(codec: &Option<String>) -> bool {
    match codec.as_deref() {
        None | Some("none") | Some("") => false,
        Some(_) => true,
    }
}

fn to_u64(v: Option<f64>) -> Option<u64> {
    v.filter(|n| n.is_finite() && *n >= 0.0).map(|n| n as u64)
}

impl From<RawFormat> for MediaFormat {
    fn from(r: RawFormat) -> Self {
        let (size, size_is_approx) = match (to_u64(r.filesize), to_u64(r.filesize_approx)) {
            (Some(s), _) => (Some(s), false),
            (None, Some(s)) => (Some(s), true),
            (None, None) => (None, false),
        };
        MediaFormat {
            id: r.format_id,
            ext: r.ext.unwrap_or_else(|| "bin".to_owned()),
            height: to_u64(r.height).map(|h| h as u32).filter(|h| *h > 0),
            fps: r.fps.filter(|f| f.is_finite() && *f > 0.0),
            tbr: r.tbr.filter(|t| t.is_finite() && *t > 0.0),
            size,
            size_is_approx,
            has_video: has_stream(&r.vcodec),
            has_audio: has_stream(&r.acodec),
            note: r.format_note,
        }
    }
}

/// Parse a `yt-dlp -J` document.
///
/// Split from the process call so the mapping can be tested against captured
/// documents without running anything.
pub fn parse_info(source_url: &Url, json: &[u8]) -> Result<ExtractedInfo, YtdlpError> {
    let raw: RawInfo = serde_json::from_slice(json).map_err(|e| YtdlpError::Other {
        message: format!("could not parse yt-dlp output: {e}"),
    })?;

    if raw.kind.as_deref() == Some("playlist") || raw.entries.is_some() {
        return Err(YtdlpError::Other {
            message: "this URL is a playlist, which is not supported yet".to_owned(),
        });
    }

    let formats: Vec<MediaFormat> = raw.formats.into_iter().map(MediaFormat::from).collect();

    // Subtitle choices live in the same field as media format ids, told apart
    // by a reserved prefix. If a site ever published a real format id inside
    // that namespace the two would be indistinguishable, and a transcript
    // request could silently fetch a video. Refuse rather than guess.
    if let Some(clash) = formats.iter().find(|f| {
        crate::format::SUBTITLE_ID_PREFIXES
            .iter()
            .any(|p| f.id.starts_with(p))
    }) {
        return Err(YtdlpError::Other {
            message: format!(
                "extractor reported a format id `{}` that collides with odl's reserved subtitle namespace; refusing to guess which was meant",
                clash.id
            ),
        });
    }

    if formats.is_empty() {
        return Err(YtdlpError::Other {
            message: "yt-dlp reported no downloadable formats for this URL".to_owned(),
        });
    }

    let (size, size_is_approx) = match (to_u64(raw.filesize), to_u64(raw.filesize_approx)) {
        (Some(s), _) => (Some(s), false),
        (None, Some(s)) => (Some(s), true),
        (None, None) => (None, false),
    };

    Ok(ExtractedInfo {
        source_url: source_url.clone(),
        title: raw
            .title
            .filter(|t| !t.trim().is_empty())
            // A title is only ever a display/filename hint; falling back to the
            // URL keeps a nameless extraction usable.
            .unwrap_or_else(|| source_url.as_str().to_owned()),
        extractor: raw
            .extractor
            .or(raw.extractor_key)
            .unwrap_or_else(|| "unknown".to_owned()),
        default_format_id: raw.format_id,
        default_ext: raw.ext,
        size,
        size_is_approx,
        formats,
        subtitles: collect_subtitles(raw.subtitles, raw.automatic_captions),
        thumbnail: raw.thumbnail.filter(|t| !t.trim().is_empty()),
        duration_seconds: raw.duration.filter(|d| d.is_finite() && *d > 0.0),
        uploader: raw.uploader.filter(|u| !u.trim().is_empty()),
        upload_date: raw.upload_date.filter(|d| !d.trim().is_empty()),
    })
}

/// Shared flags for every yt-dlp invocation.
///
/// `extra_args` are appended last so a user override wins over our defaults.
pub fn base_args(opts: &YtdlpOptions, tools: &Tools, proxy: Option<&str>) -> Vec<String> {
    let mut args: Vec<String> = vec![
        // A link that also names a playlist means the video, not the playlist.
        "--no-playlist".into(),
        "--no-colors".into(),
        // Warnings on stderr are informational and would otherwise look like
        // failure output when a command does fail.
        "--no-warnings".into(),
        // Never read a user's yt-dlp config: it can redirect output paths and
        // post-processing in ways that would break the contract with odl.
        "--ignore-config".into(),
    ];
    if let Some(p) = proxy {
        // Covers extraction as well as transfer; without it, extraction would
        // leak around a proxy the user configured for downloads.
        args.push("--proxy".into());
        args.push(p.to_owned());
    }
    if let Some(ffmpeg) = &tools.ffmpeg {
        args.push("--ffmpeg-location".into());
        args.push(ffmpeg.display().to_string());
    }
    if let Some(browser) = opts.cookies_from_browser() {
        args.push("--cookies-from-browser".into());
        args.push(browser.to_owned());
    }
    args.extend(opts.extra_args().iter().cloned());
    args
}

/// Run `yt-dlp -J` and parse the result.
///
/// `selector` is passed through so the returned document already carries the
/// resolved format, container and size for the default choice.
pub async fn extract(
    url: &Url,
    opts: &YtdlpOptions,
    tools: &Tools,
    proxy: Option<&str>,
    selector: &str,
) -> Result<ExtractedInfo, YtdlpError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(YtdlpError::Other {
            message: format!("refusing to hand a {} URL to yt-dlp", url.scheme()),
        });
    }

    let mut cmd = binary::command(&tools.ytdlp);
    cmd.args(base_args(opts, tools, proxy));
    cmd.arg("-J");
    cmd.arg("-f").arg(selector);
    // `--` and then the URL: a URL is data, and must never be read as a flag
    // however it is spelled.
    cmd.arg("--").arg(url.as_str());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let output = cmd.output().await.map_err(|e| YtdlpError::NotUsable {
        path: tools.ytdlp.display().to_string(),
        message: e.to_string(),
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let trimmed: String = stderr.chars().take(MAX_STDERR_BYTES).collect();
        if trimmed.contains("Unsupported URL") {
            return Err(YtdlpError::UnsupportedUrl);
        }
        if crate::ytdlp::run::is_rate_limited(&trimmed) {
            return Err(YtdlpError::RateLimited {
                detail: last_meaningful_line(&trimmed),
            });
        }
        return Err(YtdlpError::ProcessFailed {
            code: output.status.code(),
            stderr: last_meaningful_line(&trimmed),
        });
    }

    parse_info(url, &output.stdout)
}

/// The last non-empty line of captured stderr, which is where yt-dlp puts the
/// actual reason. Earlier lines are usually progress noise.
pub fn last_meaningful_line(stderr: &str) -> Option<String> {
    stderr
        .lines()
        .map(str::trim)
        .rfind(|l| !l.is_empty())
        .map(|l| l.to_owned())
}

/// Whether a muxer is available for this configuration.
pub fn can_merge(opts: &YtdlpOptions) -> bool {
    binary::is_available(opts.ffmpeg_path(), "ffmpeg")
}

/// Path of the file yt-dlp records its final output path into.
pub fn output_path_file(download_dir: &Path) -> std::path::PathBuf {
    download_dir.join(crate::ytdlp::run::OUTPUT_PATH_FILENAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url() -> Url {
        Url::parse("https://www.youtube.com/watch?v=abc").unwrap()
    }

    const SAMPLE: &str = r#"{
        "id": "abc",
        "title": "Some Video",
        "extractor": "youtube",
        "ext": "mkv",
        "format_id": "137+251",
        "filesize_approx": 12345678.0,
        "formats": [
            {"format_id": "18", "ext": "mp4", "vcodec": "avc1", "acodec": "mp4a",
             "height": 360, "fps": 30, "tbr": 600.5, "filesize": 5000000},
            {"format_id": "137", "ext": "mp4", "vcodec": "avc1", "acodec": "none",
             "height": 1080, "fps": 30, "tbr": 4000, "filesize": 10000000},
            {"format_id": "251", "ext": "webm", "vcodec": "none", "acodec": "opus",
             "tbr": 128, "filesize_approx": 2000000.0}
        ]
    }"#;

    #[test]
    fn maps_streams_sizes_and_resolution() {
        let info = parse_info(&url(), SAMPLE.as_bytes()).expect("parse");
        assert_eq!(info.title, "Some Video");
        assert_eq!(info.extractor, "youtube");
        assert_eq!(info.default_format_id.as_deref(), Some("137+251"));
        assert_eq!(info.size, Some(12_345_678));
        assert!(info.size_is_approx);

        let f137 = info.formats.iter().find(|f| f.id == "137").unwrap();
        assert!(
            f137.has_video && !f137.has_audio,
            "acodec \"none\" is no audio"
        );
        assert_eq!(f137.height, Some(1080));
        assert_eq!(f137.size, Some(10_000_000));
        assert!(!f137.size_is_approx);

        let f251 = info.formats.iter().find(|f| f.id == "251").unwrap();
        assert!(f251.is_audio_only());
        assert!(f251.size_is_approx, "filesize_approx is an estimate");

        let f18 = info.formats.iter().find(|f| f.id == "18").unwrap();
        assert!(f18.is_complete());
    }

    #[test]
    fn resolved_selection_uses_yt_dlps_own_container() {
        let info = parse_info(&url(), SAMPLE.as_bytes()).expect("parse");
        // Trusting yt-dlp for the id it resolved beats re-deriving it.
        assert_eq!(info.ext_for("137+251"), "mkv");
        // For an override we derive it: mp4 video + webm audio cannot share
        // either container.
        assert_eq!(info.ext_for("18+251"), "mkv");
        // Two mp4-family parts keep the mp4 container.
        assert_eq!(info.ext_for("137+18"), "mp4");
        assert_eq!(info.ext_for("18"), "mp4");
        // An id with a part we know nothing about must not be named after the
        // half we happen to recognise.
        assert_eq!(info.ext_for("137+999"), "mkv");
    }

    #[test]
    fn size_of_an_override_sums_its_parts() {
        let info = parse_info(&url(), SAMPLE.as_bytes()).expect("parse");
        assert_eq!(info.size_for("18"), (Some(5_000_000), false));
        // 137 is exact, 251 is an estimate, so the sum is an estimate.
        assert_eq!(info.size_for("137+251"), (Some(12_345_678), true));

        let (size, approx) = info.size_for("137");
        assert_eq!(size, Some(10_000_000));
        assert!(!approx);
    }

    #[test]
    fn unknown_part_size_is_reported_as_unknown_not_understated() {
        let json = r#"{"title":"t","formats":[
            {"format_id":"a","ext":"mp4","vcodec":"avc1","acodec":"none","height":720},
            {"format_id":"b","ext":"m4a","vcodec":"none","acodec":"mp4a","filesize":100}
        ]}"#;
        let info = parse_info(&url(), json.as_bytes()).expect("parse");
        assert_eq!(info.size_for("a+b"), (None, true));
    }

    #[test]
    fn playlists_are_refused_rather_than_half_handled() {
        let json = r#"{"_type":"playlist","title":"L","entries":[{"id":"1"}]}"#;
        let err = parse_info(&url(), json.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("playlist"));
    }

    #[test]
    fn a_format_id_inside_the_subtitle_namespace_is_refused() {
        // Two different things sharing one identifier means a transcript
        // request could fetch a video instead. Better to stop.
        let json = r#"{"title":"t","formats":[
            {"format_id":"subs:en","ext":"mp4","vcodec":"a","acodec":"b"}
        ]}"#;
        let err = parse_info(&url(), json.as_bytes()).unwrap_err();
        assert!(err.to_string().contains("reserved subtitle namespace"));
    }

    #[test]
    fn subtitle_tracks_with_unusable_language_tags_are_dropped() {
        let json = r#"{"title":"t",
            "subtitles":{"en":[{"ext":"srt"}],"weird:tag":[{"ext":"srt"}]},
            "formats":[{"format_id":"18","ext":"mp4","vcodec":"a","acodec":"b"}]}"#;
        let info = parse_info(&url(), json.as_bytes()).expect("parse");
        let offer = info.offer(true);
        let ids: Vec<&str> = offer
            .quality_tiers()
            .iter()
            .map(|t| t.format_id.clone())
            .filter(|id| id.starts_with("subs"))
            .map(|id| Box::leak(id.into_boxed_str()) as &str)
            .collect();
        assert_eq!(
            ids,
            ["subs:en"],
            "a tag that cannot round-trip must not be offered"
        );
    }

    #[test]
    fn a_document_without_formats_is_an_error() {
        let json = r#"{"title":"t","formats":[]}"#;
        assert!(parse_info(&url(), json.as_bytes()).is_err());
    }

    #[test]
    fn missing_title_falls_back_to_the_url() {
        let json = r#"{"formats":[{"format_id":"18","ext":"mp4","vcodec":"a","acodec":"b"}]}"#;
        let info = parse_info(&url(), json.as_bytes()).expect("parse");
        assert_eq!(info.title, url().as_str());
    }

    #[test]
    fn base_args_forward_the_proxy_and_never_read_user_config() {
        let opts = YtdlpOptions::default();
        let tools = Tools {
            ytdlp: std::path::PathBuf::from("/usr/bin/yt-dlp"),
            ffmpeg: None,
        };
        let args = base_args(&opts, &tools, Some("socks5://127.0.0.1:9050"));
        assert!(args.contains(&"--ignore-config".to_owned()));
        let i = args
            .iter()
            .position(|a| a == "--proxy")
            .expect("proxy flag");
        assert_eq!(args[i + 1], "socks5://127.0.0.1:9050");
    }

    #[test]
    fn extra_args_are_appended_last_so_they_win() {
        let opts = crate::config::YtdlpOptionsBuilder::default()
            .extra_args(vec!["--retries".into(), "9".into()])
            .build()
            .unwrap();
        let tools = Tools {
            ytdlp: std::path::PathBuf::from("/usr/bin/yt-dlp"),
            ffmpeg: None,
        };
        let args = base_args(&opts, &tools, None);
        assert_eq!(&args[args.len() - 2..], ["--retries", "9"]);
    }

    #[test]
    fn selector_depends_on_muxer_availability() {
        assert_eq!(default_selector(true), "bv*+ba/b");
        assert_eq!(default_selector(false), "b");
    }
}
