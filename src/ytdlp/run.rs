//! Running a delegated download and reporting it as odl progress.
//!
//! yt-dlp writes into odl's own per-download directory rather than to the
//! save directory, so odl stays the owner of the metadata, the lockfile and
//! the final move. Progress arrives as one JSON object per line on stdout,
//! shaped by an explicit `--progress-template` so the format does not drift
//! between yt-dlp releases.

use crate::config::YtdlpOptions;
use crate::error::{OdlError, YtdlpError};
use crate::progress::{DownloadContext, Phase, ProgressEvent};
use crate::ytdlp::binary::{self, Tools};
use crate::ytdlp::extract::{base_args, last_meaningful_line, output_path_file};
use crate::ytdlp::process::{DEFAULT_GRACE, ManagedChild};
use http::header::HeaderMap;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, BufReader};
use url::Url;

/// Progress lines are emitted at most this often. Fast enough for a smooth
/// bar, slow enough that a fragmented download with thousands of fragments
/// does not spend its time formatting text.
const PROGRESS_DELTA_SECONDS: &str = "0.12";

/// Cap on retained stderr, matching the extraction path.
const MAX_STDERR_BYTES: usize = 16 * 1024;

/// Name of the file yt-dlp records its final output path into. Ours, not
/// downloaded data.
pub(crate) const OUTPUT_PATH_FILENAME: &str = "output.path";

/// Everything needed to run one delegated download.
#[derive(Debug)]
pub struct DownloadPlan<'a> {
    /// Page URL. Re-extracted on every run, so an expired media URL from a
    /// previous attempt is never reused.
    pub source_url: &'a Url,
    /// Format pinned at evaluate time. Never re-decided here — continuing a
    /// partial file under a different format would silently corrupt it.
    pub format_id: &'a str,
    pub download_dir: &'a Path,
    /// Output file stem inside `download_dir`; yt-dlp appends the container.
    pub stem: &'a str,
    pub total_size: Option<u64>,
    pub use_server_time: bool,
    pub proxy: Option<&'a str>,
    pub speed_limit: Option<u64>,
    pub headers: Option<&'a HeaderMap>,
    /// Maps to fragment-level parallelism; ignored for formats that are a
    /// single HTTP stream.
    pub concurrent_fragments: u64,
    /// How many times the tool itself should retry a stalled or failed
    /// transfer before giving up and letting odl decide what to do.
    pub max_retries: u32,
    /// How long the tool waits between its own retries.
    pub wait_between_retries: std::time::Duration,
}

/// One templated status line.
///
/// Download and post-processing lines share a struct and are told apart by
/// `k`, carried *inside* the JSON: the `download:`/`postprocess:` prefixes in
/// a `--progress-template` argument select which template is being defined
/// and are not echoed in the output, so they cannot be used to discriminate.
#[derive(Debug, Deserialize)]
struct StatusLine {
    /// `d` for a download line, `p` for post-processing.
    #[serde(default)]
    k: Option<String>,
    /// Bytes downloaded of the current format.
    #[serde(default)]
    d: Option<f64>,
    /// Total bytes of the current format, exact or estimated.
    #[serde(default)]
    t: Option<f64>,
    /// Bytes per second.
    #[serde(default)]
    s: Option<f64>,
    /// Format currently being fetched. Changes when a merged download moves
    /// from video to audio.
    #[serde(default)]
    f: Option<String>,
    /// Name of the running post-processor.
    #[serde(default)]
    pp: Option<String>,
    #[serde(default)]
    st: Option<String>,
}

/// Tracks bytes across the several formats a merged download fetches in
/// sequence.
///
/// yt-dlp restarts `downloaded_bytes` from zero for each format, so summing
/// the latest value per format is what makes the aggregate monotonic.
#[derive(Debug, Default)]
struct ByteTracker {
    per_format: HashMap<String, u64>,
}

impl ByteTracker {
    fn record(&mut self, format: Option<&str>, downloaded: u64) -> u64 {
        let key = format.unwrap_or("_").to_owned();
        self.per_format.insert(key, downloaded);
        self.total()
    }

    fn total(&self) -> u64 {
        self.per_format.values().copied().sum()
    }
}

/// `j` renders a value as JSON, `%(a,b)` falls back to a second field, and
/// `|x` supplies a default. The defaults are what keep a line parseable: a
/// field with no value and no default renders as a bare `NA`, which is not
/// valid JSON and would cost us the whole line.
fn progress_template() -> String {
    concat!(
        r#"download:{"k":"d","d":%(progress.downloaded_bytes|0)j,"#,
        r#""t":%(progress.total_bytes,progress.total_bytes_estimate|0)j,"#,
        r#""s":%(progress.speed|0)j,"#,
        r#""f":%(info.format_id|)j,"#,
        r#""st":%(progress.status|)j}"#
    )
    .to_owned()
}

fn postprocess_template() -> String {
    r#"postprocess:{"k":"p","pp":%(progress.postprocessor|)j,"st":%(progress.status|)j}"#.to_owned()
}

/// Build the argument list for a download run.
///
/// Split out so the exact command can be asserted in tests without spawning
/// anything.
pub fn download_args(plan: &DownloadPlan<'_>, opts: &YtdlpOptions, tools: &Tools) -> Vec<String> {
    let mut args = base_args(opts, tools, plan.proxy);

    // A transcript is a different kind of request: there is no media format to
    // select, and asking for one would download the video too.
    match crate::format::parse_subtitle_format_id(plan.format_id) {
        Some((lang, automatic)) => {
            args.push("--skip-download".into());
            args.push(if automatic {
                "--write-auto-subs".into()
            } else {
                "--write-subs".into()
            });
            args.push("--sub-langs".into());
            args.push(lang.to_owned());
        }
        None => {
            args.push("-f".into());
            args.push(plan.format_id.to_owned());
        }
    }

    // Land everything in odl's download directory; the move into the save
    // directory stays odl's job so conflict handling is unchanged.
    args.push("--paths".into());
    args.push(format!("home:{}", plan.download_dir.display()));
    args.push("-o".into());
    args.push(format!("{}.%(ext)s", plan.stem));

    // Resume an interrupted transfer rather than starting over. This is the
    // default, but stating it makes the intent explicit.
    args.push("--continue".into());

    // The produced path is reported rather than guessed: after muxing, the
    // container is not always the one either input had.
    //
    // A bare name, not a path: `--print-to-file` resolves its target relative
    // to the `home` path above, so anything but a plain filename lands in a
    // duplicated directory when `home` is itself relative.
    args.push("--print-to-file".into());
    args.push("after_move:filepath".into());
    args.push(OUTPUT_PATH_FILENAME.into());

    args.push("--newline".into());
    args.push("--progress".into());
    args.push("--progress-delta".into());
    args.push(PROGRESS_DELTA_SECONDS.into());
    args.push("--progress-template".into());
    args.push(progress_template());
    args.push("--progress-template".into());
    args.push(postprocess_template());

    if plan.use_server_time {
        args.push("--mtime".into());
    } else {
        args.push("--no-mtime".into());
    }

    if let Some(limit) = plan.speed_limit {
        args.push("--limit-rate".into());
        args.push(limit.to_string());
    }

    if plan.concurrent_fragments > 1 {
        args.push("--concurrent-fragments".into());
        args.push(plan.concurrent_fragments.to_string());
    }

    // The tool retries in several domains of its own, each with a different
    // default. Setting them all from one configured number is what makes
    // `max_retries` mean the same thing here as it does for the built-in
    // engine, instead of yt-dlp's assorted defaults quietly winning.
    let retries = plan.max_retries.to_string();
    for flag in ["--retries", "--fragment-retries", "--extractor-retries"] {
        args.push(flag.into());
        args.push(retries.clone());
    }
    // Seconds, as the tool expects. Sub-second waits round to zero, which is
    // still an honest reading of "retry almost immediately".
    args.push("--retry-sleep".into());
    args.push(plan.wait_between_retries.as_secs().to_string());

    if let Some(headers) = plan.headers {
        for (name, value) in headers.iter() {
            if let Ok(v) = value.to_str() {
                args.push("--add-header".into());
                args.push(format!("{name}:{v}"));
            }
        }
    }

    // Separator first: a URL is data and must never be parsed as a flag.
    args.push("--".into());
    args.push(plan.source_url.as_str().to_owned());
    args
}

/// Whether a directory entry holds downloaded bytes rather than bookkeeping.
fn is_payload(name: &str) -> bool {
    name != OUTPUT_PATH_FILENAME && !crate::Download::is_bookkeeping_filename(name)
}

/// Bytes present on disk for a delegated download.
///
/// Counts every file the engine produced — partials, per-format files of a
/// merge in progress, and the finished output — because unlike the multipart
/// engine there is no part table describing them.
pub async fn bytes_on_disk(download_dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(mut entries) = tokio::fs::read_dir(download_dir).await else {
        return 0;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !is_payload(name) {
            continue;
        }
        if let Ok(meta) = entry.metadata().await
            && meta.is_file()
        {
            total = total.saturating_add(meta.len());
        }
    }
    total
}

/// A finished output file left in the download directory by a previous run.
///
/// A crash between "yt-dlp finished" and "odl moved the file" leaves a
/// complete download behind; finding it avoids downloading everything again.
pub async fn find_completed_output(download_dir: &Path, stem: &str) -> Option<PathBuf> {
    let Ok(mut entries) = tokio::fs::read_dir(download_dir).await else {
        return None;
    };
    let mut best: Option<(bool, PathBuf)> = None;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(stem) || !is_payload(name) {
            continue;
        }
        // `.part` is an interrupted transfer and `.ytdl` is fragment state;
        // neither is a finished file.
        if name.ends_with(".part") || name.ends_with(".ytdl") {
            continue;
        }
        if !entry.metadata().await.map(|m| m.is_file()).unwrap_or(false) {
            continue;
        }
        // A merge leaves per-format files named `<stem>.f137.mp4` beside the
        // muxed `<stem>.mkv`. Only the latter has a single suffix, so prefer
        // it — returning an intermediate would save half the download as if
        // it were the whole thing.
        let is_muxed = name[stem.len()..].matches('.').count() <= 1;
        if is_muxed {
            return Some(entry.path());
        }
        if best.is_none() {
            best = Some((is_muxed, entry.path()));
        }
    }
    best.map(|(_, path)| path)
}

/// Delete every downloaded byte for this download, keeping odl's bookkeeping.
///
/// Used when a partial cannot be continued — a different format was chosen,
/// or the metadata belongs to another URL.
pub async fn discard_payload(download_dir: &Path) -> Result<(), OdlError> {
    let mut entries = match tokio::fs::read_dir(download_dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(OdlError::StdIoError {
                e,
                extra_info: Some(format!(
                    "Failed to list download dir at {}",
                    download_dir.display()
                )),
            });
        }
    };
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !is_payload(name) {
            continue;
        }
        if entry.metadata().await.map(|m| m.is_file()).unwrap_or(false) {
            let _ = tokio::fs::remove_file(entry.path()).await;
        }
    }
    Ok(())
}

/// Run one delegated download to completion.
///
/// Returns the path of the produced file inside `download_dir`; moving it to
/// its final home is the caller's job.
pub async fn run_download(
    plan: &DownloadPlan<'_>,
    opts: &YtdlpOptions,
    tools: &Tools,
    ctx: &DownloadContext,
) -> Result<PathBuf, OdlError> {
    if ctx.is_cancelled() {
        return Err(OdlError::Cancelled);
    }

    if let Some(existing) = find_completed_output(plan.download_dir, plan.stem).await {
        // A previous run got all the way through; nothing left to transfer.
        return Ok(existing);
    }

    // `--print-to-file` appends, so start from empty to make the last line
    // unambiguously this run's.
    let path_file = output_path_file(plan.download_dir);
    let _ = tokio::fs::remove_file(&path_file).await;

    let mut cmd = binary::command(&tools.ytdlp);
    cmd.args(download_args(plan, opts, tools));
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    let mut child = ManagedChild::spawn(&mut cmd).map_err(|e| OdlError::StdIoError {
        e,
        extra_info: Some(format!("Failed to start {}", tools.ytdlp.display())),
    })?;

    let stdout = child.take_stdout();
    let stderr = child.take_stderr();

    // Drain stderr concurrently: a full pipe would otherwise block the child
    // once it has written enough diagnostics.
    let stderr_task = tokio::spawn(async move {
        let mut buf = String::new();
        if let Some(stderr) = stderr {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if buf.len() < MAX_STDERR_BYTES {
                    buf.push_str(&line);
                    buf.push('\n');
                }
            }
        }
        buf
    });

    ctx.emit(ProgressEvent::PhaseChanged(Phase::Downloading));

    let mut tracker = ByteTracker::default();
    let mut cancelled = false;

    if let Some(stdout) = stdout {
        let mut lines = BufReader::new(stdout).lines();
        loop {
            tokio::select! {
                line = lines.next_line() => {
                    match line {
                        Ok(Some(line)) => handle_line(&line, &mut tracker, plan, ctx),
                        Ok(None) => break,
                        Err(e) => {
                            tracing::debug!(error = %e, "reading yt-dlp output failed");
                            break;
                        }
                    }
                }
                _ = ctx.cancel.cancelled() => {
                    cancelled = true;
                    break;
                }
            }
        }
    }

    if cancelled {
        let _ = child.terminate(DEFAULT_GRACE).await;
        stderr_task.abort();
        return Err(OdlError::Cancelled);
    }

    let status = child.wait().await.map_err(|e| OdlError::StdIoError {
        e,
        extra_info: Some("Failed while waiting for yt-dlp".to_owned()),
    })?;
    let stderr = stderr_task.await.unwrap_or_default();

    if !status.success() {
        if stderr.contains("Unsupported URL") {
            return Err(YtdlpError::UnsupportedUrl.into());
        }
        // A refusal by the site, not a fault in the toolchain: worth its own
        // shape so a caller can tell "try later" from "fix your setup".
        if is_rate_limited(&stderr) {
            return Err(YtdlpError::RateLimited {
                detail: last_meaningful_line(&stderr),
            }
            .into());
        }
        // Sites rotate their format ids. Typed rather than generic so the
        // caller can clear the pin and drop the partial: the bytes already on
        // disk belong to an encoding that can no longer be continued.
        if stderr.contains("Requested format is not available") {
            return Err(YtdlpError::FormatUnavailable {
                format_id: plan.format_id.to_owned(),
            }
            .into());
        }
        return Err(YtdlpError::ProcessFailed {
            code: status.code(),
            stderr: last_meaningful_line(&stderr),
        }
        .into());
    }

    read_output_path(&path_file, plan).await
}

/// Resolve where yt-dlp put the finished file.
async fn read_output_path(path_file: &Path, plan: &DownloadPlan<'_>) -> Result<PathBuf, OdlError> {
    if let Ok(contents) = tokio::fs::read_to_string(path_file).await
        && let Some(line) = contents.lines().map(str::trim).rfind(|l| !l.is_empty())
    {
        let path = PathBuf::from(line);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return Ok(path);
        }
    }

    // The reported path is a convenience, not the only way to find the file:
    // fall back to scanning before failing.
    if let Some(found) = find_completed_output(plan.download_dir, plan.stem).await {
        return Ok(found);
    }

    Err(YtdlpError::Other {
        message: "yt-dlp reported success but produced no output file".to_owned(),
    }
    .into())
}

/// Whether the tool's output describes the site refusing us as too frequent.
pub(crate) fn is_rate_limited(stderr: &str) -> bool {
    stderr.contains("HTTP Error 429") || stderr.contains("Too Many Requests")
}

/// Interpret one line of yt-dlp output.
fn handle_line(
    line: &str,
    tracker: &mut ByteTracker,
    plan: &DownloadPlan<'_>,
    ctx: &DownloadContext,
) {
    // Anything that is not one of our templated lines is yt-dlp's own
    // chatter — extractor notices, destination banners — and is only useful
    // when tracing.
    if !line.starts_with('{') {
        tracing::debug!(line, "yt-dlp");
        return;
    }
    let Ok(p) = serde_json::from_str::<StatusLine>(line) else {
        tracing::trace!(line, "unparsable status line");
        return;
    };

    match p.k.as_deref() {
        Some("d") => {
            if let Some(d) = p.d.filter(|d| d.is_finite() && *d >= 0.0) {
                let total = tracker.record(p.f.as_deref(), d as u64);
                ctx.emit(ProgressEvent::Progress {
                    downloaded: total,
                    // The plan's size covers every format of a merge; a single
                    // format's total would under-report during the first half.
                    total: plan
                        .total_size
                        .or_else(|| p.t.filter(|t| t.is_finite() && *t > 0.0).map(|t| t as u64)),
                });
            }
            // Zero is also what the template emits when the speed is not yet
            // known, and reporting that as a real sample would drag the
            // displayed rate down.
            if let Some(speed) = p.s.filter(|s| s.is_finite() && *s > 0.0) {
                ctx.emit(ProgressEvent::Speed {
                    bytes_per_second: speed,
                });
            }
            if p.st.as_deref() == Some("finished") {
                tracing::debug!(format = ?p.f, "format finished");
            }
        }
        Some("p") => {
            if p.st.as_deref() == Some("started") {
                ctx.emit(ProgressEvent::PhaseChanged(Phase::PostProcessing));
                let what = p.pp.as_deref().filter(|s| !s.is_empty());
                ctx.emit(ProgressEvent::Message(format!(
                    "{}…",
                    what.unwrap_or("post-processing")
                )));
            }
        }
        _ => tracing::debug!(line, "yt-dlp"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::{ProgressReporter, channel_reporter};
    use std::sync::Arc;

    fn tools() -> Tools {
        Tools {
            ytdlp: PathBuf::from("/usr/bin/yt-dlp"),
            ffmpeg: None,
        }
    }

    fn plan<'a>(dir: &'a Path, url: &'a Url) -> DownloadPlan<'a> {
        DownloadPlan {
            source_url: url,
            format_id: "137+251",
            download_dir: dir,
            stem: "01ABCDEF",
            total_size: Some(1_000),
            use_server_time: false,
            proxy: None,
            speed_limit: None,
            headers: None,
            concurrent_fragments: 1,
            max_retries: 3,
            wait_between_retries: std::time::Duration::from_secs(1),
        }
    }

    fn arg_after<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .map(String::as_str)
    }

    #[test]
    fn command_pins_the_format_and_targets_the_download_dir() {
        let url = Url::parse("https://www.youtube.com/watch?v=x").unwrap();
        let dir = PathBuf::from("/data/dl/video");
        let args = download_args(&plan(&dir, &url), &YtdlpOptions::default(), &tools());

        assert_eq!(arg_after(&args, "-f"), Some("137+251"));
        assert_eq!(arg_after(&args, "--paths"), Some("home:/data/dl/video"));
        assert_eq!(arg_after(&args, "-o"), Some("01ABCDEF.%(ext)s"));
        assert!(args.contains(&"--continue".to_owned()));
        // A bare name: the target is resolved against `home`, so a path here
        // would land in a duplicated directory.
        assert_eq!(arg_after(&args, "after_move:filepath"), Some("output.path"));
    }

    #[test]
    fn url_is_passed_after_a_separator_so_it_cannot_be_read_as_a_flag() {
        // A URL is remote input; if one ever starts with `-`, it must still
        // be an operand.
        let url = Url::parse("https://www.youtube.com/watch?v=x").unwrap();
        let dir = PathBuf::from("/data/dl/video");
        let args = download_args(&plan(&dir, &url), &YtdlpOptions::default(), &tools());

        let sep = args.iter().position(|a| a == "--").expect("separator");
        assert_eq!(args.len(), sep + 2, "URL must be the only operand");
        assert_eq!(args[sep + 1], url.as_str());
    }

    #[test]
    fn transfer_limits_are_forwarded() {
        let url = Url::parse("https://www.youtube.com/watch?v=x").unwrap();
        let dir = PathBuf::from("/data/dl/video");
        let mut p = plan(&dir, &url);
        p.speed_limit = Some(500_000);
        p.proxy = Some("http://127.0.0.1:8080");
        p.concurrent_fragments = 4;
        p.use_server_time = true;

        let args = download_args(&p, &YtdlpOptions::default(), &tools());
        assert_eq!(arg_after(&args, "--limit-rate"), Some("500000"));
        assert_eq!(arg_after(&args, "--proxy"), Some("http://127.0.0.1:8080"));
        assert_eq!(arg_after(&args, "--concurrent-fragments"), Some("4"));
        assert!(args.contains(&"--mtime".to_owned()));
        // Every retry domain the tool has is set from the one configured
        // number, so its assorted defaults cannot quietly win.
        assert_eq!(arg_after(&args, "--retries"), Some("3"));
        assert_eq!(arg_after(&args, "--fragment-retries"), Some("3"));
        assert_eq!(arg_after(&args, "--extractor-retries"), Some("3"));
        assert_eq!(arg_after(&args, "--retry-sleep"), Some("1"));
    }

    #[test]
    fn a_refusal_by_the_site_is_recognised_however_it_is_worded() {
        // Both spellings appear in the wild depending on which layer reports
        // it, and mistaking either for a broken toolchain would send the user
        // to fix something that is not wrong.
        assert!(is_rate_limited(
            "ERROR: Unable to download video subtitles for 'ab': HTTP Error 429: Too Many Requests"
        ));
        assert!(is_rate_limited("HTTP Error 429"));
        assert!(!is_rate_limited(
            "ERROR: Unsupported URL: https://example.com"
        ));
        assert!(!is_rate_limited("ERROR: Requested format is not available"));
    }

    #[test]
    fn byte_tracker_sums_formats_instead_of_following_the_reset() {
        let mut t = ByteTracker::default();
        assert_eq!(t.record(Some("137"), 100), 100);
        assert_eq!(t.record(Some("137"), 900), 900);
        // Second format starts from zero; the aggregate must not drop.
        assert_eq!(t.record(Some("251"), 10), 910);
        assert_eq!(t.record(Some("251"), 50), 950);
    }

    #[tokio::test]
    async fn progress_lines_become_aggregate_events() {
        let (reporter, mut rx) = channel_reporter();
        let ctx = DownloadContext::new().with_reporter(reporter as Arc<dyn ProgressReporter>);
        let url = Url::parse("https://www.youtube.com/watch?v=x").unwrap();
        let dir = PathBuf::from("/data/dl/video");
        let p = plan(&dir, &url);
        let mut tracker = ByteTracker::default();

        handle_line(
            r#"{"k":"d","d":500,"t":1000,"s":250.5,"f":"137","st":"downloading"}"#,
            &mut tracker,
            &p,
            &ctx,
        );

        let mut saw_progress = false;
        let mut saw_speed = false;
        while let Ok(ev) = rx.try_recv() {
            match ev {
                ProgressEvent::Progress { downloaded, total } => {
                    assert_eq!(downloaded, 500);
                    // Plan size wins: it covers every format of the merge.
                    assert_eq!(total, Some(1_000));
                    saw_progress = true;
                }
                ProgressEvent::Speed { bytes_per_second } => {
                    assert!((bytes_per_second - 250.5).abs() < f64::EPSILON);
                    saw_speed = true;
                }
                _ => {}
            }
        }
        assert!(saw_progress && saw_speed);
    }

    #[tokio::test]
    async fn malformed_progress_lines_are_ignored_not_fatal() {
        let ctx = DownloadContext::new();
        let url = Url::parse("https://www.youtube.com/watch?v=x").unwrap();
        let dir = PathBuf::from("/data/dl/video");
        let p = plan(&dir, &url);
        let mut tracker = ByteTracker::default();

        handle_line("{not json", &mut tracker, &p, &ctx);
        handle_line("[youtube] Extracting URL", &mut tracker, &p, &ctx);
        // A field with no value renders as a bare `NA`; the template supplies
        // defaults to avoid it, but a stray one must not be fatal either.
        handle_line(
            r#"{"k":"d","d":NA,"st":"downloading"}"#,
            &mut tracker,
            &p,
            &ctx,
        );
        handle_line(
            r#"{"k":"d","d":null,"st":"downloading"}"#,
            &mut tracker,
            &p,
            &ctx,
        );
        assert_eq!(tracker.total(), 0);
    }

    #[tokio::test]
    async fn disk_accounting_counts_payload_and_skips_bookkeeping() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("01ABC.mp4.part"), vec![0u8; 300])
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("01ABC.f251.webm"), vec![0u8; 200])
            .await
            .unwrap();
        // Bookkeeping must not inflate the reported progress.
        tokio::fs::write(dir.path().join("metadata.pb"), vec![0u8; 999])
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("lock"), b"")
            .await
            .unwrap();

        assert_eq!(bytes_on_disk(dir.path()).await, 500);
    }

    #[tokio::test]
    async fn a_finished_file_is_recognised_but_partials_are_not() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("01ABC.mp4.part"), b"x")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("01ABC.mp4.ytdl"), b"x")
            .await
            .unwrap();
        assert!(find_completed_output(dir.path(), "01ABC").await.is_none());

        tokio::fs::write(dir.path().join("01ABC.mp4"), b"done")
            .await
            .unwrap();
        let found = find_completed_output(dir.path(), "01ABC").await.unwrap();
        assert_eq!(found.file_name().unwrap(), "01ABC.mp4");
    }

    #[tokio::test]
    async fn the_muxed_output_wins_over_a_merges_leftover_streams() {
        let dir = tempfile::tempdir().unwrap();
        // Per-format files a merge leaves behind. Returning one of these would
        // deliver only the video or only the audio.
        tokio::fs::write(dir.path().join("01ABC.f137.mp4"), vec![0u8; 300])
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("01ABC.f251.webm"), vec![0u8; 100])
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("01ABC.mkv"), vec![0u8; 400])
            .await
            .unwrap();

        let found = find_completed_output(dir.path(), "01ABC").await.unwrap();
        assert_eq!(found.file_name().unwrap(), "01ABC.mkv");
    }

    #[tokio::test]
    async fn discarding_payload_keeps_metadata() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("01ABC.mp4.part"), b"partial")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("01ABC.mp4.ytdl"), b"state")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("metadata.pb"), b"keep")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("odl.lock"), b"")
            .await
            .unwrap();

        discard_payload(dir.path()).await.unwrap();

        assert!(!dir.path().join("01ABC.mp4.part").exists());
        assert!(
            !dir.path().join("01ABC.mp4.ytdl").exists(),
            "fragment state must go too, or a resume continues from a stale plan"
        );
        assert!(dir.path().join("metadata.pb").exists());
        assert!(
            dir.path().join("odl.lock").exists(),
            "removing the lockfile would break the exclusion it provides"
        );
    }
}
