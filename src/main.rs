use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use clap::{CommandFactory, Parser};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use odl::{
    Download,
    config::Config,
    conflict::{
        FileChangedResolution, FinalFileExistsResolution, NotResumableResolution,
        SameDownloadExistsResolution, SaveConflictResolver, ServerConflictResolver,
    },
    credentials::Credentials,
    download_manager::{DownloadManager, DownloadRequest, EvaluateRequest},
    engine::EnginePreference,
    error::OdlError,
    format::{
        DefaultFormatSelector, FixedFormatSelector, FormatOffer, FormatSelector, QualityTier,
    },
    progress::{
        AsyncReporter, CancellationToken, DownloadContext, Phase, ProgressEvent, ProgressReporter,
        SAMPLE_INTERVAL,
    },
};
use reqwest::Url;
use serde_json::json;
use std::io::IsTerminal;
use std::process::ExitCode;
use tokio::{self, io::AsyncBufReadExt};
mod args;
mod json;
use args::{Args, LogLevel, OutputFormat};
use json::JsonReporter;
use odl::download_manager::DownloadStatus;
use odl::engine::Engine;
use tracing::instrument;

fn init_tracing(level: LogLevel) {
    use tracing_subscriber::{EnvFilter, fmt};
    let default_directive = match level {
        LogLevel::Off => "off",
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
        LogLevel::Trace => "trace",
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_directive));
    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

/// Stable process exit code for an error, so scripts/agents can branch on
/// failure class without parsing messages. `0` is success (returned
/// elsewhere); `130` follows the shell convention for cancellation.
fn exit_code(e: &OdlError) -> u8 {
    match e {
        OdlError::CliError { .. }
        | OdlError::EmptyInputFile
        | OdlError::UrlDecodeError { .. }
        | OdlError::ConfigBuilderError(_)
        | OdlError::DownloadOptionsBuilderError(_) => 2,
        // The site refused us, which is a transient network-class condition
        // rather than a broken toolchain: scripts that retry on 3 should
        // retry on this too.
        OdlError::Network(_) | OdlError::Ytdlp(odl::error::YtdlpError::RateLimited { .. }) => 3,
        OdlError::Conflict(_) => 4,
        OdlError::StdIoError { .. } => 5,
        OdlError::MetadataError(_) => 6,
        OdlError::Ytdlp(_) => 7,
        OdlError::NotEvaluated { .. } | OdlError::InvalidRequest { .. } => 2,
        OdlError::Cancelled => 130,
        // `OdlError` is non-exhaustive so new engines can add failure modes.
        // Anything unclassified shares the generic failure code.
        OdlError::Other { .. } | _ => 1,
    }
}

/// Stable machine-readable error category string (pairs with [`exit_code`]).
fn error_kind(e: &OdlError) -> &'static str {
    match e {
        OdlError::CliError { .. } => "cli",
        OdlError::EmptyInputFile => "empty_input_file",
        OdlError::UrlDecodeError { .. } => "url_decode",
        OdlError::ConfigBuilderError(_) | OdlError::DownloadOptionsBuilderError(_) => "config",
        OdlError::Network(_) | OdlError::Ytdlp(odl::error::YtdlpError::RateLimited { .. }) => {
            "network"
        }
        OdlError::Conflict(_) => "conflict",
        OdlError::StdIoError { .. } => "io",
        OdlError::MetadataError(_) => "metadata",
        OdlError::Ytdlp(_) => "ytdlp",
        OdlError::NotEvaluated { .. } => "not_evaluated",
        OdlError::InvalidRequest { .. } => "invalid_request",
        OdlError::Cancelled => "cancelled",
        OdlError::Other { .. } | _ => "other",
    }
}

/// Emit a top-level error in the selected format and return its exit code.
fn report_error(e: &OdlError, format: OutputFormat) -> ExitCode {
    match format {
        // Cancelling is something the user just did on purpose. The interrupt
        // handler already acknowledged it and every download's own line shows
        // it, so a third "Error:" banner is noise.
        OutputFormat::Text if matches!(e, OdlError::Cancelled) => {}
        OutputFormat::Text => {
            eprintln!("Error: {}", e);
            #[cfg(debug_assertions)]
            {
                eprintln!("{e:?}");
            }
        }
        OutputFormat::Json => {
            let v = json!({
                "type": "error",
                "kind": error_kind(e),
                "message": e.to_string(),
                "exit_code": exit_code(e),
            });
            eprintln!("{v}");
        }
    }
    ExitCode::from(exit_code(e))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadType {
    Url(Url),
    File(Box<PathBuf>),
    FileAtUrl(Url),
}

#[derive(Copy, Clone)]
struct CliResolver {
    file_changed: FileChangedResolution,
    not_resumable: NotResumableResolution,
    same_download_exists: SameDownloadExistsResolution,
    final_file_exists: FinalFileExistsResolution,
}

#[async_trait]
impl ServerConflictResolver for CliResolver {
    async fn resolve_file_changed(&self, _: &Download) -> FileChangedResolution {
        if self.file_changed == FileChangedResolution::Restart {
            FileChangedResolution::Restart
        } else {
            FileChangedResolution::Abort
        }
    }
    async fn resolve_not_resumable(&self, _: &Download) -> NotResumableResolution {
        if self.not_resumable == NotResumableResolution::Restart {
            NotResumableResolution::Restart
        } else {
            NotResumableResolution::Abort
        }
    }
}

#[async_trait]
impl SaveConflictResolver for CliResolver {
    async fn same_download_exists(&self, _: &Download) -> SameDownloadExistsResolution {
        // explicit CLI choice takes precedence
        self.same_download_exists
    }
    async fn final_file_exists(&self, _: &Download) -> FinalFileExistsResolution {
        if self.final_file_exists == FinalFileExistsResolution::ReplaceAndContinue {
            FinalFileExistsResolution::ReplaceAndContinue
        } else if self.final_file_exists == FinalFileExistsResolution::AddNumberToNameAndContinue {
            FinalFileExistsResolution::AddNumberToNameAndContinue
        } else {
            FinalFileExistsResolution::Abort
        }
    }
}

/// Asks which quality to download, on the terminal.
///
/// Progress bars are suspended for the duration so the menu is not overwritten
/// by a redraw, and the prompt goes to stderr so a piped stdout stays clean.
struct InteractiveFormatSelector {
    mp: Arc<MultiProgress>,
}

#[async_trait]
impl FormatSelector for InteractiveFormatSelector {
    async fn select(&self, offer: &FormatOffer) -> Option<String> {
        let tiers = offer.quality_tiers();
        // Nothing to decide: asking would be noise.
        if tiers.len() < 2 {
            return DefaultFormatSelector.select(offer).await;
        }

        let mp = Arc::clone(&self.mp);
        let title = offer.title.clone();
        let can_merge = offer.can_merge;
        let fallback = DefaultFormatSelector.select(offer).await;

        tokio::task::spawn_blocking(move || {
            mp.suspend(|| prompt_for_quality(&title, &tiers, can_merge, fallback))
        })
        .await
        .unwrap_or(None)
    }
}

/// Render the quality menu and read a choice. Blocking; call off the runtime.
fn prompt_for_quality(
    title: &str,
    tiers: &[QualityTier],
    can_merge: bool,
    fallback: Option<String>,
) -> Option<String> {
    use std::io::Write;

    let mut err = std::io::stderr();
    // The title is the one piece of context saying *what* is being chosen.
    // Printed bare it reads like stray output.
    let _ = writeln!(err, "\nTitle:  {title}");

    // The default is the best tier that can actually be downloaded, which is
    // not the first one when higher qualities need a muxer we lack.
    let default = tiers.iter().position(|t| t.available)?;
    let width = tiers
        .iter()
        .map(|t| t.quality.to_string().chars().count())
        .max()
        .unwrap_or(8);
    let ext_width = tiers
        .iter()
        .map(|t| t.ext.chars().count())
        .max()
        .unwrap_or(4);

    for (i, tier) in tiers.iter().enumerate() {
        let size = match tier.size {
            Some(s) if tier.size_is_approx => format!("~{}", HumanBytes(s)),
            Some(s) => HumanBytes(s).to_string(),
            None => "size unknown".to_owned(),
        };
        // Unavailable tiers stay listed with the reason attached: a menu that
        // silently stopped at 480p would read as all the site offers.
        let note = if tier.available {
            ""
        } else {
            "  — needs ffmpeg"
        };
        let marker = if i == default { '*' } else { ' ' };
        let quality = tier.quality.to_string();
        // The container matters as much as the resolution: it decides what
        // will actually open the file.
        let _ = writeln!(
            err,
            " {marker}{:>2}) {quality:<width$}  {:<ext_width$}  {size}{note}",
            i + 1,
            tier.ext
        );
    }

    if !can_merge && tiers.iter().any(|t| !t.available) {
        let _ = writeln!(
            err,
            "\n  Higher qualities are served as separate video and audio streams,\n  \
             which need ffmpeg to join. Install it with `odl tools install ffmpeg`."
        );
    }
    let _ = write!(
        err,
        "\nQuality [1-{}, default {}]: ",
        tiers.len(),
        default + 1
    );
    let _ = err.flush();

    let mut line = String::new();
    if std::io::stdin().read_line(&mut line).is_err() {
        return fallback;
    }
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Some(tiers[default].format_id.clone());
    }
    match trimmed.parse::<usize>() {
        Ok(n) if (1..=tiers.len()).contains(&n) && tiers[n - 1].available => {
            Some(tiers[n - 1].format_id.clone())
        }
        // Choosing something that needs a missing tool is a specific mistake
        // and deserves a specific answer rather than a silent substitution.
        Ok(n) if (1..=tiers.len()).contains(&n) => {
            let _ = writeln!(
                err,
                "That quality needs ffmpeg, which is not installed; taking {} instead.",
                tiers[default].quality
            );
            Some(tiers[default].format_id.clone())
        }
        _ => {
            let _ = writeln!(err, "Not a listed choice; taking the best available.");
            Some(tiers[default].format_id.clone())
        }
    }
}

/// Turn the first interrupt into a cancellation, and a second one into an
/// immediate exit.
///
/// This matters most for engines that drive a helper process: those run in a
/// process group of their own so odl controls their teardown, which also
/// means a terminal's Ctrl-C never reaches them. Without this, dying on the
/// signal would leave the helper — and anything it spawned — running.
///
/// The second interrupt is the escape hatch for a teardown that itself hangs.
/// It skips cleanup deliberately: a user pressing Ctrl-C twice wants out.
fn spawn_interrupt_handler(cancel: CancellationToken) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_err() {
            // No handler could be installed; leave the default disposition
            // rather than pretending cancellation is wired up.
            return;
        }
        eprintln!("\nInterrupted; finishing up. Press Ctrl-C again to quit immediately.");
        cancel.cancel();

        if tokio::signal::ctrl_c().await.is_ok() {
            // Exiting here skips every destructor, so the helpers' own
            // teardown never runs. Kill their process groups first, or
            // "quit immediately" would mean leaving a downloader running
            // with nothing left to stop it.
            #[cfg(feature = "ytdlp")]
            odl::ytdlp::process::kill_all_groups();
            std::process::exit(130);
        }
    });
}

/// Whether to prompt for quality, given the CLI flags and the environment.
fn should_prompt_for_format(
    choice: args::ChooseFormat,
    format: OutputFormat,
    url_count: usize,
) -> bool {
    // A prompt needs a terminal on both ends and human-readable output. This
    // is checked even for `Always`: a question nobody can answer is a hang,
    // and `--format json` is exactly where an automated caller lives.
    let can_ask = format == OutputFormat::Text
        && std::io::stdin().is_terminal()
        && std::io::stderr().is_terminal();
    if !can_ask {
        return false;
    }
    match choice {
        args::ChooseFormat::Never => false,
        args::ChooseFormat::Always => true,
        // One decision is a question; a hundred-URL list would be an
        // interrogation, so `auto` stays quiet for batches.
        args::ChooseFormat::Auto => url_count == 1,
    }
}

pub const PROGRESS_CHARS: &str = "█▇▆▅▄▃▂▁";

/// Atomics shared between a bar and its indicatif style closures so the
/// closures can render fast-reacting speed / ETA without locking.
struct BarMetrics {
    speed_bits: Arc<AtomicU64>,
    downloaded: Arc<AtomicU64>,
    total: Arc<AtomicU64>,
}

impl BarMetrics {
    fn new() -> Self {
        Self {
            speed_bits: Arc::new(AtomicU64::new(0)),
            downloaded: Arc::new(AtomicU64::new(0)),
            total: Arc::new(AtomicU64::new(0)),
        }
    }
}

struct PartBar {
    bar: ProgressBar,
    metrics: BarMetrics,
}

/// Drives a parent + per-part `indicatif` bar set from
/// `odl::progress::ProgressEvent`s. One `CliReporter` per download.
struct CliReporter {
    mp: Arc<MultiProgress>,
    parent: ProgressBar,
    parts: Mutex<HashMap<String, PartBar>>,
    parent_metrics: BarMetrics,
    /// Last resolved filename. Restored as parent's bar message when
    /// transient phases (Evaluating / ResolvingConflicts / retry
    /// countdown) clear, so the bar lands back on the file label.
    filename: Mutex<Option<String>>,
    /// What this download was asked for, for messages raised before a
    /// filename is known.
    url: String,
    /// Stand-in row for an engine that transfers in one piece.
    ///
    /// The parent line is a header: it carries the name and the totals, and
    /// leaves the bar to the rows beneath it. An engine with no parts would
    /// otherwise draw no bar at all, so it gets a single row of its own —
    /// same shape as a part row, so every download reads the same way.
    solo: Mutex<Option<PartBar>>,
}

impl CliReporter {
    fn new(
        mp: Arc<MultiProgress>,
        parent: ProgressBar,
        parent_metrics: BarMetrics,
        url: String,
    ) -> Self {
        Self {
            url,
            mp,
            parent,
            parts: Mutex::new(HashMap::new()),
            parent_metrics,
            filename: Mutex::new(None),
            solo: Mutex::new(None),
        }
    }

    /// Add the stand-in row, unless real parts are doing the drawing.
    fn ensure_solo_row(&self) {
        if !self.parts.lock().unwrap().is_empty() {
            return;
        }
        let mut solo = self.solo.lock().unwrap();
        if solo.is_some() {
            return;
        }
        let metrics = BarMetrics::new();
        // A bar whose length is zero renders as complete, so a download would
        // appear finished before it began. The total is usually known by now
        // from evaluate; when it is not, a length-less bar draws empty and
        // gets its real length from the first progress event.
        let total = self.parent_metrics.total.load(Ordering::Relaxed);
        let bar = if total > 0 {
            ProgressBar::new(total)
        } else {
            ProgressBar::new_spinner()
        };
        let bar = self.mp.add(bar.with_style(build_solo_row_style()));
        bar.enable_steady_tick(SAMPLE_INTERVAL);
        *solo = Some(PartBar { bar, metrics });
    }

    /// Clear the stand-in row, if there is one.
    fn clear_solo_row(&self) {
        if let Some(p) = self.solo.lock().unwrap().take() {
            p.bar.finish_and_clear();
        }
    }
}

fn phase_label(phase: Phase) -> &'static str {
    match phase {
        Phase::Evaluating => "Evaluating",
        Phase::ResolvingConflicts => "Resolving conflicts",
        Phase::Downloading => "Downloading",
        Phase::PostProcessing => "Processing",
        Phase::Assembling => "Assembling",
        Phase::Flushing => "Flushing data to disk",
        Phase::Verifying => "Verifying checksum",
        _ => "Working",
    }
}

impl CliReporter {
    /// Name this download goes by in a one-line status.
    ///
    /// Falls back to the URL rather than a generic word: a failure raised
    /// before the filename is known still has to say *which* download failed.
    fn describe_target(&self) -> String {
        self.filename
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| self.url.clone())
    }

    /// Collapse the whole bar group down to a single final line.
    ///
    /// Child bars are drained first so nothing is left dangling, and the
    /// parent's template is replaced rather than abandoned in place — an
    /// abandoned progress template keeps rendering the last percentage and
    /// speed, which read as current long after they stopped being so.
    fn finish_with_status(&self, template: &str, message: String) {
        self.clear_solo_row();
        {
            let mut parts = self.parts.lock().unwrap();
            for (_, p) in parts.drain() {
                p.bar.finish_and_clear();
            }
        }
        let style =
            ProgressStyle::with_template(template).expect("templating a status line cannot fail");
        self.parent.set_style(style);
        self.parent.finish_with_message(message);
    }
}

impl ProgressReporter for CliReporter {
    fn on_event(&self, event: ProgressEvent) {
        match event {
            ProgressEvent::FilenameResolved(name) => {
                *self.filename.lock().unwrap() = Some(name.clone());
                self.parent.set_message(name);
            }
            ProgressEvent::PhaseChanged(phase) => {
                // Once Downloading begins (or any later phase), restore the
                // filename as the bar message so transient phase labels
                // ("Resolving conflicts", "Evaluating") don't stick.
                // Assembling/Flushing/Verifying still get explicit labels
                // because they replace the download progress.
                match phase {
                    Phase::Downloading => {
                        if let Some(name) = self.filename.lock().unwrap().clone() {
                            self.parent.set_message(name);
                        }
                        // An engine with parts announces them instead; this
                        // only takes effect when none ever arrive.
                        self.ensure_solo_row();
                    }
                    _ => {
                        self.parent.set_message(phase_label(phase));
                    }
                }
            }
            ProgressEvent::Progress { downloaded, total } => {
                self.parent_metrics
                    .downloaded
                    .store(downloaded, Ordering::Relaxed);
                if let Some(t) = total {
                    self.parent_metrics.total.store(t, Ordering::Relaxed);
                    self.parent.set_length(t);
                }
                self.parent.set_position(downloaded);

                // With no parts to report their own share, the aggregate is
                // what the stand-in row shows.
                if let Some(p) = self.solo.lock().unwrap().as_ref() {
                    p.metrics.downloaded.store(downloaded, Ordering::Relaxed);
                    if let Some(t) = total {
                        p.metrics.total.store(t, Ordering::Relaxed);
                        p.bar.set_length(t);
                    }
                    p.bar.set_position(downloaded);
                }
            }
            ProgressEvent::Speed { bytes_per_second } => {
                self.parent_metrics
                    .speed_bits
                    .store(bytes_per_second.to_bits(), Ordering::Relaxed);
                if let Some(p) = self.solo.lock().unwrap().as_ref() {
                    p.metrics
                        .speed_bits
                        .store(bytes_per_second.to_bits(), Ordering::Relaxed);
                }
            }
            ProgressEvent::PartAdded { ulid, size, .. } => {
                // Real parts draw their own rows, so the stand-in steps aside.
                self.clear_solo_row();
                let metrics = BarMetrics::new();
                metrics.total.store(size, Ordering::Relaxed);
                let style = build_child_style(&metrics);
                let bar = self.mp.add(ProgressBar::new(size).with_style(style));
                bar.enable_steady_tick(SAMPLE_INTERVAL);
                self.parts
                    .lock()
                    .unwrap()
                    .insert(ulid, PartBar { bar, metrics });
            }
            ProgressEvent::PartProgress {
                ulid,
                downloaded,
                total,
            } => {
                if let Some(p) = self.parts.lock().unwrap().get(&ulid) {
                    p.metrics.downloaded.store(downloaded, Ordering::Relaxed);
                    p.metrics.total.store(total, Ordering::Relaxed);
                    p.bar.set_length(total);
                    p.bar.set_position(downloaded);
                }
            }
            ProgressEvent::PartSpeed {
                ulid,
                bytes_per_second,
            } => {
                if let Some(p) = self.parts.lock().unwrap().get(&ulid) {
                    p.metrics
                        .speed_bits
                        .store(bytes_per_second.to_bits(), Ordering::Relaxed);
                }
            }
            ProgressEvent::PartFinished { ulid } => {
                if let Some(p) = self.parts.lock().unwrap().remove(&ulid) {
                    p.bar.finish_and_clear();
                }
            }
            ProgressEvent::PartRetrying { ulid, attempt } => {
                if let Some(p) = self.parts.lock().unwrap().get(&ulid) {
                    p.bar.set_message(format!("retry #{attempt}"));
                }
            }
            ProgressEvent::Message(msg) => {
                if !msg.is_empty() {
                    self.parent.set_message(msg);
                }
            }
            ProgressEvent::Completed {
                path,
                already_complete,
            } => {
                // Drain child bars first so the final parent line lands at
                // the bottom of the group with no leftover assembly /
                // part rows.
                self.clear_solo_row();
                {
                    let mut parts = self.parts.lock().unwrap();
                    for (_, p) in parts.drain() {
                        p.bar.finish_and_clear();
                    }
                }
                // Replace the parent's template with a single-line "saved"
                // style so the path is rendered as the final state of the
                // bar itself. Using `mp.println` would queue the line
                // above active bars where it could be overwritten on
                // redraw; baking it into the bar's own line avoids that.
                let suffix = if already_complete {
                    " (already complete)"
                } else {
                    ""
                };
                let final_style = ProgressStyle::with_template("✓ Saved to {msg}")
                    .expect("templating final progress should not fail");
                self.parent.set_style(final_style);
                self.parent
                    .finish_with_message(format!("{}{}", path.display(), suffix));
            }
            ProgressEvent::Cancelled => {
                self.finish_with_status("✕ Cancelled: {msg}", self.describe_target());
            }
            ProgressEvent::Failed { message } => {
                // The bar's own template is dropped first: leaving it would
                // freeze a percentage, speed and ETA next to the failure, all
                // of which stopped being true the moment it failed.
                self.finish_with_status(
                    "✕ {msg}",
                    format!("{}: {message}", self.describe_target()),
                );
            }
            // An engine may report something this build has no rendering for.
            // Ignoring it keeps the display honest rather than guessing.
            _ => {}
        }
    }
}

fn install_metric_keys(style: ProgressStyle, metrics: &BarMetrics) -> ProgressStyle {
    let speed_for_key = Arc::clone(&metrics.speed_bits);
    let dl_for_eta = Arc::clone(&metrics.downloaded);
    let total_for_eta = Arc::clone(&metrics.total);
    let speed_for_eta = Arc::clone(&metrics.speed_bits);
    style
        .with_key(
            "fast_speed",
            move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let bytes = f64::from_bits(speed_for_key.load(Ordering::Relaxed));
                let bytes = if bytes.is_finite() && bytes >= 0.0 {
                    bytes as u64
                } else {
                    0
                };
                let _ = write!(w, "{}/s", HumanBytes(bytes));
            },
        )
        .with_key(
            "fast_eta",
            move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let total = total_for_eta.load(Ordering::Relaxed);
                let downloaded = dl_for_eta.load(Ordering::Relaxed);
                let speed = f64::from_bits(speed_for_eta.load(Ordering::Relaxed));
                if total == 0 || downloaded >= total || !speed.is_finite() || speed <= 1.0 {
                    let _ = write!(w, "--");
                    return;
                }
                let remaining = (total - downloaded) as f64 / speed;
                let secs = remaining as u64;
                let _ = write!(
                    w,
                    "{:02}:{:02}:{:02}",
                    secs / 3600,
                    (secs / 60) % 60,
                    secs % 60
                );
            },
        )
}

/// Style for the stand-in row of a single-piece download.
///
/// Only the bar: the header above it already carries the size, speed and ETA,
/// and repeating them verbatim one line down would read as a rendering fault
/// rather than as detail. Same width as a part row, so a download looks the
/// same whether or not it has parts.
fn build_solo_row_style() -> ProgressStyle {
    ProgressStyle::with_template("  ↳ {bar:30.cyan/blue} {msg}")
        .expect("templating progress bar should not fail")
        .progress_chars(PROGRESS_CHARS)
}

fn build_parent_style(metrics: &BarMetrics) -> ProgressStyle {
    let style = ProgressStyle::with_template(
        "{spinner} {maybe_connect} {msg:!40}   {percent:>3}%  {decimal_bytes:<10} / {decimal_total_bytes:<10} {fast_speed:<14} eta {fast_eta:>9} elapsed {elapsed}",
    )
    .expect("templating progress bar should not fail")
    .progress_chars(PROGRESS_CHARS)
    .with_key(
        "maybe_connect",
        |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            if state.len().is_none() || state.len().is_some_and(|x| x == 0) {
                let _ = write!(w, "━");
            } else {
                let _ = write!(w, "┌");
            }
        },
    );
    install_metric_keys(style, metrics)
}

fn build_child_style(metrics: &BarMetrics) -> ProgressStyle {
    let style = ProgressStyle::with_template(
        "  ↳ {spinner} {bar:30.cyan/blue} {percent:>3}%  {decimal_bytes:<10} / {decimal_total_bytes:<10} {fast_speed:<14} eta {fast_eta:>9} {msg}",
    )
    .expect("templating progress bar should not fail")
    .progress_chars(PROGRESS_CHARS);
    install_metric_keys(style, metrics)
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Args = Args::parse();
    init_tracing(args.log_level);
    let format = args.format;
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => report_error(&e, format),
    }
}

async fn run(args: Args) -> Result<(), OdlError> {
    let format = args.format;

    // If no input and no subcommand provided, show help and exit
    if args.command.is_none() && args.input.is_none() {
        let mut cmd = Args::command();
        if let Err(e) = cmd.print_help() {
            eprintln!("Failed to print help: {}", e);
        }
        println!();
        return Ok(());
    }

    // Handle `odl config` subcommand if provided
    if let Some(cmd) = &args.command {
        match cmd {
            args::Commands::Config {
                show,
                config_file,
                download_dir,
                max_connections,
                max_concurrent_downloads,
                max_retries,
                wait_between_retries,
                n_fixed_retries,
                speed_limit,
                user_agent,
                randomize_user_agent,
                proxy,
                timeout,
                use_server_time,
                accept_invalid_certs,
                http2,
                dynamic_split,
            } => {
                // determine directory where config is stored
                let config_path = if let Some(c) = config_file {
                    c.clone()
                } else if let Some(c) = &args.config_file {
                    c.clone()
                } else {
                    Config::default_config_file()
                };

                let mut cfg: Config = Config::load_from_file(&config_path)
                    .await
                    .unwrap_or_default();

                if *show {
                    match format {
                        OutputFormat::Json => {
                            let v = json!({
                                "type": "config",
                                "path": config_path.display().to_string(),
                                "config": cfg,
                            });
                            println!("{v}");
                        }
                        OutputFormat::Text => {
                            println!("# config path: {}", config_path.display());
                            match toml::to_string_pretty(&cfg) {
                                Ok(s) => {
                                    if s.trim().is_empty() {
                                        println!("# config is empty")
                                    } else {
                                        println!("{}", s)
                                    }
                                }
                                Err(e) => eprintln!("Failed to format config: {}", e),
                            }
                        }
                    }
                    return Ok(());
                }

                // apply command-specified settings into config
                let mut dl_b = cfg.download().clone().into_builder();
                if let Some(v) = max_connections {
                    dl_b.max_connections(*v);
                }
                if let Some(v) = max_retries {
                    dl_b.max_retries(*v);
                }
                if let Some(v) = wait_between_retries {
                    dl_b.wait_between_retries(*v);
                }
                if let Some(v) = n_fixed_retries {
                    dl_b.n_fixed_retries(*v);
                }
                if let Some(v) = user_agent {
                    dl_b.user_agent(Some(v.clone()));
                }
                if let Some(v) = randomize_user_agent {
                    dl_b.randomize_user_agent(*v);
                }
                if let Some(v) = proxy {
                    dl_b.proxy(Some(v.clone()));
                }
                if let Some(v) = use_server_time {
                    dl_b.use_server_time(*v);
                }
                if let Some(v) = accept_invalid_certs {
                    dl_b.accept_invalid_certs(*v);
                }
                if let Some(v) = speed_limit {
                    dl_b.speed_limit(Some(*v));
                }
                if let Some(v) = *timeout {
                    dl_b.connect_timeout(Some(v));
                }
                if let Some(v) = http2 {
                    dl_b.http2(*v);
                }
                if let Some(v) = dynamic_split {
                    dl_b.dynamic_split(*v);
                }
                let new_download = dl_b.build()?;

                let mut cfg_b = cfg.into_builder();
                cfg_b.download(new_download);
                if let Some(v) = download_dir {
                    cfg_b.download_dir(v.clone());
                }
                if let Some(v) = max_concurrent_downloads {
                    cfg_b.max_concurrent_downloads(*v);
                }
                cfg = cfg_b.build()?;

                match cfg.save_to_file(&config_path).await {
                    Ok(()) => match format {
                        OutputFormat::Json => {
                            let v = json!({
                                "type": "config_saved",
                                "path": config_path.display().to_string(),
                            });
                            println!("{v}");
                        }
                        OutputFormat::Text => {
                            println!("Saved configuration to {}", config_path.display())
                        }
                    },
                    Err(e) => {
                        return Err(OdlError::StdIoError {
                            e,
                            extra_info: Some("failed to save configuration".to_string()),
                        });
                    }
                }
                return Ok(());
            }
            args::Commands::Tools { action } => {
                return run_tools(&args, action, format).await;
            }
            args::Commands::Probe { url } => {
                return run_probe(&args, url, format).await;
            }
            args::Commands::Status { filter } => {
                return run_status(&args, filter.as_deref(), format, false).await;
            }
            args::Commands::List { filter } => {
                return run_status(&args, filter.as_deref(), format, true).await;
            }
        }
    }
    let mp = Arc::new(MultiProgress::new());

    let dlm = build_download_manager(&args).await?;

    let mut download_type = determine_download_type(&args)?;

    if let DownloadType::FileAtUrl(url) = &download_type {
        let path = download_remote_file(&dlm, url.clone()).await?;
        download_type = DownloadType::File(Box::new(path));
    }

    let mut user_provided_filename: Option<String> = None;
    let save_dir: PathBuf = if let Some(path) = args.output.clone() {
        if let DownloadType::Url(_) = &download_type {
            user_provided_filename = path
                .file_name()
                .map(|os_str| os_str.to_string_lossy().into_owned());
            path.parent()
                .expect("Failed to get output's parent directory")
                .to_path_buf()
        } else {
            path
        }
    } else {
        std::env::current_dir()?
    };

    // todo: stream file, as processing a large file in advance is not a good idea
    let mut urls = Vec::new();
    match &download_type {
        DownloadType::Url(url) => {
            urls.push(url.clone());
        }
        DownloadType::File(path) => {
            let file = tokio::fs::File::open(&**path).await?;
            let reader = tokio::io::BufReader::new(file);
            let mut lines = tokio::io::BufReader::new(reader).lines();
            while let Some(line) = lines.next_line().await? {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("//") {
                    continue;
                }
                match Url::parse(trimmed) {
                    Ok(url) => urls.push(url),
                    Err(e) => {
                        println!("Skipping invalid URL '{}': {}", trimmed, e);
                    }
                }
            }
        }
        DownloadType::FileAtUrl(_) => {
            panic!("FileAtUrl should have been handled already");
        }
    }

    // We'll spawn tasks only after acquiring a permit so we don't
    // allocate a future per-URL up-front (which wastes resources for
    // large remote lists). This keeps the number of active tasks
    // bounded by the download manager's semaphore.
    let mut handles = Vec::new();

    let expected_checksums = args
        .checksums
        .iter()
        .map(|s| odl::hash::HashDigest::parse_cli(s))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|message| OdlError::CliError { message })?;

    let dlm = Arc::new(dlm);
    let credentials = if let Some(user) = args.http_user.as_deref() {
        Some(Credentials::new(user, args.http_password.as_deref()))
    } else {
        None
    };

    let resolver = {
        // map CLI enum choices to internal conflict enums
        let file_changed = match args.on_file_changed {
            args::FileChangedAction::Abort => FileChangedResolution::Abort,
            args::FileChangedAction::Restart => FileChangedResolution::Restart,
        };
        let not_resumable = match args.on_not_resumable {
            args::NotResumableAction::Abort => NotResumableResolution::Abort,
            args::NotResumableAction::Restart => NotResumableResolution::Restart,
        };
        let same_download_exists = match args.on_same_download_exists {
            args::SameDownloadAction::Abort => SameDownloadExistsResolution::Abort,
            args::SameDownloadAction::Resume => SameDownloadExistsResolution::Resume,
            args::SameDownloadAction::AddNumberToNameAndContinue => {
                SameDownloadExistsResolution::AddNumberToNameAndContinue
            }
        };
        let final_file_exists = match args.on_final_file_exists {
            args::FinalFileAction::Abort => FinalFileExistsResolution::Abort,
            args::FinalFileAction::ReplaceAndContinue => {
                FinalFileExistsResolution::ReplaceAndContinue
            }
            args::FinalFileAction::AddNumberToNameAndContinue => {
                FinalFileExistsResolution::AddNumberToNameAndContinue
            }
        };

        CliResolver {
            file_changed,
            not_resumable,
            same_download_exists,
            final_file_exists,
        }
    };

    let engine_preference = match args.engine {
        args::EngineChoice::Auto => EnginePreference::Auto,
        args::EngineChoice::Http => EnginePreference::Engine(Engine::HttpMultipart),
        args::EngineChoice::Ytdlp => EnginePreference::Engine(Engine::Ytdlp),
    };

    // Before anything is downloaded: if one of these links needs a helper that
    // is missing, this is the moment to offer it — mid-download would be too
    // late, and after the fact would mean the wrong file was already fetched.
    // Rebuilt when this installs something: the manager holds a snapshot of
    // the config taken before the install, so without this the helper it just
    // fetched would be invisible for the rest of the run — and a media link
    // would quietly fall back to fetching the web page.
    let dlm = if maybe_offer_missing_tools(&args, &urls, format, &dlm).await? {
        Arc::new(build_download_manager(&args).await?)
    } else {
        dlm
    };

    let cancel = CancellationToken::new();
    spawn_interrupt_handler(cancel.clone());

    // Naming a format is itself a decision to re-decide: it would be useless
    // if an already-pinned download ignored it.
    let reselect_format =
        args.format_id.is_some() || args.choose_format == args::ChooseFormat::Always;
    let format_selector: Arc<dyn FormatSelector> = match args.format_id.clone() {
        Some(id) => Arc::new(FixedFormatSelector(id)),
        None if should_prompt_for_format(args.choose_format, format, urls.len()) => {
            Arc::new(InteractiveFormatSelector {
                mp: Arc::clone(&mp),
            })
        }
        None => Arc::new(DefaultFormatSelector),
    };

    for url in urls.into_iter() {
        let reporter: Arc<dyn ProgressReporter> = match format {
            // NDJSON events go straight to stdout. Progress fires at the
            // sampler cadence (~8 Hz), so printing on the download task is
            // cheap and needs no async-forwarder buffering.
            OutputFormat::Json => Arc::new(JsonReporter::new(url.to_string())),
            OutputFormat::Text => {
                let parent_metrics = BarMetrics::new();
                let parent_style = build_parent_style(&parent_metrics);

                // Length-less rather than zero-length: a zero-length bar reads
                // as 100% complete, so the header would claim the download had
                // finished while it was still being evaluated.
                let parent = mp.add(ProgressBar::new_spinner().with_style(parent_style));
                parent.set_message(format!("{url} (warming up)"));
                parent.enable_steady_tick(SAMPLE_INTERVAL);

                // Wrap the CliReporter in an async forwarder so every `emit`
                // hands the event off to a worker task via a lock-free mpsc
                // and returns immediately. Indicatif `set_position` / Mutex
                // hops never run on the download tasks themselves.
                let cli_reporter =
                    CliReporter::new(Arc::clone(&mp), parent, parent_metrics, url.to_string());
                AsyncReporter::spawn(cli_reporter) as Arc<dyn ProgressReporter>
            }
        };
        let ctx = DownloadContext::new()
            .with_reporter(reporter)
            .with_url(url.clone())
            // One token for every download, so a single interrupt stops the
            // whole run rather than the one job that happened to be first.
            .with_cancel(cancel.clone());

        let dlm = Arc::clone(&dlm);
        let save_dir = save_dir.clone();
        let user_provided_filename = user_provided_filename.clone();
        let credentials = credentials.clone();
        let expected_checksums = expected_checksums.clone();
        let format_selector = Arc::clone(&format_selector);
        // `resolver` is `Copy`, closures will capture by value; no extra binding needed.

        // Wait here for a permit before spawning the task. This ensures we
        // don't construct/spawn more tasks than permits available.
        let permit = dlm
            .acquire_download_permit()
            .await
            .expect("didn't expect the semaphore to close at this point");

        // Move the permit into the spawned task so it is held for the
        // duration of the download and released automatically when the
        // task completes.
        let handle = tokio::spawn(async move {
            let _permit = permit;
            let result: Result<PathBuf, OdlError> = async {
                // `download` emits its own terminal event; `evaluate` does
                // not, so only the evaluate half reports here. Emitting for
                // both would give one download two endings — the transcript
                // showed "Cancelled" and "Failed: download cancelled" for a
                // single Ctrl-C.
                let mut request = EvaluateRequest::new(url, save_dir, &resolver)
                    .ctx(&ctx)
                    .engine(engine_preference)
                    .format_selector(&*format_selector)
                    // Asking explicitly is also how a user changes the quality
                    // of a download already started.
                    .reselect_format(reselect_format);
                if let Some(c) = credentials {
                    request = request.credentials(c);
                }
                let mut instruction = match dlm.evaluate(request).await {
                    Ok(instruction) => instruction,
                    Err(OdlError::Cancelled) => {
                        ctx.emit(ProgressEvent::Cancelled);
                        return Err(OdlError::Cancelled);
                    }
                    Err(e) => {
                        ctx.emit(ProgressEvent::Failed {
                            message: e.to_string(),
                        });
                        return Err(e);
                    }
                };
                if let Some(filename) = user_provided_filename {
                    instruction.set_filename(filename);
                }
                instruction.add_checksums(expected_checksums);
                dlm.download(DownloadRequest::new(instruction, &resolver).ctx(&ctx))
                    .await
            }
            .await;
            result
        });

        handles.push(handle);
    }

    // Await all tasks and normalize JoinErrors into OdlError
    let mut results: Vec<Result<PathBuf, OdlError>> = Vec::new();
    for h in handles {
        match h.await {
            Ok(Ok(path)) => results.push(Ok(path)),
            Ok(Err(e)) => results.push(Err(e)),
            Err(join_err) => results.push(Err(OdlError::from(join_err))),
        }
    }
    // Per-download failures were already surfaced through each download's
    // reporter (a `Failed` bar line in text mode, a `failed` NDJSON event
    // in json mode). Propagate the first failure so the process exit code
    // reflects it; `report_error` prints the single top-level summary.
    let first_err = results.into_iter().find_map(|r| r.err());
    if let Some(e) = first_err {
        return Err(e);
    }

    Ok(())
}

#[instrument(skip(args), name = "Warming up odl...")]
async fn build_download_manager(args: &Args) -> Result<DownloadManager, OdlError> {
    // determine where config would live (same logic used by download manager default)
    let config_file: PathBuf = if let Some(c) = &args.config_file {
        c.clone()
    } else {
        Config::default_config_file()
    };

    let cfg = Config::load_from_file(&config_file)
        .await
        .unwrap_or_default();

    let wait_between_retries = args.wait_between_retries.and_then(|d| {
        let secs = d.as_secs_f64();
        if secs.is_finite() && secs >= 0.0 {
            Some(d)
        } else {
            None
        }
    });
    let connect_timeout = args.timeout.and_then(|d| {
        let secs = d.as_secs_f64();
        if secs.is_finite() && secs >= 0.0 {
            Some(d)
        } else {
            None
        }
    });

    let headers = if args.headers.is_empty() {
        None
    } else {
        let mut headers_map = indexmap::IndexMap::new();
        for header in &args.headers {
            if let Some((key, value)) = header.split_once(':') {
                let key = key.trim();
                let value = value.trim();
                headers_map.insert(key.to_string(), value.to_string());
            } else {
                return Err(OdlError::CliError {
                    message: format!("Header must be in KEY:VALUE format: '{}'", header),
                });
            }
        }
        Some(headers_map)
    };

    let mut dl_b = cfg.download().clone().into_builder();
    if let Some(v) = args.max_connections {
        dl_b.max_connections(v);
    }
    if let Some(v) = args.max_retries {
        dl_b.max_retries(v);
    }
    if let Some(v) = wait_between_retries {
        dl_b.wait_between_retries(v);
    }
    if let Some(v) = args.n_fixed_retries {
        dl_b.n_fixed_retries(v);
    }
    if let Some(v) = args.user_agent.clone() {
        dl_b.user_agent(Some(v));
    }
    if let Some(v) = args.randomize_user_agent {
        dl_b.randomize_user_agent(v);
    }
    if let Some(v) = args.proxy.clone() {
        dl_b.proxy(Some(v));
    }
    if let Some(v) = args.use_server_time {
        dl_b.use_server_time(v);
    }
    if let Some(v) = args.accept_invalid_certs {
        dl_b.accept_invalid_certs(v);
    }
    if let Some(v) = args.speed_limit {
        dl_b.speed_limit(Some(v));
    }
    // Only ever turned off from the command line: leaving it out must not
    // override a config that asked for verification.
    if args.no_verify_checksums {
        dl_b.verify_checksums(false);
    }
    if let Some(v) = connect_timeout {
        dl_b.connect_timeout(Some(v));
    }
    if let Some(v) = headers {
        dl_b.headers(Some(v));
    }
    if let Some(v) = args.http2 {
        dl_b.http2(v);
    }
    if let Some(v) = args.dynamic_split {
        dl_b.dynamic_split(v);
    }
    let download = dl_b.build()?;

    let mut cfg_b = cfg.into_builder();
    cfg_b.download(download);
    if let Some(v) = args.download_dir.clone() {
        cfg_b.download_dir(v);
    }
    if let Some(v) = args.max_concurrent_downloads {
        cfg_b.max_concurrent_downloads(v);
    }
    let cfg = cfg_b.build()?;

    Ok(DownloadManager::new(cfg))
}

#[instrument(skip(args), name = "Determining download type")]
fn determine_download_type(args: &Args) -> Result<DownloadType, OdlError> {
    // require input to be present for normal operation
    let input = args.input.as_ref().ok_or(OdlError::CliError {
        message: "Missing input. Provide a URL or file path, or use a subcommand like `config`."
            .to_string(),
    })?;

    Ok(match Url::parse(input) {
        Ok(url) => {
            if args.remote_list {
                DownloadType::FileAtUrl(url)
            } else {
                DownloadType::Url(url)
            }
        }
        Err(_) => {
            let path = PathBuf::from(input);
            if path.try_exists()? {
                if args.remote_list {
                    return Err(OdlError::CliError {
                        message: "Expected input to be a Url, found file path instead".to_string(),
                    });
                }
                DownloadType::File(Box::new(path))
            } else {
                return Err(OdlError::CliError {
                    message: "Input is not a valid Url or a valid file path. Check file permissions if file exists.".to_string(),
                });
            }
        }
    })
}

struct ForcedResolver;
#[async_trait]
impl ServerConflictResolver for ForcedResolver {
    async fn resolve_file_changed(&self, _: &Download) -> FileChangedResolution {
        FileChangedResolution::Restart
    }
    async fn resolve_not_resumable(&self, _: &Download) -> NotResumableResolution {
        NotResumableResolution::Restart
    }
}

#[async_trait]
impl SaveConflictResolver for ForcedResolver {
    async fn same_download_exists(&self, _: &Download) -> SameDownloadExistsResolution {
        SameDownloadExistsResolution::Resume
    }
    async fn final_file_exists(&self, _: &Download) -> FinalFileExistsResolution {
        FinalFileExistsResolution::ReplaceAndContinue
    }
}

#[instrument(skip(dlm), name = "Downloading remote file containing links")]
async fn download_remote_file(dlm: &DownloadManager, url: Url) -> Result<PathBuf, OdlError> {
    let resolver = ForcedResolver {};
    // Create a temporary directory in the OS temp dir for saving the remote file
    let tmpdir = tempfile::Builder::new()
        .prefix("odl")
        .tempdir()
        .map_err(|e| OdlError::CliError {
            message: format!("Failed to create temp dir: {e}"),
        })?;
    let save_dir = tmpdir.path().to_path_buf();

    // Acquire a download permit so this remote-file download counts
    // against the same concurrency limits as other downloads
    let _permit = dlm.acquire_download_permit().await?;

    let instruction = dlm
        .evaluate(
            // A list of links is a plain text file, never a media page: an
            // engine that resolves media would be the wrong tool entirely.
            EvaluateRequest::new(url, save_dir, &resolver)
                .engine(EnginePreference::Engine(Engine::HttpMultipart)),
        )
        .await?;

    let path = dlm
        .download(DownloadRequest::new(instruction, &resolver))
        .await?;

    // `_permit` will be dropped here when going out of scope, releasing
    // the semaphore permit back to the manager.
    Ok(path)
}

/// Map a metadata checksum to a JSON object with stable string-named
/// algorithm/encoding fields.
fn checksum_json(c: &odl::download_metadata::FileChecksum) -> serde_json::Value {
    use odl::download_metadata::{ChecksumAlgorithm, ChecksumEncoding};
    let algorithm = ChecksumAlgorithm::try_from(c.algorithm)
        .map(|a| a.as_str_name())
        .unwrap_or("unknown");
    let encoding = ChecksumEncoding::try_from(c.encoding)
        .map(|e| e.as_str_name())
        .unwrap_or("unknown");
    json!({"algorithm": algorithm, "digest": c.digest, "encoding": encoding})
}

/// Offer to install the helpers when a link needs them and they are absent.
///
/// Only ever asks on a terminal, only when a link actually calls for the
/// engine, and only for a helper that is genuinely missing. ffmpeg is asked
/// about after yt-dlp, and only if yt-dlp is now present: on its own it would
/// do nothing.
#[cfg(feature = "ytdlp")]
async fn maybe_offer_missing_tools(
    args: &Args,
    urls: &[Url],
    format: OutputFormat,
    dlm: &DownloadManager,
) -> Result<bool, OdlError> {
    use odl::ytdlp::install::{self, Tool};

    // A prompt would corrupt a JSON stream and cannot be answered by a script.
    if format != OutputFormat::Text
        || !std::io::stdin().is_terminal()
        || args.engine == args::EngineChoice::Http
    {
        return Ok(false);
    }

    let config_path = config_path_for(args);
    let mut cfg = Config::load_from_file(&config_path).await?;
    if !cfg.ytdlp().enabled() {
        return Ok(false);
    }

    let forced = args.engine == args::EngineChoice::Ytdlp;
    let wanted = forced
        || urls
            .iter()
            .any(|u| odl::ytdlp::should_delegate(u, cfg.ytdlp()));
    if !wanted {
        return Ok(false);
    }

    let mut tools = odl::ytdlp::tools(cfg.ytdlp()).await.ok();
    let mut changed = false;
    let mut installed_something = false;

    // A previous decline is remembered: being asked the same question on every
    // media link would be nagging, and the answer was already given.
    if tools.is_none() && cfg.ytdlp().offer_ytdlp_install() {
        let mut ytdlp_cfg = cfg.ytdlp().clone();
        match offer_tool(Tool::Ytdlp, None, &config_path, cfg.download(), dlm, false).await? {
            OfferOutcome::Installed(path) => {
                ytdlp_cfg.set_binary_path(Some(path));
                changed = true;
                installed_something = true;
            }
            OfferOutcome::Declined => {
                ytdlp_cfg.set_offer_ytdlp_install(false);
                changed = true;
            }
            OfferOutcome::NothingToDo => {}
        }
        cfg = cfg.into_builder().ytdlp(ytdlp_cfg).build()?;
        if changed {
            tools = odl::ytdlp::tools(cfg.ytdlp()).await.ok();
        }
    }

    // Only worth asking once the engine that would use it exists.
    if let Some(t) = &tools
        && t.ffmpeg.is_none()
        && cfg.ytdlp().offer_ffmpeg_install()
        && install::can_install(Tool::Ffmpeg)
    {
        let mut ytdlp_cfg = cfg.ytdlp().clone();
        match offer_tool(Tool::Ffmpeg, None, &config_path, cfg.download(), dlm, false).await? {
            OfferOutcome::Installed(path) => {
                ytdlp_cfg.set_ffmpeg_path(Some(path));
                changed = true;
                installed_something = true;
            }
            OfferOutcome::Declined => {
                ytdlp_cfg.set_offer_ffmpeg_install(false);
                changed = true;
            }
            OfferOutcome::NothingToDo => {}
        }
        cfg = cfg.into_builder().ytdlp(ytdlp_cfg).build()?;
    }

    if changed {
        cfg.save_to_file(&config_path).await?;
        eprintln!("Updated {}\n", config_path.display());
    }
    // Only an install changes what a download can do; a recorded decline does
    // not, so it is not worth rebuilding the manager for.
    Ok(installed_something)
}

#[cfg(not(feature = "ytdlp"))]
async fn maybe_offer_missing_tools(
    _args: &Args,
    _urls: &[Url],
    _format: OutputFormat,
    _dlm: &DownloadManager,
) -> Result<bool, OdlError> {
    Ok(false)
}

/// Path of the config file this invocation reads and writes.
///
/// Only the helper-install paths need it, which a build without a delegating
/// engine does not compile.
#[cfg(feature = "ytdlp")]
fn config_path_for(args: &Args) -> PathBuf {
    args.config_file
        .clone()
        .unwrap_or_else(Config::default_config_file)
}

/// Ask a yes/no question on the terminal.
///
///
/// Returns `false` without prompting when there is no terminal to ask on:
/// a script must never be blocked by a question it cannot see.
#[cfg(feature = "ytdlp")]
fn confirm(question: &str) -> Option<bool> {
    use std::io::Write;
    // `None`, not `false`: "nobody could be asked" is not the same as "the
    // user said no". Recording the second when only the first happened would
    // silently disable a future offer on the strength of a question that was
    // never put.
    if !std::io::stdin().is_terminal() || !std::io::stderr().is_terminal() {
        return None;
    }
    let mut err = std::io::stderr();
    let _ = write!(err, "{question} [y/N]: ");
    let _ = err.flush();
    let mut line = String::new();
    if std::io::stdin().read_line(&mut line).is_err() {
        return None;
    }
    Some(matches!(
        line.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

/// HTTP client for fetching a helper.
///
/// Built from the user's own download settings rather than from invented
/// constants: someone who needs a proxy to reach the internet needs it here
/// too, and a connect timeout they chose should not be silently overridden.
#[cfg(feature = "ytdlp")]
fn install_client(net: &odl::config::DownloadOptions) -> Result<reqwest::Client, OdlError> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = Option::<reqwest::Proxy>::from(net) {
        builder = builder.proxy(proxy);
    }
    if net.accept_invalid_certs() {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(timeout) = net.connect_timeout() {
        builder = builder.connect_timeout(timeout);
    }
    builder.build().map_err(|e| OdlError::CliError {
        message: format!("could not create an HTTP client: {e}"),
    })
}

/// What came of offering to install a helper.
#[cfg(feature = "ytdlp")]
enum OfferOutcome {
    Installed(PathBuf),
    /// The user said no. Recorded so the question is not repeated.
    Declined,
    /// Nothing was asked: it is already there, or there is no build to offer.
    NothingToDo,
}

/// Describe one helper, and offer to install it when it is missing.
#[cfg(feature = "ytdlp")]
async fn offer_tool(
    tool: odl::ytdlp::install::Tool,
    installed: Option<&std::path::Path>,
    config_path: &std::path::Path,
    net: &odl::config::DownloadOptions,
    dlm: &DownloadManager,
    assume_yes: bool,
) -> Result<OfferOutcome, OdlError> {
    use odl::ytdlp::install;

    if let Some(path) = installed {
        eprintln!("{}: already installed at {}", tool.as_str(), path.display());
        return Ok(OfferOutcome::NothingToDo);
    }
    if !install::can_install(tool) {
        eprintln!(
            "{}: not installed, and odl has no verified build for this platform.\n  Install it yourself — {} — then set `{}` in {}.",
            tool.as_str(),
            tool.manual_instructions(),
            tool.config_key(),
            config_path.display(),
        );
        return Ok(OfferOutcome::NothingToDo);
    }

    let dir = install::tools_dir();
    eprintln!("\n{} is not installed.", tool.as_str());
    eprintln!("  {}", tool.purpose());
    eprintln!("  odl can download it from {}.", tool.source_description());
    eprintln!("  It will be verified against the checksums published with it, saved to");
    eprintln!(
        "  {}, and recorded as `{}` in",
        dir.display(),
        tool.config_key()
    );
    eprintln!("  {}.", config_path.display());
    eprintln!(
        "  Or install it yourself — {} — and set that key by hand.",
        tool.manual_instructions()
    );

    if !assume_yes {
        match confirm(&format!("Download {} now?", tool.as_str())) {
            Some(true) => {}
            Some(false) => {
                eprintln!(
                    "Skipped {0}. odl will not ask again; run `odl tools install {0}` when you want it.",
                    tool.as_str()
                );
                return Ok(OfferOutcome::Declined);
            }
            // Nothing to answer with. Say what would have happened and leave
            // the configuration untouched, so a scripted run cannot record a
            // refusal the user never made.
            None => {
                eprintln!(
                    "Not installing {0}: no terminal to confirm on. Re-run with `-y` to install it without asking.",
                    tool.as_str()
                );
                return Ok(OfferOutcome::NothingToDo);
            }
        }
    }

    // The release listing is a few kilobytes of JSON, fetched directly with
    // the user's own network settings — their proxy, connect timeout and
    // certificate policy.
    let client = install_client(net)?;
    let plan = install::plan(&client, tool).await.map_err(OdlError::from)?;

    // The asset itself goes through odl's own downloader: resumable, retrying,
    // checksum-verified. Fetching forty megabytes over a bad line is the
    // problem this program exists to solve, and doing it worse here would be
    // a poor advertisement.
    eprintln!("Downloading {} ({})…", tool.as_str(), plan.name);
    let downloaded = download_asset(dlm, &plan).await?;

    let path = install::finish(tool, &downloaded, &dir)
        .await
        .map_err(OdlError::from)?;
    // The staged copy has served its purpose; leaving it would double the disk
    // cost of every installed tool.
    let _ = tokio::fs::remove_file(&downloaded).await;

    eprintln!("Installed {} to {}", tool.as_str(), path.display());
    Ok(OfferOutcome::Installed(path))
}

/// Fetch one release asset with odl itself, and return where it landed.
///
/// The HTTP engine is forced rather than left to `auto`: the engine that
/// resolves media links is the very thing being installed, so letting it be
/// chosen here would be circular.
#[cfg(feature = "ytdlp")]
async fn download_asset(
    dlm: &DownloadManager,
    plan: &odl::ytdlp::install::AssetPlan,
) -> Result<PathBuf, OdlError> {
    use odl::ytdlp::install;

    let url = Url::parse(&plan.url).map_err(|e| OdlError::CliError {
        message: format!("release asset URL is not usable: {e}"),
    })?;
    let staging = install::staging_dir();
    tokio::fs::create_dir_all(&staging).await?;

    // Resume what is there, replace a stale complete file, restart if the
    // asset changed underneath us — a release moving on is not a conflict
    // worth stopping for.
    let resolver = ForcedResolver {};
    let mut instruction = dlm
        .evaluate(
            EvaluateRequest::new(url, staging, &resolver)
                .engine(EnginePreference::Engine(Engine::HttpMultipart)),
        )
        .await?;
    // Name it after the asset rather than whatever the URL's last segment
    // happens to be, so a resume finds the same file next time.
    instruction.set_filename(plan.name.clone());

    // Verification is the downloader's job: it checks after assembly and
    // fails with a conflict rather than leaving a bad binary in place.
    let digest = odl::hash::HashDigest::parse_cli(&format!("sha256:{}", plan.sha256))
        .map_err(|message| OdlError::CliError { message })?;
    instruction.add_checksums([digest]);

    dlm.download(DownloadRequest::new(instruction, &resolver))
        .await
}

/// `odl tools status` / `odl tools install` — manage the helper programs.
#[cfg(feature = "ytdlp")]
async fn run_tools(
    args: &Args,
    action: &args::ToolsAction,
    format: OutputFormat,
) -> Result<(), OdlError> {
    use odl::ytdlp::install::Tool;

    let config_path = config_path_for(args);
    let mut cfg = Config::load_from_file(&config_path).await?;
    let dlm = build_download_manager(args).await?;
    let found = odl::ytdlp::tools(cfg.ytdlp()).await.ok();
    let ytdlp_path = found.as_ref().map(|t| t.ytdlp.clone());
    let ffmpeg_path = found.as_ref().and_then(|t| t.ffmpeg.clone());

    match action {
        args::ToolsAction::Status => {
            match format {
                OutputFormat::Json => {
                    let v = json!({
                        "type": "tools",
                        "config_path": config_path.to_string_lossy(),
                        "tools_dir": odl::ytdlp::install::tools_dir().to_string_lossy(),
                        "yt_dlp": ytdlp_path.as_ref().map(|p| p.to_string_lossy()),
                        "ffmpeg": ffmpeg_path.as_ref().map(|p| p.to_string_lossy()),
                        "can_install_yt_dlp": odl::ytdlp::install::can_install(Tool::Ytdlp),
                        "can_install_ffmpeg": odl::ytdlp::install::can_install(Tool::Ffmpeg),
                    });
                    println!("{v}");
                }
                OutputFormat::Text => {
                    println!(
                        "yt-dlp:  {}",
                        ytdlp_path
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "not installed".to_owned())
                    );
                    println!(
                        "ffmpeg:  {}",
                        ffmpeg_path
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "not installed".to_owned())
                    );
                    println!("config:  {}", config_path.display());
                }
            }
            Ok(())
        }
        args::ToolsAction::Install { tool, yes } => {
            // ffmpeg is offered after yt-dlp: it is only worth having once the
            // engine that uses it exists.
            let wanted: Vec<Tool> = match tool {
                Some(args::ToolChoice::Ytdlp) => vec![Tool::Ytdlp],
                Some(args::ToolChoice::Ffmpeg) => vec![Tool::Ffmpeg],
                None => vec![Tool::Ytdlp, Tool::Ffmpeg],
            };

            let mut changed = false;
            for t in wanted {
                let current = match t {
                    Tool::Ytdlp => ytdlp_path.clone(),
                    Tool::Ffmpeg => ffmpeg_path.clone(),
                };
                // An explicit `odl tools install` overrides a past decline:
                // asking for it now is a clearer signal than the old no.
                let outcome = offer_tool(
                    t,
                    current.as_deref(),
                    &config_path,
                    cfg.download(),
                    &dlm,
                    *yes,
                )
                .await?;
                let mut ytdlp_cfg = cfg.ytdlp().clone();
                match (t, outcome) {
                    (Tool::Ytdlp, OfferOutcome::Installed(path)) => {
                        ytdlp_cfg.set_binary_path(Some(path));
                        ytdlp_cfg.set_offer_ytdlp_install(true);
                        changed = true;
                    }
                    (Tool::Ffmpeg, OfferOutcome::Installed(path)) => {
                        ytdlp_cfg.set_ffmpeg_path(Some(path));
                        ytdlp_cfg.set_offer_ffmpeg_install(true);
                        changed = true;
                    }
                    (Tool::Ytdlp, OfferOutcome::Declined) => {
                        ytdlp_cfg.set_offer_ytdlp_install(false);
                        changed = true;
                    }
                    (Tool::Ffmpeg, OfferOutcome::Declined) => {
                        ytdlp_cfg.set_offer_ffmpeg_install(false);
                        changed = true;
                    }
                    (_, OfferOutcome::NothingToDo) => {}
                }
                cfg = cfg.into_builder().ytdlp(ytdlp_cfg).build()?;
            }
            if changed {
                cfg.save_to_file(&config_path).await?;
                eprintln!("Updated {}", config_path.display());
            }
            Ok(())
        }
    }
}

#[cfg(not(feature = "ytdlp"))]
async fn run_tools(
    _args: &Args,
    _action: &args::ToolsAction,
    _format: OutputFormat,
) -> Result<(), OdlError> {
    Err(OdlError::CliError {
        message: "this build of odl was compiled without yt-dlp support".to_owned(),
    })
}

/// `odl probe <url>` — HEAD-probe a URL and report what a download would
/// resolve to, without writing anything. Uses the same config/flags as a
/// real download so the reported filename/resumability match.
async fn run_probe(args: &Args, url_str: &str, format: OutputFormat) -> Result<(), OdlError> {
    let url = Url::parse(url_str).map_err(|e| OdlError::CliError {
        message: format!("Invalid URL '{url_str}': {e}"),
    })?;
    let dlm = build_download_manager(args).await?;
    // Resolve against the cwd but never rename/abort: we want the server's
    // own filename, not a conflict-avoidant alternative.
    let save_dir = std::env::current_dir()?;
    let resolver = ForcedResolver;
    let _permit = dlm.acquire_download_permit().await?;
    let instruction = dlm
        .evaluate(EvaluateRequest::new(url, save_dir, &resolver))
        .await?;

    let checksums: Vec<serde_json::Value> = instruction
        .as_metadata()
        .checksums
        .iter()
        .map(checksum_json)
        .collect();
    let last_modified_rfc3339 = instruction.last_modified_as_date().map(|d| d.to_rfc3339());

    let engine = instruction.engine();
    let caps = engine.capabilities();

    match format {
        OutputFormat::Json => {
            let v = json!({
                "type": "probe",
                "url": instruction.url().to_string(),
                "filename": instruction.filename(),
                "size": instruction.size(),
                "size_is_approx": instruction.size_is_approx(),
                "engine": engine.as_str(),
                "quality": instruction.quality().map(|q| q.to_string()),
                "resumable": instruction.is_resumable(),
                "etag": instruction.etag(),
                "last_modified": instruction.last_modified(),
                "last_modified_rfc3339": last_modified_rfc3339,
                "requires_auth": instruction.requires_auth(),
                "requires_basic_auth": instruction.requires_basic_auth(),
                "checksums": checksums,
            });
            println!("{v}");
        }
        OutputFormat::Text => {
            println!("url:           {}", instruction.url());
            println!("filename:      {}", instruction.filename());
            if engine != Engine::HttpMultipart {
                println!("engine:        {}", engine.as_str());
            }
            if let Some(quality) = instruction.quality() {
                println!("quality:       {quality}");
            }
            let approx = if instruction.size_is_approx() {
                "~"
            } else {
                ""
            };
            match instruction.size() {
                Some(s) => println!("size:          {}{} ({} bytes)", approx, HumanBytes(s), s),
                None => println!("size:          unknown"),
            }
            println!("resumable:     {}", instruction.is_resumable());
            // Fields the engine cannot observe are left out rather than shown
            // as `-`, which would read as "the server sent nothing".
            if caps.response_headers {
                println!(
                    "etag:          {}",
                    instruction.etag().as_deref().unwrap_or("-")
                );
                println!(
                    "last_modified: {}",
                    last_modified_rfc3339.as_deref().unwrap_or("-")
                );
                println!("requires_auth: {}", instruction.requires_auth());
            }
            for c in instruction.as_metadata().checksums.iter() {
                use odl::download_metadata::{ChecksumAlgorithm, ChecksumEncoding};
                let algo = ChecksumAlgorithm::try_from(c.algorithm)
                    .map(|a| a.as_str_name())
                    .unwrap_or("unknown");
                let enc = ChecksumEncoding::try_from(c.encoding)
                    .map(|e| e.as_str_name())
                    .unwrap_or("unknown");
                println!("checksum:      {} {} ({})", algo, c.digest, enc);
            }
        }
    }
    Ok(())
}

/// Percent complete for a download. A finished download is 100% even
/// though its parts have been removed post-assembly (so `downloaded` is
/// 0); otherwise it is `downloaded / size` when the total size is known.
fn percent_complete(downloaded: u64, size: Option<u64>, finished: bool) -> Option<f64> {
    if finished {
        return Some(100.0);
    }
    match size {
        Some(total) if total > 0 => Some((downloaded as f64 / total as f64) * 100.0),
        _ => None,
    }
}

fn status_json(d: &DownloadStatus) -> serde_json::Value {
    json!({
        "filename": d.filename,
        "url": d.url,
        "save_dir": d.save_dir.to_string_lossy(),
        "final_file_path": d.final_file_path.to_string_lossy(),
        "final_file_exists": d.final_file_exists,
        "download_dir": d.download_dir.to_string_lossy(),
        "size": d.size,
        "downloaded": d.downloaded,
        "percent": percent_complete(d.downloaded, d.size, d.finished),
        "finished": d.finished,
        "resumable": d.is_resumable,
        "parts_total": d.parts_total,
        "parts_finished": d.parts_finished,
        "engine": d.engine.as_str(),
        "size_is_approx": d.size_is_approx,
        "quality": d.quality.as_ref().map(|q| q.to_string()),
    })
}

/// `odl status [filter]` / `odl list [filter]` — report tracked downloads
/// from the configured download directory. `brief` controls text density;
/// JSON output is identical for both.
async fn run_status(
    args: &Args,
    filter: Option<&str>,
    format: OutputFormat,
    brief: bool,
) -> Result<(), OdlError> {
    let dlm = build_download_manager(args).await?;
    let mut downloads = dlm.list_downloads().await?;
    if let Some(f) = filter {
        downloads.retain(|d| d.url.contains(f) || d.filename.contains(f));
    }

    match format {
        OutputFormat::Json => {
            let items: Vec<serde_json::Value> = downloads.iter().map(status_json).collect();
            let v = json!({"type": "status", "count": items.len(), "downloads": items});
            println!("{v}");
        }
        OutputFormat::Text => {
            if downloads.is_empty() {
                println!("No tracked downloads.");
                return Ok(());
            }
            for d in &downloads {
                let pct = percent_complete(d.downloaded, d.size, d.finished)
                    .map(|p| format!("{p:.1}%"))
                    .unwrap_or_else(|| "?%".to_string());
                let state = if d.finished {
                    "done"
                } else if d.final_file_exists {
                    "assembled"
                } else {
                    "partial"
                };
                if brief {
                    println!("{:>10}  {:>9}  {}", state, pct, d.filename);
                } else {
                    let caps = d.engine.capabilities();
                    println!("{}", d.filename);
                    println!("  url:        {}", d.url);
                    println!("  state:      {}", state);
                    if d.engine != Engine::HttpMultipart {
                        println!("  engine:     {}", d.engine.as_str());
                    }
                    if let Some(quality) = &d.quality {
                        println!("  quality:    {quality}");
                    }
                    // A `~` marks a size the engine could only estimate, so a
                    // percentage that drifts past 100 reads as expected rather
                    // than as a bug.
                    let approx = if d.size_is_approx { "~" } else { "" };
                    // A finished download has no bytes left in its working
                    // directory — parts are removed after assembly, and a
                    // delegated file is moved out — so the live counter reads
                    // zero. Showing "0 B / 12 MiB (100%)" invites reading a
                    // completed download as a failed one.
                    let downloaded = if d.finished {
                        d.size.unwrap_or(d.downloaded)
                    } else {
                        d.downloaded
                    };
                    match d.size {
                        Some(s) => println!(
                            "  progress:   {} / {}{} ({})",
                            HumanBytes(downloaded),
                            approx,
                            HumanBytes(s),
                            pct
                        ),
                        // A finished download whose size was never advertised
                        // has nothing meaningful to put on either side of the
                        // slash; "0 B / unknown" would read as a failure.
                        None if d.finished => println!("  progress:   complete"),
                        None => println!("  progress:   {} / unknown", HumanBytes(downloaded)),
                    }
                    // Part counts are an artefact of the multipart engine;
                    // showing "1/1" for an engine that has no parts would
                    // invite reading it as a stalled download.
                    if caps.multipart {
                        println!("  parts:      {}/{}", d.parts_finished, d.parts_total);
                    }
                    println!("  resumable:  {}", d.is_resumable);
                    println!("  final file: {}", d.final_file_path.display());
                }
            }
        }
    }
    Ok(())
}
