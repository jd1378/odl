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
    error::OdlError,
    progress::{
        AsyncReporter, DownloadContext, Phase, ProgressEvent, ProgressReporter, SAMPLE_INTERVAL,
    },
};
use reqwest::Url;
use serde_json::json;
use std::process::ExitCode;
use tokio::{self, io::AsyncBufReadExt};
mod args;
mod json;
use args::{Args, LogLevel, OutputFormat};
use json::JsonReporter;
use odl::download_manager::DownloadStatus;
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
        OdlError::Network(_) => 3,
        OdlError::Conflict(_) => 4,
        OdlError::StdIoError { .. } => 5,
        OdlError::MetadataError(_) => 6,
        OdlError::Cancelled => 130,
        OdlError::Other { .. } => 1,
    }
}

/// Stable machine-readable error category string (pairs with [`exit_code`]).
fn error_kind(e: &OdlError) -> &'static str {
    match e {
        OdlError::CliError { .. } => "cli",
        OdlError::EmptyInputFile => "empty_input_file",
        OdlError::UrlDecodeError { .. } => "url_decode",
        OdlError::ConfigBuilderError(_) | OdlError::DownloadOptionsBuilderError(_) => "config",
        OdlError::Network(_) => "network",
        OdlError::Conflict(_) => "conflict",
        OdlError::StdIoError { .. } => "io",
        OdlError::MetadataError(_) => "metadata",
        OdlError::Cancelled => "cancelled",
        OdlError::Other { .. } => "other",
    }
}

/// Emit a top-level error in the selected format and return its exit code.
fn report_error(e: &OdlError, format: OutputFormat) -> ExitCode {
    match format {
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
}

impl CliReporter {
    fn new(mp: Arc<MultiProgress>, parent: ProgressBar, parent_metrics: BarMetrics) -> Self {
        Self {
            mp,
            parent,
            parts: Mutex::new(HashMap::new()),
            parent_metrics,
            filename: Mutex::new(None),
        }
    }
}

fn phase_label(phase: Phase) -> &'static str {
    match phase {
        Phase::Evaluating => "Evaluating",
        Phase::ResolvingConflicts => "Resolving conflicts",
        Phase::Downloading => "Downloading",
        Phase::Assembling => "Assembling",
        Phase::Flushing => "Flushing data to disk",
        Phase::Verifying => "Verifying checksum",
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
            }
            ProgressEvent::Speed { bytes_per_second } => {
                self.parent_metrics
                    .speed_bits
                    .store(bytes_per_second.to_bits(), Ordering::Relaxed);
            }
            ProgressEvent::PartAdded { ulid, size, .. } => {
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
                {
                    let mut parts = self.parts.lock().unwrap();
                    for (_, p) in parts.drain() {
                        p.bar.finish_and_clear();
                    }
                }
                self.parent.abandon_with_message("Cancelled");
            }
            ProgressEvent::Failed { message } => {
                {
                    let mut parts = self.parts.lock().unwrap();
                    for (_, p) in parts.drain() {
                        p.bar.finish_and_clear();
                    }
                }
                self.parent
                    .abandon_with_message(format!("Failed: {message}"));
            }
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
    let save_dir: PathBuf = if let Some(path) = args.output {
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

    for url in urls.into_iter() {
        let reporter: Arc<dyn ProgressReporter> = match format {
            // NDJSON events go straight to stdout. Progress fires at the
            // sampler cadence (~8 Hz), so printing on the download task is
            // cheap and needs no async-forwarder buffering.
            OutputFormat::Json => Arc::new(JsonReporter::new(url.to_string())),
            OutputFormat::Text => {
                let parent_metrics = BarMetrics::new();
                let parent_style = build_parent_style(&parent_metrics);

                let parent = mp.add(ProgressBar::new(0).with_style(parent_style));
                parent.set_message(format!("{url} (warming up)"));
                parent.enable_steady_tick(SAMPLE_INTERVAL);

                // Wrap the CliReporter in an async forwarder so every `emit`
                // hands the event off to a worker task via a lock-free mpsc
                // and returns immediately. Indicatif `set_position` / Mutex
                // hops never run on the download tasks themselves.
                let cli_reporter = CliReporter::new(Arc::clone(&mp), parent, parent_metrics);
                AsyncReporter::spawn(cli_reporter) as Arc<dyn ProgressReporter>
            }
        };
        let ctx = DownloadContext::new()
            .with_reporter(reporter)
            .with_url(url.clone());

        let dlm = Arc::clone(&dlm);
        let save_dir = save_dir.clone();
        let user_provided_filename = user_provided_filename.clone();
        let credentials = credentials.clone();
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
                let mut instruction = dlm
                    .evaluate(EvaluateRequest {
                        url,
                        save_dir,
                        conflict_resolver: &resolver,
                        credentials,
                        ctx: Some(&ctx),
                        options: None,
                    })
                    .await?;
                if let Some(filename) = user_provided_filename {
                    instruction.set_filename(filename);
                }
                dlm.download(DownloadRequest {
                    instruction,
                    conflict_resolver: &resolver,
                    ctx: Some(&ctx),
                    options: None,
                })
                .await
            }
            .await;
            if let Err(ref e) = result {
                ctx.emit(ProgressEvent::Failed {
                    message: e.to_string(),
                });
            }
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
        .evaluate(EvaluateRequest::new(url, save_dir, &resolver))
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

    match format {
        OutputFormat::Json => {
            let v = json!({
                "type": "probe",
                "url": instruction.url().to_string(),
                "filename": instruction.filename(),
                "size": instruction.size(),
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
            match instruction.size() {
                Some(s) => println!("size:          {} ({} bytes)", HumanBytes(s), s),
                None => println!("size:          unknown"),
            }
            println!("resumable:     {}", instruction.is_resumable());
            println!(
                "etag:          {}",
                instruction.etag().as_deref().unwrap_or("-")
            );
            println!(
                "last_modified: {}",
                last_modified_rfc3339.as_deref().unwrap_or("-")
            );
            println!("requires_auth: {}", instruction.requires_auth());
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
                    println!("{}", d.filename);
                    println!("  url:        {}", d.url);
                    println!("  state:      {}", state);
                    match d.size {
                        Some(s) => println!(
                            "  progress:   {} / {} ({})",
                            HumanBytes(d.downloaded),
                            HumanBytes(s),
                            pct
                        ),
                        None => println!("  progress:   {} / unknown", HumanBytes(d.downloaded)),
                    }
                    println!("  parts:      {}/{}", d.parts_finished, d.parts_total);
                    println!("  resumable:  {}", d.is_resumable);
                    println!("  final file: {}", d.final_file_path.display());
                }
            }
        }
    }
    Ok(())
}
