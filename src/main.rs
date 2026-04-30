use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use clap::{CommandFactory, Parser};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use odl::{
    Download,
    config::{Config, ConfigBuilder},
    conflict::{
        FileChangedResolution, FinalFileExistsResolution, NotResumableResolution,
        SameDownloadExistsResolution, SaveConflictResolver, ServerConflictResolver,
    },
    credentials::Credentials,
    download_manager::DownloadManager,
    error::OdlError,
    progress::{
        AsyncReporter, DownloadContext, Phase, ProgressEvent, ProgressReporter, SAMPLE_INTERVAL,
    },
};
use reqwest::Url;
use tokio::{self, io::AsyncBufReadExt};
mod args;
use args::Args;
use tracing::instrument;

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
async fn main() -> Result<(), OdlError> {
    let args: Args = Args::parse();

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
                    return Ok(());
                }

                // apply command-specified settings into config
                if let Some(v) = download_dir {
                    cfg.download_dir = v.clone();
                }
                if let Some(v) = max_connections {
                    cfg.max_connections = *v;
                }
                if let Some(v) = max_concurrent_downloads {
                    cfg.max_concurrent_downloads = *v;
                }
                if let Some(v) = max_retries {
                    cfg.max_retries = *v;
                }
                if let Some(v) = n_fixed_retries {
                    cfg.n_fixed_retries = *v;
                }
                if let Some(v) = wait_between_retries {
                    cfg.wait_between_retries = *v;
                }
                if let Some(v) = speed_limit {
                    cfg.speed_limit = Some(*v);
                }
                if let Some(v) = user_agent {
                    cfg.user_agent = Some(v.clone());
                }
                if let Some(v) = randomize_user_agent {
                    cfg.randomize_user_agent = *v;
                }
                if let Some(v) = proxy {
                    cfg.proxy = Some(v.clone());
                }
                if let Some(v) = *timeout {
                    cfg.connect_timeout = Some(v);
                }
                if let Some(v) = use_server_time {
                    cfg.use_server_time = *v;
                }
                if let Some(v) = accept_invalid_certs {
                    cfg.accept_invalid_certs = *v;
                }

                match cfg.save_to_file(&config_path).await {
                    Ok(()) => println!("Saved configuration to {}", config_path.display()),
                    Err(e) => eprintln!("Failed to save configuration: {}", e),
                }
                return Ok(());
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
        let parent_metrics = BarMetrics::new();
        let parent_style = build_parent_style(&parent_metrics);

        let parent = mp.add(ProgressBar::new(0).with_style(parent_style));
        parent.set_message(format!("{url} (warming up)"));
        parent.enable_steady_tick(SAMPLE_INTERVAL);

        // Wrap the CliReporter in an async forwarder so every `emit`
        // hands the event off to a worker task via a lock-free mpsc and
        // returns immediately. Indicatif `set_position` / Mutex hops
        // never run on the download tasks themselves.
        let cli_reporter = CliReporter::new(Arc::clone(&mp), parent, parent_metrics);
        let reporter = AsyncReporter::spawn(cli_reporter);
        let ctx = DownloadContext::new()
            .with_reporter(reporter as Arc<dyn ProgressReporter>)
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
            let mut instruction = dlm
                .evaluate_with(url, save_dir, credentials, &resolver, &ctx)
                .await?;
            if let Some(filename) = user_provided_filename {
                instruction.set_filename(filename);
            }
            dlm.download_with(instruction, &resolver, &ctx).await
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
    for res in results {
        if let Err(e) = res {
            eprintln!("Error: {}", e);
            #[cfg(debug_assertions)]
            {
                eprintln!("{e:?}");
            }
        }
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

    let max_connections: u64 = args.max_connections.unwrap_or(cfg.max_connections);
    let max_concurrent_downloads: usize = args
        .max_concurrent_downloads
        .unwrap_or(cfg.max_concurrent_downloads);
    let max_retries: u32 = args.max_retries.unwrap_or(cfg.max_retries);
    let wait_between_retries: Duration = args
        .wait_between_retries
        .or(Some(cfg.wait_between_retries))
        .and_then(|d| {
            let secs = d.as_secs_f64();
            if secs.is_finite() && secs >= 0.0 {
                Some(d)
            } else {
                None
            }
        })
        .unwrap_or(Config::default_wait_between_retries());
    let n_fixed_retries = args.n_fixed_retries.unwrap_or(cfg.n_fixed_retries);
    let user_agent = args.user_agent.clone().or(cfg.user_agent);
    let randomize_user_agent = args
        .randomize_user_agent
        .unwrap_or(cfg.randomize_user_agent);
    let proxy_str = args.proxy.clone().or(cfg.proxy);
    // proxy_str remains an Option<String> and will be handed to the builder;
    // validation/parsing is performed by the ConfigBuilder validation step.
    let use_server_time = args.use_server_time.unwrap_or(cfg.use_server_time);
    let accept_invalid_certs = args
        .accept_invalid_certs
        .unwrap_or(cfg.accept_invalid_certs);
    let speed_limit = args.speed_limit.or(cfg.speed_limit);
    let connect_timeout = args.timeout.or(cfg.connect_timeout).and_then(|d| {
        let secs = d.as_secs_f64();
        if secs.is_finite() && secs >= 0.0 {
            Some(d)
        } else {
            None
        }
    });

    let mut builder = ConfigBuilder::default(); // Changed to ConfigBuilder
    builder
        .config_file(config_file)
        .connect_timeout(connect_timeout)
        .max_connections(max_connections)
        .max_concurrent_downloads(max_concurrent_downloads)
        .max_retries(max_retries)
        .n_fixed_retries(n_fixed_retries)
        .wait_between_retries(wait_between_retries)
        .user_agent(user_agent)
        .randomize_user_agent(randomize_user_agent)
        .proxy(proxy_str)
        .use_server_time(use_server_time)
        .accept_invalid_certs(accept_invalid_certs)
        .speed_limit(speed_limit);

    // always set download_dir from args or fallback to config
    let download_dir = args
        .download_dir
        .clone()
        .unwrap_or(cfg.download_dir.clone());
    builder.download_dir(download_dir);

    if !args.headers.is_empty() {
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
        builder.headers(Some(headers_map));
    }

    let cfg = builder.build()?; // Build the configuration
    Ok(DownloadManager::new(cfg)) // Create DownloadManager with the new configuration
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

    let instruction = dlm.evaluate(url, save_dir, None, &resolver).await?;

    let path = dlm.download(instruction, &resolver).await?;

    // `_permit` will be dropped here when going out of scope, releasing
    // the semaphore permit back to the manager.
    Ok(path)
}
