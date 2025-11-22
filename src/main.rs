use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use clap::{CommandFactory, Parser};
use indicatif::{ProgressState, ProgressStyle};
use odl::{
    Download,
    conflict::{
        FileChangedResolution, FinalFileExistsResolution, NotResumableResolution,
        SameDownloadExistsResolution, SaveConflictResolver, ServerConflictResolver,
    },
    credentials::Credentials,
    download_manager::{DownloadManager, DownloadManagerBuilder},
    error::OdlError,
};
use reqwest::{Proxy, Url};
use tokio::{self, io::AsyncBufReadExt};
mod args;
use args::Args;
mod config;
use config::Config;
use futures::future::join_all;
use tracing::{Instrument, info_span, instrument};
use tracing_indicatif::{IndicatifLayer, TickSettings, span_ext::IndicatifSpanExt};
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadType {
    Url(Url),
    File(Box<PathBuf>),
    FileAtUrl(Url),
}

struct CliResolver {
    force: bool,
}

#[async_trait]
impl ServerConflictResolver for CliResolver {
    async fn resolve_file_changed(&self, _: &Download) -> FileChangedResolution {
        if self.force {
            FileChangedResolution::Restart
        } else {
            FileChangedResolution::Abort
        }
    }
    async fn resolve_not_resumable(&self, _: &Download) -> NotResumableResolution {
        if self.force {
            NotResumableResolution::Restart
        } else {
            NotResumableResolution::Abort
        }
    }
}

#[async_trait]
impl SaveConflictResolver for CliResolver {
    async fn same_download_exists(&self, _: &Download) -> SameDownloadExistsResolution {
        SameDownloadExistsResolution::Resume
    }
    async fn final_file_exists(&self, _: &Download) -> FinalFileExistsResolution {
        if self.force {
            FinalFileExistsResolution::ReplaceAndContinue
        } else {
            FinalFileExistsResolution::Abort
        }
    }
}

pub const PROGRESS_CHARS: &'static str = "█▇▆▅▄▃▂▁";

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
                data_dir,
                max_connections,
                max_concurrent_downloads,
                max_retries,
                wait_between_retries,
                speed_limit,
                user_agent,
                randomize_user_agent,
                proxy,
                timeout,
                use_server_time,
                accept_invalid_certs,
            } => {
                // determine directory where config is stored
                let target_dir = if let Some(d) = data_dir {
                    d.clone()
                } else if let Some(d) = &args.data_dir {
                    d.clone()
                } else {
                    dirs::data_dir()
                        .map(|mut p| {
                            p.push("odl");
                            p
                        })
                        .unwrap_or_else(|| {
                            let tmp = PathBuf::from("/tmp/odl");
                            std::fs::create_dir_all(&tmp).ok();
                            tmp
                        })
                };

                let mut cfg = match Config::load_from_dir(&target_dir) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("Failed to load existing config: {}", e);
                        Config::default()
                    }
                };

                if *show {
                    println!(
                        "# config path: {}",
                        Config::config_path_for_dir(&target_dir).display()
                    );
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
                if let Some(v) = max_connections {
                    cfg.max_connections = Some(*v);
                }
                if let Some(v) = max_concurrent_downloads {
                    cfg.max_concurrent_downloads = Some(*v);
                }
                if let Some(v) = max_retries {
                    cfg.max_retries = Some(*v);
                }
                if let Some(v) = wait_between_retries {
                    cfg.wait_between_retries_secs = Some(*v);
                }
                if let Some(v) = speed_limit {
                    cfg.speed_limit = Some(*v);
                }
                if let Some(v) = user_agent {
                    cfg.user_agent = Some(v.clone());
                }
                if let Some(v) = randomize_user_agent {
                    cfg.randomize_user_agent = Some(*v);
                }
                if let Some(v) = proxy {
                    cfg.proxy = Some(v.clone());
                }
                if let Some(v) = *timeout {
                    cfg.connect_timeout_secs = Some(v.as_secs_f64());
                }
                if let Some(v) = use_server_time {
                    cfg.use_server_time = Some(*v);
                }
                if let Some(v) = accept_invalid_certs {
                    cfg.accept_invalid_certs = Some(*v);
                }

                match cfg.save_to_dir(&target_dir) {
                    Ok(()) => println!(
                        "Saved configuration to {}/config.toml",
                        target_dir.display()
                    ),
                    Err(e) => eprintln!("Failed to save configuration: {}", e),
                }
                return Ok(());
            }
        }
    }
    let child_style = ProgressStyle::with_template(
            "{span_child_prefix}{spinner} {bar:40.cyan/blue} {percent:>3}%  {decimal_bytes:<10} / {decimal_total_bytes:<10} {decimal_bytes_per_sec:<12}"
        )
        .expect("templating progress bar should not fail").progress_chars(PROGRESS_CHARS);
    let indicatif_layer = IndicatifLayer::new()
        .with_progress_style(child_style)
        .with_tick_settings(TickSettings {
            term_draw_hz: 10,
            default_tick_interval: None,
            footer_tick_interval: None,
            ..Default::default()
        })
        .with_span_child_prefix_symbol("↳ ")
        .with_span_child_prefix_indent("  ");
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(indicatif_layer.get_stderr_writer())
                .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
        )
        .with(indicatif_layer)
        .init();

    let dlm = build_download_manager(&args)?;

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

    let mut futures = Vec::new();
    let parent_style = ProgressStyle::with_template(
        "{spinner} {maybe_connect} {msg:!40}   {percent:>3}%  {decimal_bytes:<10} / {decimal_total_bytes:<10} {decimal_bytes_per_sec:<12} eta {eta_precise} elapsed {elapsed}",
    )
    .expect("templating progress bar should not fail").with_key("maybe_connect", |state: &ProgressState, writer: &mut dyn std::fmt::Write| {
            if state.len().is_none() || state.len().is_some_and(|x| x == 0) {
                let _ = write!(writer, "━");
            } else {
                let _ = write!(writer, "┌");
            }
        });

    let dlm = Arc::new(dlm);
    let credentials = if let Some(user) = args.http_user.as_deref() {
        Some(Credentials::new(user, args.http_password.as_deref()))
    } else {
        None
    };

    for url in urls.into_iter() {
        let download_span = info_span!("download", url = %url);
        download_span.pb_set_style(&parent_style);
        download_span.pb_set_message("Warming up");
        download_span.pb_start();
        let dlm = Arc::clone(&dlm);
        let resolver = CliResolver { force: args.force };
        let save_dir = save_dir.clone();
        let user_provided_filename = user_provided_filename.clone();
        let credentials = credentials.clone();

        futures.push(
            async move {
                let permit = dlm
                    .acquire_download_permit()
                    .await
                    .expect("didn't expect the semaphore to close at this point");
                let mut instruction = dlm.evaluate(url, save_dir, credentials, &resolver).await?;
                if let Some(filename) = user_provided_filename {
                    instruction.set_filename(filename);
                }
                let result = dlm.download(instruction, &resolver).await;
                drop(permit);
                result
            }
            .instrument(download_span),
        );
    }

    let results: Vec<Result<PathBuf, OdlError>> = join_all(futures).await;
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
fn build_download_manager(args: &Args) -> Result<DownloadManager, OdlError> {
    // determine where config would live (same logic used by download manager default)
    let config_dir = if let Some(d) = &args.data_dir {
        d.clone()
    } else {
        dirs::data_dir()
            .map(|mut p| {
                p.push("odl");
                p
            })
            .unwrap_or_else(|| {
                let tmp = PathBuf::from("/tmp/odl");
                std::fs::create_dir_all(&tmp).ok();
                tmp
            })
    };

    let cfg = match Config::load_from_dir(&config_dir) {
        Ok(c) => c,
        Err(_) => Config::default(),
    };

    // helper: choose value preference: CLI arg (if Some) else config (if Some) else fallback default
    let max_connections = args.max_connections.or(cfg.max_connections).unwrap_or(4u64);
    let max_concurrent_downloads = args
        .max_concurrent_downloads
        .or(cfg.max_concurrent_downloads)
        .unwrap_or(3usize);
    let max_retries = args.retry.or(cfg.max_retries).unwrap_or(10u64);
    let wait_between_secs = args
        .waitretry
        .map(|f| f as f64)
        .or(cfg.wait_between_retries_secs)
        .unwrap_or(0.3f64);
    let user_agent = args.user_agent.clone().or(cfg.user_agent);
    let randomize_user_agent = args
        .randomize_user_agent
        .or(cfg.randomize_user_agent)
        .unwrap_or(true);
    let proxy_str = args.proxy.clone().or(cfg.proxy);
    let proxy = if let Some(proxy_str) = proxy_str {
        match Proxy::all(&proxy_str) {
            Ok(p) => Some(p),
            Err(e) => {
                return Err(OdlError::CliError {
                    message: format!("Failed to parse proxy '{}': {}", proxy_str, e),
                });
            }
        }
    } else {
        None
    };
    let use_server_time = args
        .use_server_time
        .or(cfg.use_server_time)
        .unwrap_or(false);
    let accept_invalid_certs = args
        .accept_invalid_certs
        .or(cfg.accept_invalid_certs)
        .unwrap_or(false);
    let speed_limit = args.speed_limit.or(cfg.speed_limit);

    let mut builder = DownloadManagerBuilder::default();
    builder
        .max_connections(max_connections)
        .max_concurrent_downloads(max_concurrent_downloads)
        .max_retries(max_retries)
        .wait_between_retries(Duration::from_secs_f64(wait_between_secs))
        .user_agent(user_agent)
        .randomize_user_agent(randomize_user_agent)
        .proxy(proxy)
        .use_server_time(use_server_time)
        .accept_invalid_certs(accept_invalid_certs)
        .speed_limit(speed_limit);

    // connect timeout preference: CLI timeout (if set) else config value
    if let Some(secs) = args
        .timeout
        .map(|d| d.as_secs_f64())
        .or(cfg.connect_timeout_secs)
    {
        builder.connect_timeout(Some(Duration::from_secs_f64(secs)));
    }

    if let Some(data_dir) = args.data_dir.clone().or(cfg.data_dir) {
        builder.data_dir(data_dir);
    }

    if !args.headers.is_empty() {
        let mut headers_map = reqwest::header::HeaderMap::new();
        for header in &args.headers {
            if let Some((key, value)) = header.split_once(':') {
                let key = key.trim();
                let value = value.trim();
                if let Ok(header_name) = reqwest::header::HeaderName::from_bytes(key.as_bytes()) {
                    if let Ok(header_value) = reqwest::header::HeaderValue::from_str(value) {
                        headers_map.insert(header_name, header_value);
                    } else {
                        return Err(OdlError::CliError {
                            message: format!("Invalid header value for '{}': '{}'", key, value),
                        });
                    }
                } else {
                    return Err(OdlError::CliError {
                        message: format!("Invalid header name: '{}'", key),
                    });
                }
            } else {
                return Err(OdlError::CliError {
                    message: format!("Header must be in KEY:VALUE format: '{}'", header),
                });
            }
        }
        builder.headers(Some(headers_map));
    }

    Ok(builder.build()?)
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

    let instruction = dlm.evaluate(url, save_dir, None, &resolver).await?;

    dlm.download(instruction, &resolver).await
}
