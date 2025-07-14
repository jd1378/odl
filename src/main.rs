use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use clap::Parser;
use indicatif::{ProgressState, ProgressStyle};
use odl::{
    Download,
    conflict::{
        FileChangedResolution, FinalFileExistsResolution, NotResumableResolution,
        SameDownloadExistsResolution, SaveConflictResolver, ServerConflictResolver,
    },
    download_manager::{DownloadManager, DownloadManagerBuilder},
    error::OdlError,
};
use reqwest::{Proxy, Url};
use tokio::{self, io::AsyncBufReadExt};
mod args;
use args::Args;
use futures::future::join_all;
use tracing::instrument;
use tracing_indicatif::IndicatifLayer;
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

pub const CHARS_FADE_IN: &'static str = "█▓▒░  ";
pub const TEMPLATE_BAR_WITH_POSITION: &'static str =
    "{bar:40.blue} {pos:>}/{len} ({percent}%) eta {eta_precise:.blue}";

fn elapsed_subsec(state: &ProgressState, writer: &mut dyn std::fmt::Write) {
    let seconds = state.elapsed().as_secs();
    let sub_seconds = (state.elapsed().as_millis() % 1000) / 100;
    let _ = writer.write_str(&format!("{}.{}s", seconds, sub_seconds));
}

#[tokio::main]
async fn main() -> Result<(), OdlError> {
    let args = Args::parse();
    let indicatif_layer = IndicatifLayer::new().with_progress_style(
        ProgressStyle::with_template(
            "{color_start}{span_child_prefix} {span_name} -- {bar:40.blue} {pos:>}/{len} ({percent}%) eta {eta_precise:.blue}{color_end}",
        )
        .unwrap().with_key(
            "elapsed_subsec",
            elapsed_subsec,
        ).with_key(
            "color_start",
            |state: &ProgressState, writer: &mut dyn std::fmt::Write| {
                if state.elapsed() > Duration::from_secs(6) && state.len().is_some_and(|l| l > 0) && state.pos() == 0 {
                    let _ = write!(writer, "\x1b[{}m", 3 + 30); 
                }  
            },
        ).with_key(
            "color_end",
            |_: &ProgressState, writer: &mut dyn std::fmt::Write| {
                let _ = write!(writer, "\x1b[0m");
            },
        )
    ).with_span_child_prefix_symbol("↳ ").with_span_child_prefix_indent(" ");
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

    let resolver = CliResolver { force: args.force };

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
    for url in urls.into_iter() { 
        let fut = dlm.evaluate_and_download_queued(url, None, &resolver, &resolver);
        futures.push(fut);
    }

    let results: Vec<Result<_, OdlError>> = join_all(futures).await;
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
    let proxy = if let Some(proxy_str) = &args.proxy {
        match Proxy::all(proxy_str) {
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

    let mut builder = DownloadManagerBuilder::default();
    builder
        .max_connections(args.max_connections)
        .max_concurrent_downloads(args.max_concurrent_downloads)
        .max_retries(args.retry)
        .wait_between_retries(Duration::from_secs_f32(args.waitretry))
        .user_agent(args.user_agent.clone())
        .randomize_user_agent(args.randomize_user_agent)
        .proxy(proxy)
        .use_server_time(args.use_server_time)
        .accept_invalid_certs(args.accept_invalid_certs);

    if let Some(download_dir) = args.temp_download_dir.clone() {
        builder.download_dir(download_dir);
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
    Ok(match Url::parse(&args.input) {
        Ok(url) => {
            if args.remote_list {
                DownloadType::FileAtUrl(url)
            } else {
                DownloadType::Url(url)
            }
        }
        Err(_) => {
            let path = PathBuf::from(&args.input);
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

    let instruction = dlm.evaluate(url, None, &resolver).await?;

    dlm.download(instruction, &resolver).await
}
