use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use clap::Parser;
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

#[tokio::main]
async fn main() -> Result<(), OdlError> {
    let args = Args::parse();

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
        .user_agent(args.user_agent)
        .randomize_user_agent(args.randomize_user_agent)
        .proxy(proxy)
        .use_server_time(args.use_server_time)
        .accept_invalid_certs(args.accept_invalid_certs);

    if let Some(download_dir) = args.temp_download_dir {
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

    let mut download_type = match Url::parse(&args.input) {
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
    };

    let dlm: DownloadManager = builder.build()?;

    if let DownloadType::FileAtUrl(url) = &download_type {
        let path = download_file_anyway(&dlm, url.clone()).await?;
        download_type = DownloadType::File(Box::new(path));
    }

    let resolver = CliResolver { force: args.force };

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
        let dlm = &dlm;
        let resolver = &resolver;
        let fut = dlm.evaluate_and_download_queued(url, None, resolver, resolver);
        futures.push(fut);
    }

    let results: Vec<Result<_, OdlError>> = join_all(futures).await;
    for res in results {
        if let Err(e) = res {
            eprintln!("Download failed: {}", e);
        }
    }

    Ok(())
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

async fn download_file_anyway(dlm: &DownloadManager, url: Url) -> Result<PathBuf, OdlError> {
    let resolver = ForcedResolver {};

    let instruction = dlm.evaluate(url, None, &resolver).await?;

    dlm.download(instruction, &resolver).await
}
