use derive_builder::Builder;
use reqwest::{
    Proxy,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::fs;
use tokio::sync::Semaphore;

#[rustfmt::skip]
mod defaults {
    use std::{path::PathBuf, time::Duration};
    use crate::{fs_utils};

    pub fn default_config_file() -> PathBuf { 
        let default_data_dir = fs_utils::get_odl_dir();
        default_data_dir.join("config.toml")
    }
    pub fn default_download_dir() -> PathBuf { fs_utils::get_odl_dir() }
    pub fn default_max_connections() -> u64 { 4 }
    pub fn default_max_concurrent_downloads() -> usize { 3 }
    pub fn default_max_retries() -> u64 { 3 }
    pub fn default_wait_between_retries() -> Duration { Duration::from_millis(700) }
    pub fn default_user_agent() -> Option<String> { None }
    pub fn default_randomize_user_agent() -> bool { false }
    pub fn default_proxy() -> Option<String> { None }
    pub fn default_use_server_time() -> bool { false }
    pub fn default_accept_invalid_certs() -> bool { false }
    pub fn default_speed_limit() -> Option<u64> { None }
    pub fn default_connect_timeout() -> Option<Duration> { Some(Duration::from_secs(5)) }
    pub fn default_headers() -> Option<indexmap::IndexMap<String, String>> { None }
}

use defaults::*;
/// Persistent download manager configuration. Fields are optional so that
/// unspecified values can fall back to code defaults.
#[derive(Builder, Debug, Clone, Serialize, Deserialize)]
#[builder(
    build_fn(validate = "Self::validate", private, name = "private_build"),
    default
)]
pub struct Config {
    /// Config file path for download manager to use.
    #[serde(default = "default_config_file", skip_serializing)]
    pub config_file: PathBuf,

    /// Where download manager keeps each download's parts and progress metadata
    #[serde(default = "default_download_dir")]
    pub download_dir: PathBuf,

    /// Max connections that download manager can make in parallel for a single file
    #[serde(default = "default_max_connections")]
    pub max_connections: u64,

    /// The maximum number of files that the download manager can download in parallel.
    ///
    /// This controls the overall concurrency of downloads. For example, if set to 4, up to 4 files
    /// will be downloaded at the same time, regardless of how many connections are used for each file.
    ///
    /// Note: For controlling how many parts of a single file can be downloaded concurrently,
    /// see the `max_connections` option.
    #[serde(default = "default_max_concurrent_downloads")]
    pub max_concurrent_downloads: usize,

    /// Number of maximum retries after which a download is considered failed. After third retry it increases exponentially.
    /// For example the time for max_retries=6 and wait_between_retries=500ms will be:
    /// 500ms, 500ms, 500ms, 1000ms, 2000ms, 4000ms
    #[serde(default = "default_max_retries")]
    pub max_retries: u64,

    /// Amount of time to wait between retries. After third retry it increases exponentially.
    #[serde(default = "default_wait_between_retries")]
    pub wait_between_retries: Duration,

    /// Custom user agent. Setting this option overrides `randomize_user_agent` to false
    #[serde(default = "default_user_agent")]
    pub user_agent: Option<String>,

    /// Randomize user agent for each request.
    #[serde(default = "default_randomize_user_agent")]
    pub randomize_user_agent: bool,

    /// Custom request Proxy to use for downloads (proxy URL string)
    #[serde(default = "default_proxy")]
    pub proxy: Option<String>,

    /// Whether to use the last-modified sent by server when saving the file
    #[serde(default = "default_use_server_time")]
    pub use_server_time: bool,

    /// Should we accept invalid SSL certificates? Do not use unless you are absolutely sure of what you are doing.
    #[serde(default = "default_accept_invalid_certs")]
    pub accept_invalid_certs: bool,

    /// Optional maximum aggregate download speed per download in bytes per second.
    #[serde(default = "default_speed_limit")]
    pub speed_limit: Option<u64>,

    /// Connect timeout for requests. Defaults to 500 ms.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: Option<Duration>,

    /// Optional custom headers to add to each request. Keys and values are strings.
    ///
    /// Example in `config.toml`:
    ///
    /// [headers]
    /// Authorization = "Bearer TOKEN"
    /// Accept = "application/json"
    #[serde(default = "default_headers")]
    pub headers: Option<indexmap::IndexMap<String, String>>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            config_file: default_config_file(),
            download_dir: default_download_dir(),
            max_connections: default_max_connections(),
            max_concurrent_downloads: default_max_concurrent_downloads(),
            max_retries: default_max_retries(),
            wait_between_retries: default_wait_between_retries(),
            user_agent: default_user_agent(),
            randomize_user_agent: default_randomize_user_agent(),
            proxy: default_proxy(),
            use_server_time: default_use_server_time(),
            accept_invalid_certs: default_accept_invalid_certs(),
            speed_limit: default_speed_limit(),
            connect_timeout: default_connect_timeout(),
            headers: default_headers(),
        }
    }
}

impl ConfigBuilder {
    fn validate(&self) -> Result<(), ConfigBuilderError> {
        let max_concurrent = self
            .max_concurrent_downloads
            .unwrap_or(default_max_concurrent_downloads());
        if max_concurrent == 0 || max_concurrent >= Semaphore::MAX_PERMITS {
            return Err(ConfigBuilderError::UninitializedField(
                "max_concurrent_downloads",
            ));
        }
        if let Some(max_connections) = self.max_connections {
            if max_connections == 0 {
                return Err(ConfigBuilderError::UninitializedField("max_connections"));
            }
        }
        if let Some(wait_between_retries) = self.wait_between_retries {
            if wait_between_retries == Duration::from_millis(0) {
                return Err(ConfigBuilderError::UninitializedField(
                    "wait_between_retries",
                ));
            }
        }
        if let Some(Some(limit)) = self.speed_limit {
            if limit == 0 {
                return Err(ConfigBuilderError::UninitializedField("speed_limit"));
            }
        }
        if let Some(Some(timeout)) = self.connect_timeout {
            if timeout == Duration::from_millis(0) {
                return Err(ConfigBuilderError::UninitializedField("request_timeout"));
            }
        }
        if let Some(Some(proxy)) = self.proxy.as_ref() {
            if Proxy::all(proxy).is_err() {
                return Err(ConfigBuilderError::ValidationError("proxy".to_owned()));
            }
        }

        if let Some(Some(headers)) = self.headers.as_ref() {
            for (k, v) in headers.iter() {
                if HeaderName::from_bytes(k.as_bytes()).is_err() {
                    tracing::warn!(
                        "Invalid header name in provided config. It will be ignored. Name={:?}",
                        k
                    );
                    continue;
                } else if HeaderValue::from_str(v).is_err() {
                    tracing::warn!(
                        "Invalid header value in provided config. It will be ignored. Header: {}={:?}",
                        k,
                        v
                    );
                    continue;
                }
            }
        }

        Ok(())
    }

    pub fn build(&self) -> Result<Config, ConfigBuilderError> {
        let result = self.private_build()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn headers_preserve_order_on_parse() {
        let s = r#"
max_connections = 1

[headers]
Z-Header = "z"
A-Header = "a"
M-Header = "m"
"#;

        let cfg: Config = toml::from_str(s).expect("parse");
        let headers = cfg.headers.expect("headers");
        let keys: Vec<&str> = headers.keys().map(|k| k.as_str()).collect();
        // TOML parsing order may vary; ensure we have the expected header names regardless of order.
        let keys_set: std::collections::HashSet<&str> = keys.into_iter().collect();
        let expected: std::collections::HashSet<&str> = vec!["Z-Header", "A-Header", "M-Header"]
            .into_iter()
            .collect();
        assert_eq!(keys_set, expected);
    }
}

impl Config {
    pub fn default_config_file() -> PathBuf {
        return default_config_file();
    }

    pub fn default_wait_between_retries() -> Duration {
        return default_wait_between_retries();
    }

    /// Path to the config file inside the provided download dir.
    pub fn config_path_for_dir<P: AsRef<Path>>(data_dir: P) -> PathBuf {
        let mut p = data_dir.as_ref().to_path_buf();
        p.push("config.toml");
        p
    }

    /// Load configuration from the given directory's `config.toml`.
    /// If file does not exist, returns Ok(Default::default()).
    pub async fn load_from_file<P: AsRef<Path>>(cfg_path: P) -> Result<Config, io::Error> {
        let path = cfg_path.as_ref().to_path_buf();
        if tokio::fs::metadata(&path).await.is_err() {
            return Ok(Config::default());
        }
        let s = fs::read_to_string(&path).await?;
        let cfg: Config =
            toml::from_str(&s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(cfg)
    }

    /// Save configuration to `data_dir/config.toml`. Creates parent dir if needed.
    pub async fn save_to_file<P: AsRef<Path>>(&self, cfg_path: P) -> Result<(), io::Error> {
        let pathbuf = cfg_path.as_ref().to_path_buf();
        if let Some(p) = pathbuf.parent() {
            fs::create_dir_all(p).await?;
        }
        let s =
            toml::to_string_pretty(&self).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(pathbuf, s).await?;
        Ok(())
    }
}

impl From<&Config> for HeaderMap {
    fn from(cfg: &Config) -> Self {
        let mut map = HeaderMap::new();

        if let Some(headers) = &cfg.headers {
            for (k, v) in headers.iter() {
                // ignore invalid header names/values instead of returning an error
                if let Ok(name) = HeaderName::from_bytes(k.as_bytes()) {
                    if let Ok(value) = HeaderValue::from_str(v) {
                        map.insert(name, value);
                    }
                }
            }
        }

        map
    }
}

impl From<&Config> for Option<Proxy> {
    fn from(cfg: &Config) -> Self {
        cfg.proxy
            .as_deref()
            .and_then(|s| reqwest::Proxy::all(s).ok())
    }
}
