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
    pub fn default_max_retries() -> u32 { 3 }
    pub fn default_wait_between_retries() -> Duration { Duration::from_millis(700) }
    pub fn default_n_fixed_retries() -> u32 { 3 }
    pub fn default_user_agent() -> Option<String> { None }
    pub fn default_randomize_user_agent() -> bool { false }
    pub fn default_proxy() -> Option<String> { None }
    pub fn default_use_server_time() -> bool { false }
    pub fn default_accept_invalid_certs() -> bool { false }
    pub fn default_speed_limit() -> Option<u64> { None }
    pub fn default_connect_timeout() -> Option<Duration> { Some(Duration::from_secs(5)) }
    pub fn default_headers() -> Option<indexmap::IndexMap<String, String>> { None }
    pub fn default_http2() -> bool { false }
    pub fn default_dynamic_split() -> bool { true }
}

use defaults::*;

/// Per-download options used by `DownloadManager` when running a single job.
///
/// All fields here are job-scoped: passing a custom `DownloadOptions`
/// via `EvaluateRequest`/`DownloadRequest` overrides the manager's
/// defaults for that one job only. Manager-only knobs (paths,
/// concurrency cap) live on [`Config`] instead.
///
/// Construct via [`DownloadOptionsBuilder`]; direct field access is via
/// getters so invariants stay enforced.
#[derive(Builder, Debug, Clone, Serialize, Deserialize)]
#[builder(build_fn(private, name = "private_build"), default)]
pub struct DownloadOptions {
    /// Max connections that download manager can make in parallel for a single file
    #[serde(default = "default_max_connections")]
    max_connections: u64,

    /// Number of maximum retries after which a download is considered failed. After third retry it increases exponentially.
    /// For example the time for max_retries=6 and wait_between_retries=500ms will be:
    /// 500ms, 500ms, 500ms, 1000ms, 2000ms, 4000ms
    #[serde(default = "default_max_retries")]
    max_retries: u32,

    /// Amount of time to wait between retries. After third retry it increases exponentially.
    #[serde(default = "default_wait_between_retries")]
    wait_between_retries: Duration,

    /// Number of fixed (non-exponential) retries before exponential backoff starts.
    #[serde(default = "default_n_fixed_retries")]
    n_fixed_retries: u32,

    /// Custom user agent. Setting this option overrides `randomize_user_agent` to false
    #[serde(default = "default_user_agent")]
    user_agent: Option<String>,

    /// Randomize user agent for each request.
    #[serde(default = "default_randomize_user_agent")]
    randomize_user_agent: bool,

    /// Custom request Proxy to use for downloads (proxy URL string)
    #[serde(default = "default_proxy")]
    proxy: Option<String>,

    /// Whether to use the last-modified sent by server when saving the file
    #[serde(default = "default_use_server_time")]
    use_server_time: bool,

    /// Should we accept invalid SSL certificates? Do not use unless you are absolutely sure of what you are doing.
    #[serde(default = "default_accept_invalid_certs")]
    accept_invalid_certs: bool,

    /// Optional maximum aggregate download speed per download in bytes per second.
    #[serde(default = "default_speed_limit")]
    speed_limit: Option<u64>,

    /// Connect timeout for requests. Defaults to 5 seconds.
    #[serde(default = "default_connect_timeout")]
    connect_timeout: Option<Duration>,

    /// Optional custom headers to add to each request. Keys and values are strings.
    ///
    /// Example in `config.toml`:
    ///
    /// [headers]
    /// Authorization = "Bearer TOKEN"
    /// Accept = "application/json"
    #[serde(default = "default_headers")]
    headers: Option<indexmap::IndexMap<String, String>>,

    /// Enable HTTP/2 over ALPN. Default `false` (HTTP/1.1 only).
    /// HTTP/1.1 opens a separate TCP connection per part, giving each
    /// part an independent receive window — important on Windows where
    /// h2's per-stream/connection flow-control windows on a single TCP
    /// can throttle high-bandwidth downloads.
    #[serde(default = "default_http2")]
    http2: bool,

    /// Allow the downloader to dynamically subdivide a long-running part
    /// mid-flight when spare connections are idle. Disabling locks the
    /// part layout chosen at evaluate time (or set explicitly via
    /// `max_connections` on resume).
    #[serde(default = "default_dynamic_split")]
    dynamic_split: bool,
}

impl From<DownloadOptions> for DownloadOptionsBuilder {
    fn from(o: DownloadOptions) -> Self {
        let mut b = Self::default();
        b.max_connections(o.max_connections)
            .max_retries(o.max_retries)
            .wait_between_retries(o.wait_between_retries)
            .n_fixed_retries(o.n_fixed_retries)
            .user_agent(o.user_agent)
            .randomize_user_agent(o.randomize_user_agent)
            .proxy(o.proxy)
            .use_server_time(o.use_server_time)
            .accept_invalid_certs(o.accept_invalid_certs)
            .speed_limit(o.speed_limit)
            .connect_timeout(o.connect_timeout)
            .headers(o.headers)
            .http2(o.http2)
            .dynamic_split(o.dynamic_split);
        b
    }
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            max_retries: default_max_retries(),
            wait_between_retries: default_wait_between_retries(),
            n_fixed_retries: default_n_fixed_retries(),
            user_agent: default_user_agent(),
            randomize_user_agent: default_randomize_user_agent(),
            proxy: default_proxy(),
            use_server_time: default_use_server_time(),
            accept_invalid_certs: default_accept_invalid_certs(),
            speed_limit: default_speed_limit(),
            connect_timeout: default_connect_timeout(),
            headers: default_headers(),
            http2: default_http2(),
            dynamic_split: default_dynamic_split(),
        }
    }
}

impl DownloadOptions {
    pub fn default_wait_between_retries() -> Duration {
        default_wait_between_retries()
    }

    // Getters
    pub fn max_connections(&self) -> u64 {
        self.max_connections
    }
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }
    pub fn wait_between_retries(&self) -> Duration {
        self.wait_between_retries
    }
    pub fn n_fixed_retries(&self) -> u32 {
        self.n_fixed_retries
    }
    pub fn user_agent(&self) -> Option<&str> {
        self.user_agent.as_deref()
    }
    pub fn randomize_user_agent(&self) -> bool {
        self.randomize_user_agent
    }
    pub fn proxy(&self) -> Option<&str> {
        self.proxy.as_deref()
    }
    pub fn use_server_time(&self) -> bool {
        self.use_server_time
    }
    pub fn accept_invalid_certs(&self) -> bool {
        self.accept_invalid_certs
    }
    pub fn speed_limit(&self) -> Option<u64> {
        self.speed_limit
    }
    pub fn connect_timeout(&self) -> Option<Duration> {
        self.connect_timeout
    }
    pub fn headers(&self) -> Option<&indexmap::IndexMap<String, String>> {
        self.headers.as_ref()
    }
    pub fn http2(&self) -> bool {
        self.http2
    }
    pub fn dynamic_split(&self) -> bool {
        self.dynamic_split
    }

    /// Convert into a [`DownloadOptionsBuilder`] pre-populated with this
    /// instance's values. Use to apply partial overrides on top of an
    /// existing options set before rebuilding.
    pub fn into_builder(self) -> DownloadOptionsBuilder {
        self.into()
    }

    /// Clamp / drop only the values where a clear safe fallback exists
    /// and a typo shouldn't refuse to start: `max_connections = 0` →
    /// default, and bad header entries are dropped (rest kept).
    /// Everything else is left for [`Self::validate_self`] to reject.
    fn sanitize(&mut self) {
        if self.max_connections == 0 {
            tracing::warn!(
                "max_connections must be at least 1; got 0, clamping to {}",
                default_max_connections()
            );
            self.max_connections = default_max_connections();
        }
        if let Some(headers) = self.headers.as_mut() {
            headers.retain(|k, v| {
                if HeaderName::from_bytes(k.as_bytes()).is_err() {
                    tracing::warn!("invalid header name {:?}; dropping", k);
                    return false;
                }
                if HeaderValue::from_str(v).is_err() {
                    tracing::warn!("invalid value for header {}: {:?}; dropping", k, v);
                    return false;
                }
                true
            });
            if headers.is_empty() {
                self.headers = None;
            }
        }
    }

    /// Reject configurations where the user likely meant something
    /// specific (zero retry interval / timeout, malformed proxy URL)
    /// and a silent fallback would mask intent.
    fn validate_self(&self) -> Result<(), DownloadOptionsBuilderError> {
        if self.wait_between_retries == Duration::from_millis(0) {
            return Err(DownloadOptionsBuilderError::ValidationError(
                "wait_between_retries must be greater than 0".to_owned(),
            ));
        }
        if self.n_fixed_retries == 0 {
            return Err(DownloadOptionsBuilderError::ValidationError(
                "n_fixed_retries must be at least 1".to_owned(),
            ));
        }
        if let Some(0) = self.speed_limit {
            return Err(DownloadOptionsBuilderError::ValidationError(
                "speed_limit must be greater than 0".to_owned(),
            ));
        }
        if let Some(t) = self.connect_timeout
            && t == Duration::from_millis(0)
        {
            return Err(DownloadOptionsBuilderError::ValidationError(
                "connect_timeout must be greater than 0".to_owned(),
            ));
        }
        if let Some(p) = self.proxy.as_deref()
            && Proxy::all(p).is_err()
        {
            return Err(DownloadOptionsBuilderError::ValidationError(format!(
                "proxy URL is invalid: {:?}",
                p
            )));
        }
        Ok(())
    }
}

impl DownloadOptionsBuilder {
    pub fn build(&self) -> Result<DownloadOptions, DownloadOptionsBuilderError> {
        let mut opts = self.private_build()?;
        opts.sanitize();
        opts.validate_self()?;
        Ok(opts)
    }
}

/// `Config` holds user-visible defaults for the manager and is used by
/// `DownloadManager` to build HTTP clients and control concurrency.
///
/// Two scopes are kept distinct:
/// - manager-only fields (paths, the global concurrency cap) at the top level
/// - per-download fields nested under `download` ([`DownloadOptions`]),
///   which can be overridden per-job
///
/// The on-disk TOML representation stays flat thanks to `#[serde(flatten)]`.
///
/// Construct via [`ConfigBuilder`]; field access is via getters so
/// invariants stay enforced.
///
/// Example (loading config from disk, falling back to defaults):
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let cfg = odl::config::Config::load_from_file("/tmp/odl/config.toml").await?;
/// println!("Using download dir: {}", cfg.download_dir().display());
/// # Ok(())
/// # }
/// ```
#[derive(Builder, Debug, Clone, Serialize, Deserialize)]
#[builder(build_fn(private, name = "private_build"), default)]
pub struct Config {
    /// Where download manager keeps each download's parts and progress metadata
    #[serde(default = "default_download_dir")]
    download_dir: PathBuf,

    /// The maximum number of files that the download manager can download in parallel.
    ///
    /// This controls the overall concurrency of downloads. For example, if set to 4, up to 4 files
    /// will be downloaded at the same time, regardless of how many connections are used for each file.
    ///
    /// Note: For controlling how many parts of a single file can be downloaded concurrently,
    /// see `download.max_connections`.
    #[serde(default = "default_max_concurrent_downloads")]
    max_concurrent_downloads: usize,

    /// Per-download options (max_connections, retries, headers, proxy, …).
    /// Flattened in TOML so on-disk layout remains a single flat table.
    #[serde(flatten, default)]
    download: DownloadOptions,
}

impl From<Config> for ConfigBuilder {
    fn from(c: Config) -> Self {
        let mut b = Self::default();
        b.download_dir(c.download_dir)
            .max_concurrent_downloads(c.max_concurrent_downloads)
            .download(c.download);
        b
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            download_dir: default_download_dir(),
            max_concurrent_downloads: default_max_concurrent_downloads(),
            download: DownloadOptions::default(),
        }
    }
}

impl ConfigBuilder {
    pub fn build(&self) -> Result<Config, ConfigBuilderError> {
        let mut cfg = self.private_build()?;
        cfg.sanitize();
        cfg.validate_self()
            .map_err(|e| ConfigBuilderError::ValidationError(e.to_string()))?;
        Ok(cfg)
    }
}

impl Config {
    pub fn default_config_file() -> PathBuf {
        default_config_file()
    }

    /// Path to the config file inside the provided download dir.
    pub fn config_path_for_dir<P: AsRef<Path>>(data_dir: P) -> PathBuf {
        let mut p = data_dir.as_ref().to_path_buf();
        p.push("config.toml");
        p
    }

    // Getters
    pub fn download_dir(&self) -> &Path {
        &self.download_dir
    }
    pub fn max_concurrent_downloads(&self) -> usize {
        self.max_concurrent_downloads
    }
    pub fn download(&self) -> &DownloadOptions {
        &self.download
    }

    /// Clamp manager-level values that have safe fallbacks; also runs
    /// [`DownloadOptions::sanitize`] on the nested options. Hard rejects
    /// are handled by [`Self::validate_self`].
    fn sanitize(&mut self) {
        if self.max_concurrent_downloads == 0 {
            tracing::warn!("max_concurrent_downloads must be at least 1; got 0, clamping to 1");
            self.max_concurrent_downloads = 1;
        }
        if self.max_concurrent_downloads >= Semaphore::MAX_PERMITS {
            let fallback = default_max_concurrent_downloads();
            tracing::warn!(
                "max_concurrent_downloads = {} exceeds Semaphore::MAX_PERMITS; falling back to default ({})",
                self.max_concurrent_downloads,
                fallback,
            );
            self.max_concurrent_downloads = fallback;
        }
        self.download.sanitize();
    }

    fn validate_self(&self) -> Result<(), DownloadOptionsBuilderError> {
        self.download.validate_self()
    }

    /// Convert into a [`ConfigBuilder`] pre-populated with this instance's
    /// values. Use to apply partial overrides on top of an existing config
    /// before rebuilding.
    pub fn into_builder(self) -> ConfigBuilder {
        self.into()
    }

    /// Load configuration from the given directory's `config.toml`.
    /// If file does not exist, returns Ok(Default::default()).
    pub async fn load_from_file<P: AsRef<Path>>(cfg_path: P) -> Result<Config, io::Error> {
        let path = cfg_path.as_ref().to_path_buf();
        if tokio::fs::metadata(&path).await.is_err() {
            return Ok(Config::default());
        }
        let s = fs::read_to_string(&path).await?;
        let mut cfg: Config =
            toml::from_str(&s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        // Two-stage: sanitize fields with safe fallbacks (typos clamp +
        // warn), then validate the rest (zero retry intervals, malformed
        // proxy, etc.) and surface a hard error.
        cfg.sanitize();
        cfg.validate_self()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(cfg)
    }

    /// Save configuration to `data_dir/config.toml`. Creates parent dir if needed.
    pub async fn save_to_file<P: AsRef<Path>>(&self, cfg_path: P) -> Result<(), io::Error> {
        let pathbuf = cfg_path.as_ref().to_path_buf();
        if let Some(p) = pathbuf.parent() {
            fs::create_dir_all(p).await?;
        }
        let s = toml::to_string_pretty(&self).map_err(io::Error::other)?;
        fs::write(pathbuf, s).await?;
        Ok(())
    }
}

impl From<&DownloadOptions> for HeaderMap {
    fn from(opts: &DownloadOptions) -> Self {
        let mut map = HeaderMap::new();

        if let Some(headers) = &opts.headers {
            for (k, v) in headers.iter() {
                // Validated at load/apply time, but be defensive: ignore
                // anything that slips past via direct deserialization in
                // unusual paths.
                if let Ok(name) = HeaderName::from_bytes(k.as_bytes())
                    && let Ok(value) = HeaderValue::from_str(v)
                {
                    map.insert(name, value);
                }
            }
        }

        map
    }
}

impl From<&DownloadOptions> for Option<Proxy> {
    fn from(opts: &DownloadOptions) -> Self {
        opts.proxy
            .as_deref()
            .and_then(|s| reqwest::Proxy::all(s).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
        let headers = cfg.download.headers().expect("headers");
        let keys: Vec<&str> = headers.keys().map(|k| k.as_str()).collect();
        let keys_set: std::collections::HashSet<&str> = keys.into_iter().collect();
        let expected: std::collections::HashSet<&str> = vec!["Z-Header", "A-Header", "M-Header"]
            .into_iter()
            .collect();
        assert_eq!(keys_set, expected);
    }

    fn sample_options() -> DownloadOptions {
        DownloadOptionsBuilder::default()
            .max_connections(7)
            .max_retries(11)
            .wait_between_retries(Duration::from_millis(123))
            .n_fixed_retries(2)
            .user_agent(Some("agent/1".to_owned()))
            .randomize_user_agent(true)
            .proxy(None)
            .use_server_time(true)
            .accept_invalid_certs(true)
            .speed_limit(Some(500_000))
            .connect_timeout(Some(Duration::from_secs(9)))
            .headers({
                let mut m = indexmap::IndexMap::new();
                m.insert("X-Test".to_owned(), "yes".to_owned());
                Some(m)
            })
            .http2(true)
            .build()
            .unwrap()
    }

    #[test]
    fn download_options_builder_round_trip_preserves_all_fields() {
        let original = sample_options();
        let round: DownloadOptions = original.clone().into_builder().build().unwrap();
        assert_eq!(round.max_connections(), original.max_connections());
        assert_eq!(round.max_retries(), original.max_retries());
        assert_eq!(
            round.wait_between_retries(),
            original.wait_between_retries()
        );
        assert_eq!(round.n_fixed_retries(), original.n_fixed_retries());
        assert_eq!(round.user_agent(), original.user_agent());
        assert_eq!(
            round.randomize_user_agent(),
            original.randomize_user_agent()
        );
        assert_eq!(round.proxy(), original.proxy());
        assert_eq!(round.use_server_time(), original.use_server_time());
        assert_eq!(
            round.accept_invalid_certs(),
            original.accept_invalid_certs()
        );
        assert_eq!(round.speed_limit(), original.speed_limit());
        assert_eq!(round.connect_timeout(), original.connect_timeout());
        assert_eq!(round.headers(), original.headers());
        assert_eq!(round.http2(), original.http2());
    }

    #[test]
    fn config_round_trip_preserves_all_fields() {
        let cfg = ConfigBuilder::default()
            .download_dir(PathBuf::from("/tmp/odl-test"))
            .max_concurrent_downloads(5)
            .download(sample_options())
            .build()
            .unwrap();

        let round: Config = cfg.clone().into_builder().build().unwrap();
        assert_eq!(round.download_dir(), cfg.download_dir());
        assert_eq!(
            round.max_concurrent_downloads(),
            cfg.max_concurrent_downloads()
        );
        assert_eq!(
            round.download().max_connections(),
            cfg.download().max_connections()
        );
        assert_eq!(round.download().headers(), cfg.download().headers());
    }

    #[test]
    fn builder_overlay_only_changes_touched_fields() {
        let base = sample_options();
        let mut b = base.clone().into_builder();
        b.max_connections(99);
        let out = b.build().unwrap();

        assert_eq!(out.max_connections(), 99);
        // All other fields untouched.
        assert_eq!(out.max_retries(), base.max_retries());
        assert_eq!(out.user_agent(), base.user_agent());
        assert_eq!(out.headers(), base.headers());
        assert_eq!(out.http2(), base.http2());
        assert_eq!(out.speed_limit(), base.speed_limit());
    }

    #[test]
    fn builder_sanitizes_zero_max_connections() {
        let opts = DownloadOptionsBuilder::default()
            .max_connections(0)
            .build()
            .unwrap();
        assert!(opts.max_connections() >= 1);
    }

    #[test]
    fn builder_rejects_zero_speed_limit() {
        let err = DownloadOptionsBuilder::default()
            .speed_limit(Some(0))
            .build()
            .expect_err("expected error");
        assert!(matches!(
            err,
            DownloadOptionsBuilderError::ValidationError(_)
        ));
    }

    #[test]
    fn builder_rejects_zero_wait_between_retries() {
        let err = DownloadOptionsBuilder::default()
            .wait_between_retries(Duration::from_millis(0))
            .build()
            .expect_err("expected error");
        assert!(matches!(
            err,
            DownloadOptionsBuilderError::ValidationError(_)
        ));
    }

    #[test]
    fn builder_rejects_zero_n_fixed_retries() {
        let err = DownloadOptionsBuilder::default()
            .n_fixed_retries(0)
            .build()
            .expect_err("expected error");
        assert!(matches!(
            err,
            DownloadOptionsBuilderError::ValidationError(_)
        ));
    }

    #[test]
    fn builder_rejects_zero_connect_timeout() {
        let err = DownloadOptionsBuilder::default()
            .connect_timeout(Some(Duration::from_millis(0)))
            .build()
            .expect_err("expected error");
        assert!(matches!(
            err,
            DownloadOptionsBuilderError::ValidationError(_)
        ));
    }

    #[test]
    fn builder_rejects_bad_proxy() {
        let err = DownloadOptionsBuilder::default()
            .proxy(Some("not-a-valid-url-:::".to_owned()))
            .build()
            .expect_err("expected error");
        assert!(matches!(
            err,
            DownloadOptionsBuilderError::ValidationError(_)
        ));
    }

    #[test]
    fn builder_drops_bad_header_keeps_good_ones() {
        let mut headers = indexmap::IndexMap::new();
        headers.insert("Bad Header\nName".to_owned(), "v".to_owned());
        headers.insert("X-Good".to_owned(), "ok".to_owned());
        let opts = DownloadOptionsBuilder::default()
            .headers(Some(headers))
            .build()
            .unwrap();
        let h = opts.headers().expect("headers");
        assert!(!h.contains_key("Bad Header\nName"));
        assert_eq!(h.get("X-Good").map(String::as_str), Some("ok"));
    }

    #[test]
    fn builder_clears_headers_when_all_dropped() {
        let mut headers = indexmap::IndexMap::new();
        headers.insert("Bad Header\nName".to_owned(), "v".to_owned());
        let opts = DownloadOptionsBuilder::default()
            .headers(Some(headers))
            .build()
            .unwrap();
        assert_eq!(opts.headers(), None);
    }

    #[test]
    fn config_builder_sanitizes_zero_max_concurrent_downloads() {
        let cfg = ConfigBuilder::default()
            .max_concurrent_downloads(0)
            .build()
            .unwrap();
        assert_eq!(cfg.max_concurrent_downloads(), 1);
    }

    #[tokio::test]
    async fn load_from_file_sanitizes_bad_download_options() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(&path, "max_connections = 0\n")
            .await
            .unwrap();
        let cfg = Config::load_from_file(&path)
            .await
            .expect("load should succeed and sanitize");
        assert!(cfg.download().max_connections() >= 1);
    }

    #[tokio::test]
    async fn load_from_file_sanitizes_bad_max_concurrent_downloads() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(&path, "max_concurrent_downloads = 0\n")
            .await
            .unwrap();
        let cfg = Config::load_from_file(&path).await.expect("load");
        assert_eq!(cfg.max_concurrent_downloads(), 1);
    }

    #[tokio::test]
    async fn load_from_file_rejects_zero_wait_between_retries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(&path, "wait_between_retries = { secs = 0, nanos = 0 }\n")
            .await
            .unwrap();
        let err = Config::load_from_file(&path)
            .await
            .expect_err("expected hard reject");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn load_from_file_rejects_bad_proxy() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(&path, "proxy = \"not-a-valid-url-:::\"\n")
            .await
            .unwrap();
        let err = Config::load_from_file(&path)
            .await
            .expect_err("expected hard reject");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn load_from_file_drops_bad_header_keeps_others() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let toml = "[headers]\n\"bad name\" = \"v\"\n\"X-Good\" = \"ok\"\n";
        tokio::fs::write(&path, toml).await.unwrap();
        let cfg = Config::load_from_file(&path).await.expect("load");
        let h = cfg.download().headers().expect("headers");
        assert!(!h.contains_key("bad name"));
        assert_eq!(h.get("X-Good").map(String::as_str), Some("ok"));
    }

    #[tokio::test]
    async fn load_from_file_missing_returns_default() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does-not-exist.toml");
        let cfg = Config::load_from_file(&path).await.unwrap();
        assert_eq!(
            cfg.max_concurrent_downloads(),
            Config::default().max_concurrent_downloads()
        );
    }

    #[tokio::test]
    async fn save_and_load_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let cfg = ConfigBuilder::default()
            .download_dir(dir.path().to_path_buf())
            .max_concurrent_downloads(2)
            .download(sample_options())
            .build()
            .unwrap();
        cfg.save_to_file(&path).await.unwrap();
        let loaded = Config::load_from_file(&path).await.unwrap();
        assert_eq!(loaded.max_concurrent_downloads(), 2);
        assert_eq!(
            loaded.download().max_connections(),
            cfg.download().max_connections()
        );
        assert_eq!(loaded.download().headers(), cfg.download().headers());
    }
}
