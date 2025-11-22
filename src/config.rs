use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::{fs, io};

/// Persistent download manager configuration. Fields are optional so that
/// unspecified values can fall back to code defaults.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    pub data_dir: Option<PathBuf>,
    pub max_connections: Option<u64>,
    pub max_concurrent_downloads: Option<usize>,
    pub max_retries: Option<u64>,
    /// wait between retries in seconds (can be fractional)
    pub wait_between_retries_secs: Option<f64>,
    pub user_agent: Option<String>,
    pub randomize_user_agent: Option<bool>,
    pub proxy: Option<String>,
    pub use_server_time: Option<bool>,
    pub accept_invalid_certs: Option<bool>,
    pub speed_limit: Option<u64>,
    /// connect timeout in seconds (can be fractional)
    pub connect_timeout_secs: Option<f64>,
}

impl Config {
    /// Path to the config file inside the provided download dir.
    pub fn config_path_for_dir<P: AsRef<Path>>(data_dir: P) -> PathBuf {
        let mut p = data_dir.as_ref().to_path_buf();
        p.push("config.toml");
        p
    }

    /// Load configuration from the given directory's `config.toml`.
    /// If file does not exist, returns Ok(Default::default()).
    pub fn load_from_dir<P: AsRef<Path>>(data_dir: P) -> Result<Config, io::Error> {
        let path = Config::config_path_for_dir(data_dir);
        if !path.exists() {
            return Ok(Config::default());
        }
        let s = fs::read_to_string(&path)?;
        let cfg: Config =
            toml::from_str(&s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(cfg)
    }

    /// Save configuration to `data_dir/config.toml`. Creates parent dir if needed.
    pub fn save_to_dir<P: AsRef<Path>>(&self, data_dir: P) -> Result<(), io::Error> {
        let cfg_path = Config::config_path_for_dir(data_dir);
        if let Some(p) = cfg_path.parent() {
            fs::create_dir_all(p)?;
        }
        let s =
            toml::to_string_pretty(&self).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(cfg_path, s)?;
        Ok(())
    }
}
