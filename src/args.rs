use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clap::{Parser, Subcommand};

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum FileChangedAction {
    Abort,
    Restart,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum NotResumableAction {
    Abort,
    Restart,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum SameDownloadAction {
    Abort,
    Resume,
    AddNumberToNameAndContinue,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum FinalFileAction {
    Abort,
    ReplaceAndContinue,
    AddNumberToNameAndContinue,
}

fn parse_speed(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty speed string".to_string());
    }

    // Remove common trailing rate markers like `/s` or `bps` (case-insensitive)
    let mut working = s.to_string();
    let lower = working.to_lowercase();
    if lower.ends_with("/s") {
        working.truncate(working.len() - 2);
    } else if lower.ends_with("bps") {
        working.truncate(working.len() - 3);
    }
    let working = working.trim();

    // split into numeric prefix and suffix
    let mut idx = 0usize;
    for (i, ch) in working.char_indices() {
        if !(ch.is_ascii_digit() || ch == '.') {
            idx = i;
            break;
        }
        idx = i + ch.len_utf8();
    }

    let (num_part, suf_part) = if idx == 0 {
        // no numeric prefix
        return Err(format!("invalid speed '{}': missing numeric value", s));
    } else if idx >= working.len() {
        (working, "")
    } else {
        (working[..idx].trim(), working[idx..].trim())
    };

    let value =
        f64::from_str(num_part).map_err(|e| format!("invalid number '{}': {}", num_part, e))?;
    if value < 0.0 {
        return Err("speed must be non-negative".to_string());
    }

    let suffix_owned = suf_part
        .trim()
        .trim_start_matches([' ', '\t', '\''])
        .to_lowercase();

    // Determine multiplier (all based on 1024)
    let multiplier: f64 = match suffix_owned.as_str() {
        "" | "b" | "byte" | "bytes" => 1.0,
        "k" | "kb" | "kib" | "kibibyte" | "kb/s" => 1024f64,
        "m" | "mb" | "mib" | "mibibyte" => 1024f64.powi(2),
        "g" | "gb" | "gib" | "gibibyte" => 1024f64.powi(3),
        // Allow common variants like "kib/s" trimmed earlier, also accept single-letter with optional trailing 'b'
        other => {
            // try to match prefixes (e.g., "kib", "kb", "k")
            let o = other.trim();
            if o.starts_with('k') {
                1024f64
            } else if o.starts_with('m') {
                1024f64.powi(2)
            } else if o.starts_with('g') {
                1024f64.powi(3)
            } else {
                return Err(format!("unknown size suffix '{}'", other));
            }
        }
    };

    let bytes_f = value * multiplier;
    if !bytes_f.is_finite() || bytes_f < 0.0 {
        return Err("resulting speed is out of range".to_string());
    }
    let bytes = bytes_f as u128; // use wider intermediate to reduce overflow risk
    if bytes > (u64::MAX as u128) {
        return Err("speed too large".to_string());
    }
    Ok(bytes as u64)
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration string".to_string());
    }

    // split numeric prefix and suffix
    let mut idx = 0usize;
    for (i, ch) in s.char_indices() {
        if !(ch.is_ascii_digit() || ch == '.') {
            idx = i;
            break;
        }
        idx = i + ch.len_utf8();
    }

    if idx == 0 {
        return Err(format!("invalid duration '{}': missing numeric value", s));
    }

    let (num_part, suf_part) = if idx >= s.len() {
        (s, "")
    } else {
        (s[..idx].trim(), s[idx..].trim())
    };

    let value =
        f64::from_str(num_part).map_err(|e| format!("Invalid number '{}': {}", num_part, e))?;
    if !value.is_finite() || value < 0.0 {
        return Err("Duration must be a non-negative finite number".to_string());
    }

    let suffix = suf_part
        .trim()
        .trim_start_matches([' ', '\t', '\''])
        .to_lowercase();

    let multiplier_secs = match suffix.as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1.0,
        "m" | "min" | "mins" | "minute" | "minutes" => 60.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3600.0,
        "d" | "day" | "days" => 86400.0,
        other => {
            // accept common variants with plurals/prefixes
            if other.starts_with('s') {
                1.0
            } else if other.starts_with('m') {
                60.0
            } else if other.starts_with('h') {
                3600.0
            } else if other.starts_with('d') {
                86400.0
            } else {
                return Err(format!("unknown duration suffix '{}'", other));
            }
        }
    };

    let secs_f = value * multiplier_secs;
    if !secs_f.is_finite() || secs_f < 0.0 {
        return Err("Resulting duration is out of range".to_string());
    }
    Ok(Duration::from_secs_f64(secs_f))
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// The URL of the file to download, or a path to a file containing one URL per line.
    /// Blank lines and lines starting with `#` or `//` are ignored.
    /// Optional so subcommands (like `config`) can be used without providing an input.
    pub input: Option<String>,

    /// If true, tries to download the file at url and read it as a text file and then use it as input
    #[arg(long, default_value_t = false)]
    pub remote_list: bool,

    /// Max connections that download manager can make in parallel for a single file
    #[arg(long, value_name = "COUNT")]
    pub max_connections: Option<u64>,

    /// The maximum number of files that the download manager can download in parallel.
    ///
    /// This controls the overall concurrency of downloads. For example, if set to 4, up to 4 files
    /// will be downloaded at the same time, regardless of how many connections are used for each file.
    ///
    /// Note: For controlling how many parts of a single file can be downloaded concurrently,
    /// see the `max_connections` option.
    #[arg(long, value_name = "COUNT")]
    pub max_concurrent_downloads: Option<usize>,

    /// When `input` is a URL, this specifies the output file path.
    /// When `input` is a file containing URLs, this specifies the output directory for downloaded files.
    /// Will use server provided name if not specified or if `input` is a file.
    #[arg(short, long, value_name = "FILE|DIR")]
    pub output: Option<PathBuf>,

    /// This is the path where odl tracks download progress.
    /// All data will be downloaded here first before being appended at the output location.
    #[arg(short, long, value_name = "DIR")]
    pub download_dir: Option<PathBuf>,

    /// The config file to use. defaults to `odl/config.toml` inside user's appdata directory (varies based on OS)
    #[arg(short, long, value_name = "FILE")]
    pub config_file: Option<PathBuf>,

    /// User agent to use for making requests. This option overrides random-user-agent.
    #[arg(short = 'U', long)]
    pub user_agent: Option<String>,

    /// Should the user_agent be randomized for each request?
    #[arg(long)]
    pub randomize_user_agent: Option<bool>,

    #[arg(long, value_name = "(http(s)|socks)://")]
    pub proxy: Option<String>,

    /// Connect timeout for requests. Accepts suffixes like `30s`, `5m`, `2h`, `1d` or long forms (`seconds`, `minutes`, `hours`, `days`). Default `5s`. Default Unit is seconds if omitted.
    #[arg(short, long = "timeout", value_name = "DURATION", value_parser = parse_duration)]
    pub timeout: Option<Duration>,

    /// Max number of retries in case of a network error
    #[arg(long, value_name = "COUNT")]
    pub max_retries: Option<u32>,

    /// Number of fixed (non-exponential) retries before exponential backoff starts
    #[arg(long, value_name = "COUNT")]
    pub n_fixed_retries: Option<u32>,

    /// Wait number of seconds after a network error before retry. Fractions are supported.
    #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
    pub wait_between_retries: Option<Duration>,

    /// If true, sets the downloaded file's last-modified timestamp to match the server's value (if available).
    #[arg(short, long)]
    pub use_server_time: Option<bool>,

    /// How to handle a server file-changed conflict. Possible values: `abort`, `restart`.
    /// Default: `restart` (restart the download and warn).
    #[arg(long, value_enum, default_value_t = FileChangedAction::Restart)]
    pub on_file_changed: FileChangedAction,

    /// How to handle a server not-resumable conflict. Possible values: `abort`, `restart`.
    /// Default: `restart` (restart the download and warn).
    #[arg(long, value_enum, default_value_t = NotResumableAction::Restart)]
    pub on_not_resumable: NotResumableAction,

    /// Should we accept invalid SSL certificates? Do not use unless you are absolutely sure of what you are doing.
    #[arg(long)]
    pub accept_invalid_certs: Option<bool>,

    /// Custom HTTP headers to include in each request. Specify as `KEY:VALUE`.
    #[arg(long = "header", value_name = "KEY:VALUE", num_args = 0.., action = clap::ArgAction::Append)]
    pub headers: Vec<String>,

    /// How to handle a save conflict when the same download structure exists. Possible values: `abort`, `resume`, `add-number-to-name-and-continue`.
    /// Default: `resume`.
    #[arg(long, value_enum, default_value_t = SameDownloadAction::Resume)]
    pub on_same_download_exists: SameDownloadAction,

    /// How to handle a save conflict when a final file already exists. Possible values: `abort`, `replace-and-continue`, `add-number-to-name-and-continue`.
    /// Default: `replace-and-continue`.
    #[arg(long, value_enum, default_value_t = FinalFileAction::ReplaceAndContinue)]
    pub on_final_file_exists: FinalFileAction,

    /// HTTP basic authentication username.
    #[arg(long, value_name = "USER")]
    pub http_user: Option<String>,

    /// HTTP basic authentication password.
    #[arg(long, value_name = "PASSWORD")]
    pub http_password: Option<String>,

    /// Maximum aggregate download speed per file in bytes per second.
    /// Accepts human-readable values like `100KB`, `1.5MiB`, `2G` (all units parsed as base 1024).
    /// When unset, downloads run at full speed.
    #[arg(short, long, value_name = "BYTES_PER_SEC", value_parser = parse_speed)]
    pub speed_limit: Option<u64>,
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Configure persistent download-manager settings saved in odl/config.toml
    Config {
        /// Print current configuration path and content
        #[arg(long)]
        show: bool,

        /// Config file to change (defaults to standard odl config path).
        /// You can use this to configure different download managers.
        #[arg(long, value_name = "FILE")]
        config_file: Option<PathBuf>,

        /// Where download manager keeps each download's parts and progress metadata
        #[arg(long, value_name = "DIR")]
        download_dir: Option<PathBuf>,

        /// Set max connections per-file
        #[arg(long, value_name = "COUNT")]
        max_connections: Option<u64>,

        /// Set maximum concurrent downloads
        #[arg(long, value_name = "COUNT")]
        max_concurrent_downloads: Option<usize>,

        /// Set max retries
        #[arg(long, value_name = "COUNT")]
        max_retries: Option<u32>,

        /// Number of fixed (non-exponential) retries before exponential backoff starts
        #[arg(long, value_name = "COUNT")]
        n_fixed_retries: Option<u32>,

        /// Wait between retries. Accepts suffixes like `30s`, `5m`, `2h`, `1d` or long forms (`seconds`, `minutes`, `hours`, `days`). Default `5s`. Default Unit is seconds if omitted.
        #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
        wait_between_retries: Option<Duration>,

        /// Download speed limit (bytes/sec) e.g. 1MiB
        #[arg(short, long, value_name = "BYTES_PER_SEC", value_parser = parse_speed)]
        speed_limit: Option<u64>,

        /// Custom user agent
        #[arg(long)]
        user_agent: Option<String>,

        /// Randomize user agent
        #[arg(long)]
        randomize_user_agent: Option<bool>,

        /// Proxy as string
        #[arg(long)]
        proxy: Option<String>,

        /// Connect timeout for requests. Accepts suffixes like `30s`, `5m`, `2h`, `1d` or long forms (`seconds`, `minutes`, `hours`, `days`). Default `5s`. Default Unit is seconds if omitted.
        #[arg(short, long = "timeout", value_name = "DURATION", value_parser = parse_duration)]
        timeout: Option<Duration>,

        /// Use server time when saving
        #[arg(long)]
        use_server_time: Option<bool>,

        /// Accept invalid certs
        #[arg(long)]
        accept_invalid_certs: Option<bool>,
    },
}

#[cfg(test)]
mod tests {
    use super::parse_duration;
    use super::parse_speed;
    use std::time::Duration;

    #[test]
    fn test_simple_bytes() {
        assert_eq!(parse_speed("100").unwrap(), 100);
        assert_eq!(parse_speed("100B").unwrap(), 100);
    }

    #[test]
    fn test_kilobytes() {
        assert_eq!(parse_speed("1K").unwrap(), 1024);
        assert_eq!(parse_speed("1KB").unwrap(), 1024);
        assert_eq!(parse_speed("100kib").unwrap(), 100 * 1024);
    }

    #[test]
    fn test_megabytes() {
        assert_eq!(parse_speed("1M").unwrap(), 1024u64.pow(2));
        assert_eq!(
            parse_speed("1.5MB").unwrap(),
            ((1.5f64 * (1024f64.powi(2))) as u64)
        );
    }

    #[test]
    fn test_gigabytes() {
        assert_eq!(parse_speed("2G").unwrap(), 2 * 1024u64.pow(3));
        assert_eq!(parse_speed("2GiB").unwrap(), 2 * 1024u64.pow(3));
    }

    #[test]
    fn test_suffix_with_per_second() {
        assert_eq!(parse_speed("100KB/s").unwrap(), 100 * 1024);
        assert_eq!(
            parse_speed("1.5MiB/s").unwrap(),
            ((1.5f64 * (1024f64.powi(2))) as u64)
        );
    }

    #[test]
    fn test_parse_duration_seconds_and_variants() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("30sec").unwrap(), Duration::from_secs(30));
        assert_eq!(
            parse_duration("30seconds").unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn test_parse_duration_minutes_hours_days() {
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("2min").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("1d").unwrap(), Duration::from_secs(86400));
        // fractional hours
        let d = parse_duration("1.5h").unwrap();
        assert!((d.as_secs_f64() - 1.5 * 3600.0).abs() < 1e-6);
    }
}
