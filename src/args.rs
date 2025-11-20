use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;

fn parse_speed(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty speed".to_string());
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
        .trim_start_matches(|c: char| c == ' ' || c == '\t' || c == '\'')
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
        return Err("resulting speed out of range".to_string());
    }
    let bytes = bytes_f as u128; // use wider intermediate to reduce overflow risk
    if bytes > (u64::MAX as u128) {
        return Err("speed too large".to_string());
    }
    Ok(bytes as u64)
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// The URL of the file to download, or a path to a file containing one URL per line.
    /// Blank lines and lines starting with `#` or `//` are ignored.
    pub input: String,

    /// If true, tries to download the file at url and read it as a text file and then use it as input
    #[arg(long, default_value_t = false)]
    pub remote_list: bool,

    /// Max connections that download manager can make in parallel for a single file
    #[arg(long, default_value_t = 4, value_name = "COUNT")]
    pub max_connections: u64,

    /// The maximum number of files that the download manager can download in parallel.
    ///
    /// This controls the overall concurrency of downloads. For example, if set to 4, up to 4 files
    /// will be downloaded at the same time, regardless of how many connections are used for each file.
    ///
    /// Note: For controlling how many parts of a single file can be downloaded concurrently,
    /// see the `max_connections` option.
    #[arg(long, default_value_t = 3, value_name = "COUNT")]
    pub max_concurrent_downloads: usize,

    /// When `input` is a URL, this specifies the output file path.
    /// When `input` is a file containing URLs, this specifies the output directory for downloaded files.
    /// Will use server provided name if not specified or if `input` is a file.
    #[arg(short, long, value_name = "FILE|DIR")]
    pub output: Option<PathBuf>,

    /// This is the path where odl tracks download progress.
    /// All data will be downloaded here first before being appended at the output location.
    #[arg(short, long, value_name = "DIR")]
    pub temp_download_dir: Option<PathBuf>,

    /// User agent to use for making requests. This option overrides random-user-agent.
    #[arg(short = 'U', long)]
    pub user_agent: Option<String>,

    /// Should the user_agent be randomized for each request?
    #[arg(long, default_value_t = true)]
    pub randomize_user_agent: bool,

    #[arg(long, value_name = "(http(s)|socks)://")]
    pub proxy: Option<String>,

    /// Max number of retries in case of a network error
    #[arg(long, default_value_t = 10, value_name = "COUNT")]
    pub retry: u64,

    /// Wait number of seconds after a network error before retry. Fractions are supported.
    #[arg(long, default_value_t = 0.3, value_name = "Seconds")]
    pub waitretry: f32,

    /// If true, sets the downloaded file's last-modified timestamp to match the server's value (if available).
    #[arg(short, long, default_value_t = false)]
    pub use_server_time: bool,

    /// Should we accept invalid SSL certificates? Do not use unless you are absolutely sure of what you are doing.
    #[arg(long, default_value_t = false)]
    pub accept_invalid_certs: bool,

    /// Custom HTTP headers to include in each request. Specify as `KEY:VALUE`.
    #[arg(long = "header", value_name = "KEY:VALUE", num_args = 0.., action = clap::ArgAction::Append)]
    pub headers: Vec<String>,

    /// If set, tries to replace existing files and continue without aborting.
    /// Default behavior is to abort on any kind of conflict.
    #[arg(short, long, default_value_t = false)]
    pub force: bool,

    /// HTTP basic authentication username.
    #[arg(long, value_name = "USER")]
    pub http_user: Option<String>,

    /// HTTP basic authentication password.
    #[arg(long, value_name = "PASSWORD")]
    pub http_password: Option<String>,

    /// Maximum aggregate download speed per file in bytes per second.
    /// Accepts human-readable values like `100KB`, `1.5MiB`, `2G` (all units parsed as base 1024).
    /// When unset, downloads run at full speed.
    #[arg(long, value_name = "BYTES_PER_SEC", value_parser = parse_speed)]
    pub speed_limit: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::parse_speed;

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
}
