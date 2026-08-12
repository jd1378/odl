use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clap::{Parser, Subcommand};

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputFormat {
    /// Human-readable output: progress bars, plain text.
    Text,
    /// Machine-readable output: NDJSON progress events (download) or a
    /// single JSON document (config/probe/status). Intended for agents
    /// and scripts. Progress bars are suppressed.
    Json,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

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

/// Which download engine to use.
/// What `odl tools` should do.
#[derive(clap::Subcommand, Debug)]
pub enum ToolsAction {
    /// Report which helpers are installed, where, and which version.
    Status,
    /// Download helpers into odl's data directory and record their paths in
    /// the config file. Downloads are verified against checksums published
    /// with them, and the latest release is always used — extractors break as
    /// sites change, so a pinned version would rot.
    Install {
        /// Which helper to install. Both, when omitted.
        #[arg(value_enum)]
        tool: Option<ToolChoice>,
        /// Install without asking for confirmation.
        #[arg(long, short = 'y')]
        yes: bool,
    },
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolChoice {
    #[value(name = "yt-dlp")]
    Ytdlp,
    Ffmpeg,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EngineChoice {
    /// Delegate configured media hosts to yt-dlp when it is installed, and
    /// download everything else over HTTP.
    #[default]
    Auto,
    /// Always use odl's own multipart HTTP downloader.
    Http,
    /// Always delegate to yt-dlp, failing if it is unavailable.
    Ytdlp,
}

/// When to ask which quality to download.
#[derive(clap::ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ChooseFormat {
    /// Ask when the terminal is interactive and there is a single download to
    /// decide about; take the best available otherwise.
    #[default]
    Auto,
    /// Always ask, even for a download that already picked a format. Choosing
    /// a different quality discards what was downloaded and starts over,
    /// since two encodings cannot be joined; pair it with
    /// `--on-file-changed restart` to accept that without a prompt.
    ///
    /// Still silent without a terminal or with `--format json`: a question
    /// nobody can answer is a hang, not a prompt.
    Always,
    /// Never ask; take the best available.
    Never,
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

/// Stable description of the machine-readable interface, shown under
/// `odl --help`. This is a documented contract: agents and scripts may
/// rely on these exit codes and JSON shapes across patch/minor releases.
/// Keep in sync with `exit_code`/`error_kind` in `main.rs` and the
/// `JsonReporter` in `json.rs`.
const MACHINE_INTERFACE_HELP: &str = "\
EXIT CODES:
  0    success
  1    other / internal error
  2    usage or invalid input (bad URL, missing input, invalid config/flags)
  3    network error (DNS, timeout, HTTP status, connection)
  4    conflict (save/server conflict, checksum mismatch)
  5    I/O error
  6    metadata error (lockfile in use, decode failure)
  7    yt-dlp error (missing, too old, failed, or unsupported URL)
  130  cancelled

JSON OUTPUT (--format json):
  Downloads stream newline-delimited JSON (NDJSON) to stdout, one object
  per line, each tagged with \"type\" and \"url\":
    phase      {\"phase\": evaluating|resolving_conflicts|downloading|post_processing|assembling|flushing|verifying}
    filename   {\"filename\"}
    progress   {\"downloaded\", \"total\": <int|null>}
    message    {\"message\"}
    completed  {\"path\", \"already_complete\"}
    failed     {\"message\"}
    cancelled  {}
  One-shot commands emit a single JSON document on stdout:
    probe        {\"type\":\"probe\", filename, size, size_is_approx, engine, quality, resumable, etag, last_modified, checksums, ...}
    status/list  {\"type\":\"status\", count, downloads:[...]}
    config       {\"type\":\"config\", path, config}  (config_saved on write)
  Errors print one JSON object to stderr:
    {\"type\":\"error\", \"kind\", \"message\", \"exit_code\"}

ENGINES:
  Links on known media hosts are handed to an installed `yt-dlp`; everything
  else uses odl's own multipart HTTP downloader. `--engine` forces the choice.
  A delegated download reports \"engine\":\"ytdlp\" and cannot observe the
  underlying HTTP exchange, so etag/last_modified are null and checksums is
  empty; sizes may be estimates (\"size_is_approx\": true). Configure it under
  [ytdlp] in config.toml.

  Quality is chosen once and pinned: a resume never mixes encodings. To change
  it, name another format (--format-id) or ask again (--choose-format always);
  either discards what was downloaded and starts over.";

#[derive(Parser, Debug)]
#[command(version, about, long_about = None, after_long_help = MACHINE_INTERFACE_HELP)]
pub struct Args {
    /// The URL of the file to download, or a path to a file containing one URL per line.
    /// Blank lines and lines starting with `#` or `//` are ignored.
    /// Optional so subcommands (like `config`) can be used without providing an input.
    pub input: Option<String>,

    /// If true, tries to download the file at url and read it as a text file and then use it as input
    #[arg(long, default_value_t = false)]
    pub remote_list: bool,

    /// Which download engine to use.
    #[arg(long, value_enum, default_value_t = EngineChoice::Auto)]
    pub engine: EngineChoice,

    /// Whether to ask which quality to download when a media host is delegated to yt-dlp.
    #[arg(long, value_enum, default_value_t = ChooseFormat::Auto)]
    pub choose_format: ChooseFormat,

    /// Skip verifying the finished file against known checksums.
    ///
    /// The checksums are still recorded and reported; odl just stops hashing
    /// the file to act on them, which a caller may prefer to do itself. The
    /// file's size is still checked.
    #[arg(long)]
    pub no_verify_checksums: bool,

    /// Transliterate filenames to ASCII: `Café` is saved as `Cafe`, and a
    /// title in any script becomes something every terminal and filesystem
    /// renders the same way.
    ///
    /// Lossy, and it renames the per-download directory — a download already
    /// in progress under the other setting starts over.
    #[arg(long)]
    pub ascii_filenames: bool,

    /// Download this exact media format instead of asking or picking the best.
    /// Naming a different format than a download already started discards what
    /// was downloaded and starts over, since encodings cannot be joined.
    #[arg(long, value_name = "ID")]
    pub format_id: Option<String>,

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

    /// Connect directly: ignore the configured proxy and any proxy set in the environment
    #[arg(long, conflicts_with = "proxy")]
    pub no_proxy: bool,

    /// Connect timeout for requests. Accepts suffixes like `30s`, `5m`, `2h`, `1d` or long forms (`seconds`, `minutes`, `hours`, `days`). Default `5s`.
    #[arg(short, long = "timeout", value_name = "DURATION", value_parser = humantime::parse_duration)]
    pub timeout: Option<Duration>,

    /// Max number of retries in case of a network error
    #[arg(long, value_name = "COUNT")]
    pub max_retries: Option<u32>,

    /// Number of fixed (non-exponential) retries before exponential backoff starts
    #[arg(long, value_name = "COUNT")]
    pub n_fixed_retries: Option<u32>,

    /// Wait number of seconds after a network error before retry. Fractions are supported.
    #[arg(long, value_name = "DURATION", value_parser = humantime::parse_duration)]
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

    /// Enable HTTP/2 (default: HTTP/1.1 only). HTTP/1.1 opens a separate
    /// TCP connection per part, which usually yields higher throughput
    /// on high-bandwidth links — especially on Windows where h2's
    /// flow-control windows on a single TCP can throttle downloads.
    #[arg(long)]
    pub http2: Option<bool>,

    /// Allow mid-flight subdivision of long-running parts to keep idle
    /// connections busy. Default: enabled. Pass `--dynamic-split false`
    /// to lock the part layout chosen at evaluate / resume time.
    #[arg(long)]
    pub dynamic_split: Option<bool>,

    /// Stagger the opening of connections so a server enforcing a per-IP
    /// connection-rate limit is not tripped. Default: enabled. Pass
    /// `--rampup false` to open every connection at once.
    #[arg(long)]
    pub rampup: Option<bool>,

    /// Connections opened per rampup batch. Must be >= 1.
    #[arg(long, value_name = "COUNT")]
    pub rampup_batch_size: Option<u64>,

    /// Lower bound of the random delay between rampup batches. Fractions
    /// are supported.
    #[arg(long, value_name = "DURATION", value_parser = humantime::parse_duration)]
    pub rampup_delay_min: Option<Duration>,

    /// Upper bound of the random delay between rampup batches. Must be
    /// >= `--rampup-delay-min`. Fractions are supported.
    #[arg(long, value_name = "DURATION", value_parser = humantime::parse_duration)]
    pub rampup_delay_max: Option<Duration>,

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

    /// Expected checksum(s) to verify the assembled file against. Format
    /// `ALGO:DIGEST` (digest hex-encoded) or `ALGO:ENCODING:DIGEST`.
    /// ALGO: `md5`, `sha1`, `sha256`, `sha384`, `sha512`.
    /// ENCODING: `hex` (default) or `base64`.
    /// Repeatable; checked in addition to any server-advertised checksums.
    /// A mismatch fails the download with a conflict error (exit code 4).
    #[arg(long = "checksum", value_name = "ALGO:DIGEST", action = clap::ArgAction::Append)]
    pub checksums: Vec<String>,

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

    /// Diagnostic log severity. Overridden by `RUST_LOG` env var when set.
    /// Possible values: `off`, `error`, `warn`, `info`, `debug`, `trace`. Default: `warn`.
    #[arg(long, value_enum, default_value_t = LogLevel::Warn)]
    pub log_level: LogLevel,

    /// Output format. `text` (default) is human-readable; `json` emits
    /// machine-readable output for scripts and agents (NDJSON progress
    /// events while downloading; a single JSON document for
    /// `config --show`, `probe`, and `status`/`list`). Errors are emitted
    /// as a JSON object on stderr. See also the per-variant exit codes.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text, global = true)]
    pub format: OutputFormat,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
// `Config` carries one optional field per persisted setting, so it dwarfs the
// other variants. Boxing it would mean giving up clap's derive on the variant
// for a struct that is built once, at startup, from argv.
#[allow(clippy::large_enum_variant)]
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

        /// Wait between retries. Accepts suffixes like `30s`, `5m`, `2h`, `1d` or long forms (`seconds`, `minutes`, `hours`, `days`). Default `5s`.
        #[arg(long, value_name = "DURATION", value_parser = humantime::parse_duration)]
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

        /// Connect directly, ignoring the configured proxy and the environment's.
        /// Turning it on clears any stored proxy.
        #[arg(long)]
        no_proxy: Option<bool>,

        /// Connect timeout for requests. Accepts suffixes like `30s`, `5m`, `2h`, `1d` or long forms (`seconds`, `minutes`, `hours`, `days`). Default `5s`.
        #[arg(short, long = "timeout", value_name = "DURATION", value_parser = humantime::parse_duration)]
        timeout: Option<Duration>,

        /// Use server time when saving
        #[arg(long)]
        use_server_time: Option<bool>,

        /// Accept invalid certs
        #[arg(long)]
        accept_invalid_certs: Option<bool>,

        /// Enable HTTP/2 (default: HTTP/1.1 only).
        #[arg(long)]
        http2: Option<bool>,

        /// Allow mid-flight subdivision of long-running parts.
        #[arg(long)]
        dynamic_split: Option<bool>,

        /// Stagger the opening of connections against per-IP rate limits
        #[arg(long)]
        rampup: Option<bool>,

        /// Connections opened per rampup batch
        #[arg(long, value_name = "COUNT")]
        rampup_batch_size: Option<u64>,

        /// Lower bound of the random delay between rampup batches
        #[arg(long, value_name = "DURATION", value_parser = humantime::parse_duration)]
        rampup_delay_min: Option<Duration>,

        /// Upper bound of the random delay between rampup batches
        #[arg(long, value_name = "DURATION", value_parser = humantime::parse_duration)]
        rampup_delay_max: Option<Duration>,
    },

    /// Install or inspect the helper programs odl uses for media sites.
    ///
    /// `yt-dlp` turns a media page link into a downloadable video; `ffmpeg`
    /// joins the separate video and audio streams that sites serve for higher
    /// qualities. Both are optional, and installing them yourself and setting
    /// `ytdlp.binary_path` / `ytdlp.ffmpeg_path` in config.toml works equally
    /// well.
    Tools {
        #[command(subcommand)]
        action: ToolsAction,
    },

    /// Replace this odl with the latest GitHub release.
    ///
    /// Only an odl the install script put in place is replaced — a copy
    /// installed by cargo, Homebrew, Nix or a distribution package is left to
    /// the command that owns it. The archive is verified against the SHA-256
    /// published beside it before anything is overwritten.
    #[cfg(feature = "self-update")]
    Update {
        /// Report what an update would do and exit, without downloading or
        /// replacing anything.
        #[arg(long)]
        check: bool,

        /// Do not ask before replacing the binary.
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Probe a URL without downloading: report the resolved filename,
    /// size, resumability, etag, last-modified, and any server-advertised
    /// checksums. Pair with `--format json` for machine-readable output.
    Probe {
        /// The URL to probe.
        #[arg(value_name = "URL")]
        url: String,
    },

    /// Show tracked downloads in the configured download directory:
    /// per-download progress (bytes on disk vs. total), part counts, and
    /// whether the final file has been assembled. Optional FILTER matches
    /// a substring of the URL or filename.
    Status {
        /// Only show downloads whose URL or filename contains this string.
        #[arg(value_name = "FILTER")]
        filter: Option<String>,
    },

    /// List tracked downloads (brief). Alias of `status` with terse
    /// output; honors `--format json`.
    List {
        /// Only show downloads whose URL or filename contains this string.
        #[arg(value_name = "FILTER")]
        filter: Option<String>,
    },
}

#[cfg(test)]
mod tests {
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
        assert_eq!(
            humantime::parse_duration("30s").unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            humantime::parse_duration("30sec").unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            humantime::parse_duration("30seconds").unwrap(),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn test_parse_duration_minutes_hours_days() {
        assert_eq!(
            humantime::parse_duration("2m").unwrap(),
            Duration::from_secs(120)
        );
        assert_eq!(
            humantime::parse_duration("2min").unwrap(),
            Duration::from_secs(120)
        );
        assert_eq!(
            humantime::parse_duration("1h").unwrap(),
            Duration::from_secs(3600)
        );
        assert_eq!(
            humantime::parse_duration("1d").unwrap(),
            Duration::from_secs(86400)
        );
        // fractional hours
        let d = humantime::parse_duration("1.5h").unwrap();
        assert!((d.as_secs_f64() - 1.5 * 3600.0).abs() < 1e-6);
    }
}
