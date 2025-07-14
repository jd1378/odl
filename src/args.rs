use std::path::PathBuf;

use clap::Parser;

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
    pub max_concurrent_downloads: u64,

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
}
