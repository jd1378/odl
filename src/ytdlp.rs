//! Delegation of downloads to an externally installed `yt-dlp`.
//!
//! odl never bundles `yt-dlp`: it is discovered on the system at runtime, and
//! its absence simply means a URL is downloaded by the built-in HTTP engine
//! instead. Only a curated set of hosts is delegated — see [`hosts`] — so the
//! choice of engine stays predictable rather than depending on what a server
//! happens to return.
//!
//! Layering: nothing outside this module knows that `yt-dlp` exists. The rest
//! of the crate deals in [`crate::engine`] types only.

pub mod binary;
pub mod extract;
pub mod hosts;
pub mod install;
pub mod process;
pub mod run;

use crate::config::YtdlpOptions;
use crate::error::YtdlpError;
use url::Url;

pub use binary::Tools;
pub use process::ManagedChild;

/// Whether `url` should be delegated, considering configuration only.
///
/// Cheap: no process is spawned and nothing is looked up on disk. Callers pair
/// this with [`binary::discover`] to decide the engine for a download.
pub fn should_delegate(url: &Url, opts: &YtdlpOptions) -> bool {
    opts.enabled() && hosts::is_delegated_url(url, opts.extra_hosts(), opts.excluded_hosts())
}

/// Locate the tools needed to run a delegated download.
pub async fn tools(opts: &YtdlpOptions) -> Result<Tools, YtdlpError> {
    binary::discover(opts).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::YtdlpOptionsBuilder;

    #[test]
    fn disabled_config_delegates_nothing() {
        let opts = YtdlpOptionsBuilder::default()
            .enabled(false)
            .build()
            .unwrap();
        assert!(!should_delegate(
            &Url::parse("https://www.youtube.com/watch?v=x").unwrap(),
            &opts
        ));
    }

    #[test]
    fn enabled_config_delegates_listed_hosts_only() {
        let opts = YtdlpOptions::default();
        assert!(should_delegate(
            &Url::parse("https://www.youtube.com/watch?v=x").unwrap(),
            &opts
        ));
        assert!(!should_delegate(
            &Url::parse("https://example.com/file.zip").unwrap(),
            &opts
        ));
    }
}
