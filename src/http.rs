//! The HTTP client odl uses for its own small control-plane fetches.
//!
//! Release listings and checksum files are a few kilobytes of JSON and text —
//! too small to be worth the download manager's part-splitting machinery, but
//! still subject to whatever stands between the user and the internet. Both
//! callers ([`crate::self_update::plan`] and [`crate::ytdlp::install::plan`])
//! build the same client, so it is built in one place.
//!
//! Deliberately narrower than [`crate::download_manager`]'s own client: the
//! per-job knobs it applies — default headers, user agent, HTTP/2 window
//! tuning — describe how to fetch *the user's* file from *their* server, and
//! carrying them to api.github.com would send a stranger's headers somewhere
//! the user never asked to send them.

use reqwest::Client;

use crate::{config::DownloadOptions, error::OdlError};

/// A client honouring the network settings that apply everywhere: someone who
/// needs a proxy to reach the internet needs it here too, and the timeouts
/// they chose should not be silently overridden. The read timeout matters
/// most here: these fetches read a whole body in one call, with nothing
/// watching it, so a server that goes quiet mid-response would otherwise
/// hang the process for good.
pub(crate) fn client_for(net: &DownloadOptions) -> Result<Client, OdlError> {
    let mut builder = Client::builder();
    if net.no_proxy() {
        builder = builder.no_proxy();
    } else if let Some(proxy) = net.proxy_client_setting() {
        builder = builder.proxy(proxy);
    }
    if net.accept_invalid_certs() {
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(timeout) = net.connect_timeout() {
        builder = builder.connect_timeout(timeout);
    }
    if let Some(timeout) = net.read_timeout() {
        builder = builder.read_timeout(timeout);
    }
    builder.build().map_err(|e| OdlError::Other {
        message: format!("could not create an HTTP client: {e}"),
        origin: Box::new(e),
    })
}
