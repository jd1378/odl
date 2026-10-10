//! The request that finds out what a URL serves before any part is fetched.
//!
//! It is a `GET` for the first byte, not a `HEAD`. A probe exists to predict
//! what parts will get, and parts send `GET`: a server can route the two
//! methods differently, and some do. github.com redirects a signed-in `HEAD`
//! for a release asset to a host that answers 401, and the same `GET` to one
//! that serves the file. Others refuse `HEAD` outright, or sign links for one
//! method only. Asking the way parts ask costs the same round trip, and a 206
//! shows that ranges work instead of taking `Accept-Ranges` on trust.

use http::header::{ACCEPT_ENCODING, HeaderValue, RANGE, USER_AGENT};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use url::Url;

use crate::credentials::Credentials;
use crate::error::OdlError;
use crate::progress::DownloadContext;
use crate::retry_policies::{
    FixedThenExponentialRetry, StatusVerdict, classify_status, retry_after, wait_for_retry,
};
use crate::user_agents::random_user_agent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeMethod {
    /// `GET` for the first byte.
    Range,
    /// `GET` without a range, for a resource whose first byte is out of range:
    /// an empty file answers `bytes=0-0` with 416.
    Whole,
}

/// What every probe request carries.
#[derive(Clone, Copy)]
pub(crate) struct ProbeRequest<'a> {
    pub client: &'a Client,
    pub url: &'a Url,
    pub credentials: Option<&'a Credentials>,
    pub randomize_user_agent: bool,
}

impl ProbeRequest<'_> {
    fn build(&self, method: ProbeMethod) -> RequestBuilder {
        let mut req = self.client.get(self.url.clone());
        if method == ProbeMethod::Range {
            req = req.header(RANGE, HeaderValue::from_static("bytes=0-0"));
        }
        // A body reqwest decompresses comes back without its `Content-Length`,
        // and a compressed range counts compressed bytes. Parts ask for
        // identity for the same reasons.
        req = req
            .header(ACCEPT_ENCODING, HeaderValue::from_static("identity"))
            // Asked for in case the server can give the file's digest, which
            // the assembled file is checked against.
            .header(
                "Want-Repr-Digest",
                "sha-512=9, sha-384=8, sha-256=7, sha-1=1, md5=1",
            )
            .header(
                "Want-Content-Digest",
                "sha-512=9, sha-384=8, sha-256=7, sha-1=1, md5=1",
            );
        if let Some(creds) = self.credentials {
            req = req.basic_auth(creds.username(), creds.password());
        }
        if self.randomize_user_agent {
            req = req.header(USER_AGENT, random_user_agent());
        }
        req
    }

    /// Send one probe, asking for the whole file if the first byte is out of
    /// range. Returns the last response, error status or not, and the method
    /// that got it. Only the head is read: dropping the response abandons the
    /// body, which matters for a server that ignores `Range` and starts
    /// sending the whole file.
    async fn send(
        &self,
        mut method: ProbeMethod,
    ) -> Result<(Response, ProbeMethod), reqwest::Error> {
        loop {
            let resp = self.build(method).send().await?;
            if method == ProbeMethod::Range && resp.status() == StatusCode::RANGE_NOT_SATISFIABLE {
                method = ProbeMethod::Whole;
                continue;
            }
            return Ok((resp, method));
        }
    }

    /// The evaluate probe: [`Self::send`] under the retry policy, with
    /// refusals judged the way part requests judge them.
    pub(crate) async fn run(
        &self,
        policy: &FixedThenExponentialRetry,
        ctx: &DownloadContext,
    ) -> Result<Response, OdlError> {
        // Kept across attempts: a resource found to be empty is not asked for
        // its first byte again after a transient failure.
        let mut method = ProbeMethod::Range;
        let mut attempts: u32 = 0;
        loop {
            let (cause, wait_hint) = match self.send(method).await {
                Ok((r, _)) if !is_refusal(r.status()) => return Ok(r),
                Ok((r, used)) => {
                    method = used;
                    // A server that has settled the matter is not asked again.
                    match classify_status(r.status(), r.url()) {
                        StatusVerdict::Terminal(cause) => return Err(cause),
                        StatusVerdict::Transient(cause) => (cause, retry_after(&r)),
                    }
                }
                Err(e) => (OdlError::from_reqwest(e), None),
            };
            attempts = attempts.saturating_add(1);
            if !wait_for_retry(policy, attempts, ctx, None, wait_hint).await {
                // `false` also means the wait was cancelled, and a stopped
                // download must not report the network error that happened
                // to precede the stop.
                if ctx.is_cancelled() {
                    return Err(OdlError::Cancelled);
                }
                return Err(cause);
            }
        }
    }
}

/// Whether `status` answers the probe with "no". Statuses past 599 are not
/// HTTP's and describe no file either: GitHub's asset host answers an expired
/// link with 618.
fn is_refusal(status: StatusCode) -> bool {
    status.is_client_error() || status.is_server_error() || status.as_u16() >= 600
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_past_the_http_range_are_refusals() {
        assert!(is_refusal(StatusCode::from_u16(618).unwrap()));
        assert!(is_refusal(StatusCode::UNAUTHORIZED));
        assert!(is_refusal(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_refusal(StatusCode::PARTIAL_CONTENT));
        assert!(!is_refusal(StatusCode::OK));
    }
}
