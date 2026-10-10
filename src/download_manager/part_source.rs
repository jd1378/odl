//! Where parts fetch their bytes from.
//!
//! The probe follows redirects, and parts go straight to where they led. That
//! address is often a signed link that expires: GitHub's release asset links
//! last five minutes, a presigned S3 or Azure link as long as its signer chose.
//! A part that reconnects or is split off after that is refused for good, by
//! a server that would hand out a fresh link for the URL the caller asked for.

use std::sync::Mutex;

use reqwest::{Client, StatusCode};
use url::Url;

use crate::conflict::ServerConflict;
use crate::credentials::Credentials;
use crate::download::Download;
use crate::download_manager::probe::ProbeRequest;
use crate::error::{ConflictError, OdlError};
use crate::response_info::ResponseInfo;

pub(crate) struct PartSource {
    /// The URL parts fetch, and how many times it has been renewed.
    current: Mutex<(Url, u64)>,
    /// Held while a renewal is in flight, so parts refused together renew once.
    renewing: tokio::sync::Mutex<()>,
    renewal: Option<Renewal>,
}

/// What a renewal asks, and what it must find to be trusted.
struct Renewal {
    /// Carries the caller's own headers, like the probe's client: reqwest
    /// drops the origin-bound ones if a redirect leaves the origin.
    client: Client,
    requested: Url,
    credentials: Option<Credentials>,
    randomize_user_agent: bool,
    size: Option<u64>,
    etag: Option<String>,
    last_modified: Option<i64>,
}

impl PartSource {
    /// Parts fetch `instruction`'s URL, and it is never renewed.
    pub(crate) fn fixed(instruction: &Download) -> Self {
        Self {
            current: Mutex::new((instruction.url().clone(), 0)),
            renewing: tokio::sync::Mutex::new(()),
            renewal: None,
        }
    }

    /// Like [`Self::fixed`], but when the probe was redirected, a refused
    /// link is replaced by asking the requested URL again through `client`.
    pub(crate) fn renewable(
        instruction: &Download,
        client: Client,
        randomize_user_agent: bool,
    ) -> Self {
        // Asking an unredirected URL again only gets the same URL back.
        let redirected = instruction.requested_url() != instruction.url();
        Self {
            renewal: redirected.then(|| Renewal {
                client,
                requested: instruction.requested_url().clone(),
                credentials: instruction.credentials().cloned(),
                randomize_user_agent,
                size: instruction.size(),
                etag: instruction.etag().map(str::to_owned),
                last_modified: instruction.last_modified(),
            }),
            ..Self::fixed(instruction)
        }
    }

    /// The URL to fetch, and its generation for [`Self::renew`].
    pub(crate) fn current(&self) -> (Url, u64) {
        self.current.lock().unwrap().clone()
    }

    /// Replace the link of generation `seen`, refused with a status for
    /// which [`may_have_expired`] holds.
    ///
    /// `Ok(true)` means [`Self::current`] has a newer link to try, possibly
    /// one another part already fetched. `Ok(false)` means there is none and
    /// the refusal stands. A fresh link is used only on the origin of the old
    /// one, where the part's client already sends the right headers, and only
    /// for the same file: one the server now describes differently is a
    /// [`ServerConflict::FileChanged`], since the parts on disk are of the
    /// file it was.
    pub(crate) async fn renew(&self, seen: u64) -> Result<bool, OdlError> {
        let Some(renewal) = &self.renewal else {
            return Ok(false);
        };
        let _renewing = self.renewing.lock().await;
        let (stale, generation) = self.current();
        if generation != seen {
            return Ok(true);
        }
        let probe = ProbeRequest {
            client: &renewal.client,
            url: &renewal.requested,
            credentials: renewal.credentials.as_ref(),
            randomize_user_agent: renewal.randomize_user_agent,
        };
        let resp = match probe.once().await {
            Ok(Some(resp)) => resp,
            Ok(None) | Err(_) => return Ok(false),
        };
        let info = ResponseInfo::from_response(renewal.requested.clone(), resp);
        let fresh = info.url();
        if fresh == &stale || fresh.origin() != stale.origin() {
            tracing::warn!("renewing the redirect target gave no link parts can use");
            return Ok(false);
        }
        if renewal.describes_another_file(&info) {
            return Err(OdlError::Conflict(ConflictError::Server {
                conflict: ServerConflict::FileChanged,
            }));
        }
        tracing::info!("redirect target refused parts; renewed it");
        *self.current.lock().unwrap() = (fresh.clone(), generation + 1);
        Ok(true)
    }
}

impl Renewal {
    fn describes_another_file(&self, info: &ResponseInfo) -> bool {
        fn differ<T: PartialEq>(was: Option<T>, now: Option<T>) -> bool {
            matches!((was, now), (Some(was), Some(now)) if was != now)
        }
        differ(self.size, info.total_length())
            || differ(self.etag.clone(), info.etag())
            || differ(self.last_modified, info.parse_last_modified())
    }
}

/// Whether `status` is how a link that has expired could be refused.
/// Signing schemes disagree on the code (S3 and Azure send 403, GCS 400,
/// some CDNs 404 or 410, GitHub's asset host 618), so every refusal of the
/// request itself counts. Not "later" statuses, which the retry policy
/// handles, nor 416, which is about the range, nor 407, which is the proxy's.
pub(crate) fn may_have_expired(status: StatusCode) -> bool {
    match status.as_u16() {
        407 | 408 | 416 | 425 | 429 => false,
        code => (400..500).contains(&code) || code >= 600,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_refusal_of_the_link_itself_is_renewed() {
        for code in [400, 401, 403, 404, 410, 618] {
            assert!(
                may_have_expired(StatusCode::from_u16(code).unwrap()),
                "{code}"
            );
        }
        for code in [407, 408, 416, 425, 429, 500, 503] {
            assert!(
                !may_have_expired(StatusCode::from_u16(code).unwrap()),
                "{code}"
            );
        }
    }
}
