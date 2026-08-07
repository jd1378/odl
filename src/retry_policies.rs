use crate::progress::{DownloadContext, ProgressEvent};
use reqwest_retry::{self, RetryDecision, RetryPolicy};
use std::{
    cmp,
    time::{Duration, SystemTime},
};
use tokio::time::{self, Instant};

/// Calculate exponential using base and number of past retries
fn calculate_exponential(base: u32, n_past_retries: u32) -> u32 {
    base.checked_pow(n_past_retries).unwrap_or(u32::MAX)
}

/// for a max_n_retries of 6 and n_fixed_retries of 3
/// and a wait_time of 500ms
///
/// wait times will be (ms):
///
/// 500, 500, 500, 1000, 2000, 4000
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FixedThenExponentialRetry {
    /// Maximum number of allowed retries attempts.
    pub max_n_retries: u32,
    /// Fixed wait time between retries. Also expo base 2 is multiplied by this amount.
    pub wait_time: Duration,
    /// times after which we start backing off exponentially.
    /// must be smaller or equal to max_n_retries, otherwise max_n_retries will be used
    pub n_fixed_retries: u32,
}

impl FixedThenExponentialRetry {
    fn too_many_attempts(&self, n_past_retries: u32) -> bool {
        n_past_retries >= self.max_n_retries
    }
}

impl Default for FixedThenExponentialRetry {
    fn default() -> Self {
        Self {
            max_n_retries: 6,
            wait_time: Duration::from_millis(500),
            n_fixed_retries: 3,
        }
    }
}

impl RetryPolicy for FixedThenExponentialRetry {
    fn should_retry(
        &self,
        _request_start_time: SystemTime,
        n_past_retries: u32,
    ) -> reqwest_retry::RetryDecision {
        if self.too_many_attempts(n_past_retries) {
            RetryDecision::DoNotRetry
        } else {
            let wait_time = if n_past_retries < cmp::min(self.n_fixed_retries, self.max_n_retries) {
                self.wait_time
            } else {
                let exp = calculate_exponential(2, n_past_retries - self.n_fixed_retries + 1);
                self.wait_time * exp
            };
            let execute_after = SystemTime::now() + wait_time;
            RetryDecision::Retry { execute_after }
        }
    }
}

/// Longest `Retry-After` odl will actually sit out.
///
/// The header is the server's instruction, but it is also attacker- or
/// bug-supplied: an unbounded value parks a download for as long as the
/// sender likes. Waiting is still interruptible, so this caps the damage
/// rather than second-guessing a reasonable value.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

/// Parse a `Retry-After` value: either delta-seconds or an HTTP-date.
///
/// A date already in the past yields `Duration::ZERO` — retry now — rather
/// than being discarded, which is what the server asked for.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    let value = value.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let when = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    let delta = when.timestamp() - chrono::Utc::now().timestamp();
    Some(Duration::from_secs(delta.max(0) as u64))
}

/// Consult the retry policy after a failed attempt. If retry is allowed,
/// sleeps until the scheduled retry time while emitting countdown
/// [`ProgressEvent::Message`] events on `ctx`. Returns `true` if caller
/// should retry, `false` if no further retries are allowed.
///
/// `false` conflates two different endings: the budget is spent, or the wait
/// was interrupted by cancellation. They need different errors — a stopped
/// download reported as a failure is one a caller may auto-restart — so every
/// caller must check [`DownloadContext::is_cancelled`] before deciding what
/// `false` meant. The return type cannot say so itself without breaking the
/// published signature.
///
/// `attempts_so_far` is the number of attempts already made (>= 1 after a
/// failure). The policy is queried with `attempts_so_far - 1` as
/// `n_past_retries`. The wait is interrupted early if `ctx` is cancelled,
/// in which case this returns `false`.
pub async fn wait_for_retry(
    policy: &FixedThenExponentialRetry,
    attempts_so_far: u32,
    ctx: &DownloadContext,
    ulid: Option<&str>,
    retry_after: Option<Duration>,
) -> bool {
    let n_past = attempts_so_far.saturating_sub(1);
    match policy.should_retry(SystemTime::now(), n_past) {
        RetryDecision::Retry { execute_after } => {
            // The policy still decides *whether* to retry; `Retry-After` only
            // decides when. A server that says to come back in a minute knows
            // something odl's backoff curve does not, and racing it earns
            // another refusal — but the attempt budget stays the caller's.
            let server_requested = retry_after.is_some();
            let wait = match retry_after {
                Some(d) => d.min(MAX_RETRY_AFTER),
                None => execute_after
                    .duration_since(SystemTime::now())
                    .unwrap_or_default(),
            };
            ctx.emit(ProgressEvent::RetryScheduled {
                ulid: ulid.map(str::to_owned),
                attempt: attempts_so_far,
                max_attempts: policy.max_n_retries,
                delay: wait,
                server_requested,
            });

            let sleep = time::sleep(wait);
            tokio::pin!(sleep);
            let start = Instant::now();
            let mut last_msg = String::new();

            loop {
                let remaining = wait.checked_sub(start.elapsed()).unwrap_or_default();
                let msg = format!(
                    " Retrying {}/{} in {}{}",
                    attempts_so_far,
                    policy.max_n_retries,
                    format_wait(remaining),
                    if server_requested {
                        " (server asked us to wait)"
                    } else {
                        ""
                    }
                );
                // Only emit when the rendered countdown text actually
                // changes; avoids flooding the reporter queue with
                // identical messages when N parts are retrying together.
                if msg != last_msg {
                    ctx.emit(ProgressEvent::Message(msg.clone()));
                    last_msg = msg;
                }

                tokio::select! {
                    _ = &mut sleep => break,
                    _ = ctx.cancel.cancelled() => return false,
                    _ = time::sleep(Duration::from_millis(200)) => {},
                }
            }

            ctx.emit(ProgressEvent::Message(String::new()));
            true
        }
        RetryDecision::DoNotRetry => false,
    }
}

/// Format a `Duration` compactly for retry countdown display.
pub fn format_wait(dur: Duration) -> String {
    let total_secs = dur.as_secs();
    if total_secs <= 60 {
        return format!("{:.1}s", dur.as_secs_f32());
    }
    if total_secs < 3600 {
        let mins = total_secs / 60;
        let secs = total_secs % 60;
        return format!("{}m {}s", mins, secs);
    }
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    if mins > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}h", hours)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::distr::{Distribution, Uniform};

    fn get_retry_policy() -> FixedThenExponentialRetry {
        FixedThenExponentialRetry {
            max_n_retries: 6,
            wait_time: Duration::from_millis(500),
            n_fixed_retries: 3,
        }
    }

    #[test]
    fn if_n_past_retries_is_below_maximum_it_decides_to_retry() {
        // Arrange
        let policy = get_retry_policy();
        let n_past_retries = Uniform::new(0, policy.max_n_retries)
            .unwrap()
            .sample(&mut rand::rng());
        assert!(n_past_retries < policy.max_n_retries);

        // Act
        let decision = policy.should_retry(SystemTime::now(), n_past_retries);

        // Assert
        matches!(decision, RetryDecision::Retry { .. });
    }

    #[test]
    fn if_n_past_retries_is_above_maximum_it_decides_to_mark_as_failed() {
        // Arrange
        let policy = get_retry_policy();
        let n_past_retries = Uniform::new(policy.max_n_retries, u32::MAX)
            .unwrap()
            .sample(&mut rand::rng());
        assert!(n_past_retries >= policy.max_n_retries);

        // Act
        let decision = policy.should_retry(SystemTime::now(), n_past_retries);

        // Assert
        matches!(decision, RetryDecision::DoNotRetry);
    }

    #[test]
    fn fixed_wait_time_is_used_for_initial_retries() {
        let policy = get_retry_policy();
        let tolerance = Duration::from_millis(10);
        for n_past_retries in 0..policy.n_fixed_retries {
            let before = SystemTime::now();
            let decision = policy.should_retry(before, n_past_retries);
            if let RetryDecision::Retry { execute_after } = decision {
                let duration = execute_after.duration_since(before).unwrap();
                let diff = duration.abs_diff(policy.wait_time);
                assert!(
                    diff <= tolerance,
                    "n_past_retries={}, expected {:?}, got {:?}, diff {:?}",
                    n_past_retries,
                    policy.wait_time,
                    duration,
                    diff
                );
            } else {
                panic!("Expected Retry, got {:?}", decision);
            }
        }
    }

    #[test]
    fn exponential_backoff_is_used_after_fixed_retries() {
        let policy = get_retry_policy();
        let base = 2;
        let tolerance = Duration::from_millis(10);
        for n_past_retries in policy.n_fixed_retries..policy.max_n_retries {
            let before = SystemTime::now();
            let exp: u32 = calculate_exponential(base, n_past_retries - policy.n_fixed_retries + 1);
            let expected = policy.wait_time * exp;
            let decision = policy.should_retry(before, n_past_retries);
            if let RetryDecision::Retry { execute_after } = decision {
                let duration = execute_after.duration_since(before).unwrap();
                let diff = duration.abs_diff(expected);
                assert!(
                    diff <= tolerance,
                    "n_past_retries={}, expected {:?}, got {:?}, diff {:?}",
                    n_past_retries,
                    expected,
                    duration,
                    diff
                );
            } else {
                panic!("Expected Retry, got {:?}", decision);
            }
        }
    }

    #[test]
    fn does_not_retry_when_n_past_retries_equals_max() {
        let policy = get_retry_policy();
        let n_past_retries = policy.max_n_retries;
        let decision = policy.should_retry(SystemTime::now(), n_past_retries);
        assert!(matches!(decision, RetryDecision::DoNotRetry));
    }

    #[test]
    fn calculate_exponential_handles_overflow() {
        let max = calculate_exponential(u32::MAX, 2);
        assert_eq!(max, u32::MAX);
    }

    #[test]
    fn wait_times_match_example() {
        let policy = get_retry_policy();
        let expected_waits = [500, 500, 500, 1000, 2000, 4000];
        let tolerance = Duration::from_millis(10);
        for (n_past_retries, &expected_ms) in expected_waits.iter().enumerate() {
            let before = SystemTime::now();
            let decision = policy.should_retry(before, n_past_retries as u32);
            if let RetryDecision::Retry { execute_after } = decision {
                let duration = execute_after.duration_since(before).unwrap();
                let expected = Duration::from_millis(expected_ms);
                let diff = duration.abs_diff(expected);
                assert!(
                    diff <= tolerance,
                    "n_past_retries={}, expected {:?}, got {:?}, diff {:?}",
                    n_past_retries,
                    expected,
                    duration,
                    diff
                );
            } else {
                panic!("Expected Retry, got {:?}", decision);
            }
        }
    }
}

#[cfg(test)]
mod retry_after_tests {
    use super::*;

    #[test]
    fn delta_seconds_is_taken_literally() {
        assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after("  30 "), Some(Duration::from_secs(30)));
        assert_eq!(parse_retry_after("0"), Some(Duration::ZERO));
    }

    #[test]
    fn an_http_date_becomes_the_time_until_it() {
        let soon = chrono::Utc::now() + chrono::Duration::seconds(90);
        let parsed = parse_retry_after(&soon.to_rfc2822()).expect("a date is a valid value");
        // Bounded rather than exact: the clock moves between the two calls.
        assert!(
            parsed.as_secs() >= 85 && parsed.as_secs() <= 90,
            "got {parsed:?}"
        );
    }

    #[test]
    fn a_date_already_past_means_now_rather_than_nothing() {
        // Retrying immediately is what the server asked for; discarding the
        // header would instead impose odl's own backoff.
        let past = chrono::Utc::now() - chrono::Duration::seconds(60);
        assert_eq!(parse_retry_after(&past.to_rfc2822()), Some(Duration::ZERO));
    }

    #[test]
    fn nonsense_is_ignored_so_the_configured_backoff_applies() {
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("soon"), None);
        assert_eq!(parse_retry_after("-5"), None);
    }

    #[test]
    fn an_absurd_value_is_capped_rather_than_obeyed() {
        // The header is server-supplied, so an unbounded one parks the
        // download for as long as the sender likes.
        let huge = parse_retry_after("999999999").expect("parses");
        assert!(huge > MAX_RETRY_AFTER);
        assert_eq!(huge.min(MAX_RETRY_AFTER), MAX_RETRY_AFTER);
    }
}
