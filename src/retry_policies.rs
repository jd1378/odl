use reqwest_retry::{self, RetryDecision, RetryPolicy};
use std::{
    cmp,
    time::{Duration, SystemTime},
};

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
