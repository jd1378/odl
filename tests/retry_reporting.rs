//! A wait the caller can see.
//!
//! A download that pauses to retry looks identical to one that has hung
//! unless something says how long the pause is. `retry_scheduled` carries
//! that, and says whether the delay is odl's backoff or the server's own
//! `Retry-After` — which a UI can phrase differently and a caller cannot
//! shorten.

use std::process::{Command, Stdio};
use std::time::Instant;

const SIZE: usize = 4 * 1024 * 1024;

/// Refuse every part `GET` with `status`, optionally advertising
/// `Retry-After`, and return odl's JSON stream plus how long the run took.
fn refuse_with(status: usize, retry_after: Option<&str>) -> (String, f64) {
    let mut server = mockito::Server::new();
    let url = format!("{}/file", server.url());

    let _head = server
        .mock("HEAD", "/file")
        .with_status(200)
        .with_header("content-length", &SIZE.to_string())
        .with_header("accept-ranges", "bytes")
        .with_header("etag", "retryetag")
        .create();
    let mut get = server
        .mock("GET", "/file")
        .expect_at_least(1)
        .with_status(status)
        .with_body("later");
    if let Some(v) = retry_after {
        get = get.with_header("retry-after", v);
    }
    let _get = get.create();

    let data_dir = tempfile::tempdir().unwrap();
    let save_dir = tempfile::tempdir().unwrap();

    let started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg(&url)
        .arg("-o")
        .arg(save_dir.path().join("file"))
        .arg("--download-dir")
        .arg(data_dir.path())
        .arg("--max-connections")
        .arg("1")
        .arg("--max-retries")
        .arg("1")
        .arg("--wait-between-retries")
        .arg("5s")
        .arg("--format")
        .arg("json")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .expect("failed to spawn odl binary");

    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        started.elapsed().as_secs_f64(),
    )
}

fn retry_lines(out: &str) -> Vec<serde_json::Value> {
    out.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|v| v["type"] == "retry_scheduled")
        .collect()
}

#[test]
fn a_scheduled_retry_reports_how_long_the_wait_is() {
    let (out, _) = refuse_with(503, None);
    let events = retry_lines(&out);
    let first = events.first().expect("expected a retry_scheduled event");

    // The whole point: a number, not a sentence to scrape out of a log line.
    // Not exactly 5000 — the delay is measured from the deadline the policy
    // set, so a sliver is already gone by the time it is reported.
    let delay = first["delay_ms"].as_u64().expect("delay_ms");
    assert!((4900..=5000).contains(&delay), "{first}");
    assert_eq!(first["attempt"].as_u64(), Some(1), "{first}");
    assert_eq!(first["max_attempts"].as_u64(), Some(1), "{first}");
    assert_eq!(first["server_requested"].as_bool(), Some(false), "{first}");
    assert!(
        first["part"].is_string(),
        "a part retry should name its part: {first}"
    );
}

#[test]
fn a_servers_retry_after_sets_the_wait_and_is_marked_as_theirs() {
    // Configured backoff is 5s; the server asks for 1. Honouring it is both
    // faster here and the only way to avoid racing a rate limit when the
    // server asks for longer.
    let (out, elapsed) = refuse_with(503, Some("1"));
    let events = retry_lines(&out);
    let first = events.first().expect("expected a retry_scheduled event");

    assert_eq!(first["delay_ms"].as_u64(), Some(1000), "{first}");
    assert_eq!(
        first["server_requested"].as_bool(),
        Some(true),
        "the caller must be able to tell whose delay this is: {first}"
    );
    assert!(
        elapsed < 4.0,
        "the server's 1s should have replaced the configured 5s, took {elapsed:.1}s"
    );
}

#[test]
fn a_settled_refusal_schedules_no_wait_at_all() {
    // 404 is terminal, so there is nothing to wait for and nothing to report.
    let (out, _) = refuse_with(404, None);
    assert!(
        retry_lines(&out).is_empty(),
        "a settled refusal must not announce a retry: {out}"
    );
}
