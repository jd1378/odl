//! What a part request's status code costs the user.
//!
//! The retry policy is for transfers that fail in transit. A server that
//! answers correctly, with "no", is settled: retrying a 404 spends seconds of
//! backoff to reach the answer the first response already gave, and then
//! reports a retryable error class, which invites whatever runs odl to do it
//! all again.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const SIZE: usize = 4 * 1024 * 1024;

/// Serve a resumable file on `HEAD` but answer every part `GET` with `status`.
/// Returns odl's exit code and how long it spent before giving up.
fn refuse_every_part_with(status: usize) -> (Option<i32>, Duration, String) {
    let mut server = mockito::Server::new();
    let url = format!("{}/file", server.url());

    let _head = server
        .mock("HEAD", "/file")
        .with_status(200)
        .with_header("content-length", &SIZE.to_string())
        .with_header("accept-ranges", "bytes")
        .with_header("etag", "statusetag")
        .create();
    let _get = server
        .mock("GET", "/file")
        .expect_at_least(1)
        .with_status(status)
        .with_body("no")
        .create();

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
        .arg("4")
        .arg("--format")
        .arg("json")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .expect("failed to spawn odl binary");

    (
        output.status.code(),
        started.elapsed(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
    )
}

/// Exit 4 is the conflict class; 3 is the transient/network class scripts are
/// told to retry on. Which one a refusal lands in is the whole point here.
const CONFLICT: Option<i32> = Some(4);
const RETRYABLE: Option<i32> = Some(3);

#[test]
fn a_settled_refusal_fails_at_once_and_is_not_retryable() {
    for (status, expected) in [
        (404, CONFLICT), // gone
        (410, CONFLICT), // gone, emphatically
        (403, CONFLICT), // credentials will not improve on their own
        (401, CONFLICT), // same
        (416, CONFLICT), // our range no longer fits: not the same file
        (400, Some(1)),  // malformed request; not retryable, not a conflict
    ] {
        let (code, _, out) = refuse_every_part_with(status);
        assert_eq!(code, expected, "HTTP {status} classified wrong: {out}");
    }
}

#[test]
fn a_busy_server_is_still_retried() {
    // The other half of the contract: these must keep their retries and keep
    // reporting the class that means "come back later".
    for status in [429, 500, 503] {
        let (code, _, out) = refuse_every_part_with(status);
        assert_eq!(code, RETRYABLE, "HTTP {status} must stay retryable: {out}");
    }
}

#[test]
fn a_settled_refusal_does_not_spend_the_retry_budget() {
    // Measured against a transient status in the same run rather than a fixed
    // wall-clock bound, so a slow machine cannot make this flaky.
    let (_, terminal, _) = refuse_every_part_with(404);
    let (_, transient, _) = refuse_every_part_with(503);
    assert!(
        terminal * 2 < transient,
        "a 404 should cost far less than a 503: {terminal:?} vs {transient:?}"
    );
}
