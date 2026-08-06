//! End-to-end tests for the `--checksum` CLI flag: spawn the built `odl`
//! binary against a mock HTTP server and assert the documented exit codes
//! (0 on match, 4 on mismatch, 2 on malformed input).

use std::process::{Command, Stdio};

use sha2::{Digest, Sha256};

const FILE_CONTENT: &[u8] = b"checksum end-to-end test payload";

/// Stand up a mock server that serves `FILE_CONTENT` at `/file`, run the
/// `odl` binary with the given `--checksum` argument, and return its exit
/// code. The downloaded file is written into an isolated temp dir.
fn run_with_checksum(checksum_arg: &str) -> i32 {
    run_with_checksum_args(checksum_arg, &[])
}

fn run_with_checksum_args(checksum_arg: &str, extra: &[&str]) -> i32 {
    let mut server = mockito::Server::new();
    let url = format!("{}/file", server.url());

    let head = server
        .mock("HEAD", "/file")
        .with_status(200)
        .with_header("content-length", &FILE_CONTENT.len().to_string())
        .with_header("accept-ranges", "bytes")
        .with_header("etag", "e2eetag")
        .with_header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
        .create();

    let get = server
        .mock("GET", "/file")
        .match_header(
            "range",
            mockito::Matcher::Exact(format!("bytes=0-{}", FILE_CONTENT.len() - 1)),
        )
        .with_status(206)
        .with_body(FILE_CONTENT)
        .create();

    let data_dir = tempfile::tempdir().unwrap();
    let save_dir = tempfile::tempdir().unwrap();
    let out = save_dir.path().join("file");

    let status = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg(&url)
        .arg("-o")
        .arg(&out)
        .arg("--download-dir")
        .arg(data_dir.path())
        .arg("--checksum")
        .arg(checksum_arg)
        .arg("--format")
        .arg("json")
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to spawn odl binary");

    head.assert();
    get.assert();

    status.code().expect("process terminated by signal")
}

fn correct_sha256_hex() -> String {
    let mut hasher = Sha256::new();
    hasher.update(FILE_CONTENT);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[test]
fn cli_checksum_match_succeeds() {
    let code = run_with_checksum(&format!("sha256:{}", correct_sha256_hex()));
    assert_eq!(code, 0, "matching checksum should exit 0");
}

#[test]
fn cli_checksum_mismatch_exits_conflict() {
    let wrong = "sha256:".to_string() + &"00".repeat(32);
    let code = run_with_checksum(&wrong);
    assert_eq!(code, 4, "checksum mismatch should exit 4 (conflict)");
}

#[test]
fn cli_checksum_malformed_exits_usage() {
    // Malformed input is rejected before any network access, so no mock
    // server is needed here.
    let status = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg("https://example.invalid/file")
        .arg("--checksum")
        .arg("sha256:not-hex")
        .arg("--format")
        .arg("json")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to spawn odl binary");
    assert_eq!(
        status.code(),
        Some(2),
        "malformed checksum should exit 2 (usage)"
    );
}

#[test]
fn cli_checksum_is_not_enforced_when_verification_is_off() {
    // The caller takes verification over: odl records what it was told and
    // downloads, rather than hashing the file to act on it.
    let code = run_with_checksum_args(
        &("sha256:".to_string() + &"00".repeat(32)),
        &["--no-verify-checksums"],
    );
    assert_eq!(
        code, 0,
        "a mismatch must not fail a download that opted out"
    );
}
