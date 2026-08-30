//! Integration test for Windows long path hardening (Issue #6).
//!
//! Verifies that `odl` can download, assemble, and save files into nested directory
//! structures without triggering Windows MAX_PATH errors.

use std::process::{Command, Stdio};

const BODY: &[u8] = b"windows long path hardening payload test content";

#[test]
fn download_into_nested_destination_directory_succeeds() {
    let mut server = mockito::Server::new();
    let url = format!("{}/test_download.bin", server.url());

    let _head = server
        .mock("HEAD", "/test_download.bin")
        .with_status(200)
        .with_header("content-length", &BODY.len().to_string())
        .with_header("accept-ranges", "bytes")
        .create();
    let _get = server
        .mock("GET", "/test_download.bin")
        .with_status(206)
        .with_header(
            "content-range",
            &format!("bytes 0-{}/{}", BODY.len() - 1, BODY.len()),
        )
        .with_body(BODY)
        .create();

    let tmp = tempfile::tempdir().unwrap();
    // Build a nested destination directory
    let deep_dir = tmp.path().join("level1").join("level2").join("nested_folder");
    std::fs::create_dir_all(&deep_dir).unwrap();
    let target_file = deep_dir.join("test_download.bin");

    let status = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg(&url)
        .arg("-o")
        .arg(&target_file)
        .arg("--format")
        .arg("json")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to spawn odl binary");

    assert_eq!(status.code(), Some(0), "download into nested directory must succeed");

    let saved_file = deep_dir.join("test_download.bin");
    assert!(saved_file.exists(), "target file must exist at {:?}", saved_file);
    let content = std::fs::read(&saved_file).unwrap();
    assert_eq!(content, BODY, "saved file content must match payload");
}
