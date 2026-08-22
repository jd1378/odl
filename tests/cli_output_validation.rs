//! End-to-end regression tests for output path (-o) validation in single-URL and batch modes.

use std::fs::File;
use std::io::Write;
use std::process::Command;

#[test]
fn single_url_with_existing_dir_returns_exit_2_and_cli_error() {
    let tmp_dir = tempfile::tempdir().unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg("https://example.com/file.txt")
        .arg("-o")
        .arg(tmp_dir.path())
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");

    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit code 2 for existing directory passed to -o in single-URL mode"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"kind\":\"cli\""),
        "expected error kind to be 'cli', got: {stderr}"
    );
    assert!(
        stderr.contains("\"exit_code\":2"),
        "expected error exit_code to be 2, got: {stderr}"
    );
    assert!(
        stderr.contains("is a directory"),
        "expected error message to mention directory, got: {stderr}"
    );
}

#[test]
fn single_url_with_trailing_slash_returns_exit_2() {
    let output = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg("https://example.com/file.txt")
        .arg("-o")
        .arg("some_nonexistent_dir/")
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");

    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit code 2 for trailing slash in single-URL mode"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"kind\":\"cli\""),
        "expected error kind to be 'cli', got: {stderr}"
    );
    assert!(
        stderr.contains("\"exit_code\":2"),
        "expected error exit_code to be 2, got: {stderr}"
    );
}

#[test]
fn single_url_with_trailing_backslash_returns_exit_2() {
    let output = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg("https://example.com/file.txt")
        .arg("-o")
        .arg("some_nonexistent_dir\\")
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");

    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit code 2 for trailing backslash in single-URL mode"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"kind\":\"cli\""),
        "expected error kind to be 'cli', got: {stderr}"
    );
    assert!(
        stderr.contains("\"exit_code\":2"),
        "expected error exit_code to be 2, got: {stderr}"
    );
}

#[test]
fn file_list_with_existing_file_as_output_returns_exit_2() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let list_file_path = tmp_dir.path().join("urls.txt");
    let mut list_file = File::create(&list_file_path).unwrap();
    writeln!(list_file, "https://example.com/1.txt").unwrap();

    let existing_file_path = tmp_dir.path().join("existing.bin");
    File::create(&existing_file_path).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg(&list_file_path)
        .arg("-o")
        .arg(&existing_file_path)
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");

    assert_eq!(
        output.status.code(),
        Some(2),
        "expected exit code 2 when passing existing file as -o in file-list mode"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"kind\":\"cli\""),
        "expected error kind to be 'cli', got: {stderr}"
    );
    assert!(
        stderr.contains("\"exit_code\":2"),
        "expected error exit_code to be 2, got: {stderr}"
    );
}
