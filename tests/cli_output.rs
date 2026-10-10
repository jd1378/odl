//! Where `-o` puts a download. For a single URL it names the file, unless it
//! names a directory, which keeps the server's filename. For a list of URLs
//! it is the directory every file is saved in.

#![cfg(feature = "cli")]

use std::path::{MAIN_SEPARATOR, Path, PathBuf};
use std::process::{Command, Output, Stdio};

const BODY: &[u8] = b"output location end-to-end payload";

/// A server with `BODY` at `/report.bin`.
struct Served {
    url: String,
    // Declared before the server so they drop while it still runs.
    _mocks: Vec<mockito::Mock>,
    _server: mockito::ServerGuard,
}

fn serve() -> Served {
    let mut server = mockito::Server::new();
    let mocks = vec![
        server
            .mock("GET", "/report.bin")
            .match_header("range", "bytes=0-0")
            .with_status(206)
            .with_header("content-range", &format!("bytes 0-0/{}", BODY.len()))
            .with_header("accept-ranges", "bytes")
            .with_body("x")
            .create(),
        server
            .mock("GET", "/report.bin")
            .match_header(
                "range",
                mockito::Matcher::Exact(format!("bytes=0-{}", BODY.len() - 1)),
            )
            .with_status(206)
            .with_body(BODY)
            .create(),
    ];
    Served {
        url: format!("{}/report.bin", server.url()),
        _mocks: mocks,
        _server: server,
    }
}

/// Run `odl input -o output`, keeping its work and config under `work`.
fn odl(work: &Path, input: impl AsRef<std::ffi::OsStr>, output: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg(input)
        .arg("-o")
        .arg(output)
        .arg("--download-dir")
        .arg(work.join("data"))
        .arg("--config-file")
        .arg(work.join("config.toml"))
        .arg("--format")
        .arg("json")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn odl")
}

fn assert_succeeded(out: &Output) {
    assert!(
        out.status.success(),
        "odl exited {:?}: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn single_url_into_an_existing_directory_keeps_the_server_filename() {
    let served = serve();
    let work = tempfile::tempdir().unwrap();
    let save = work.path().join("save");
    std::fs::create_dir(&save).unwrap();

    let out = odl(work.path(), &served.url, &save);

    assert_succeeded(&out);
    assert_eq!(std::fs::read(save.join("report.bin")).unwrap(), BODY);
}

#[test]
fn single_url_into_a_path_ending_in_a_separator_creates_that_directory() {
    let served = serve();
    let work = tempfile::tempdir().unwrap();
    let dir = work.path().join("new");
    let output = PathBuf::from(format!("{}{MAIN_SEPARATOR}", dir.display()));

    let out = odl(work.path(), &served.url, &output);

    assert_succeeded(&out);
    assert_eq!(std::fs::read(dir.join("report.bin")).unwrap(), BODY);
}

#[test]
fn single_url_to_a_file_path_saves_under_that_name() {
    let served = serve();
    let work = tempfile::tempdir().unwrap();
    let file = work.path().join("custom.bin");

    let out = odl(work.path(), &served.url, &file);

    assert_succeeded(&out);
    assert_eq!(std::fs::read(&file).unwrap(), BODY);
    assert!(!work.path().join("report.bin").exists());
}

/// `\` separates paths only on Windows. Elsewhere it is a filename character
/// like any other, and a name that ends in one is still a file.
#[cfg(unix)]
#[test]
fn single_url_with_a_trailing_backslash_names_a_file_on_unix() {
    let served = serve();
    let work = tempfile::tempdir().unwrap();
    let file = work.path().join("name\\");

    let out = odl(work.path(), &served.url, &file);

    assert_succeeded(&out);
    assert_eq!(std::fs::read(&file).unwrap(), BODY);
}

#[test]
fn list_refuses_an_existing_file_as_its_directory() {
    let work = tempfile::tempdir().unwrap();
    let list = work.path().join("urls.txt");
    std::fs::write(&list, "https://example.invalid/a.bin\n").unwrap();
    let existing = work.path().join("existing.bin");
    std::fs::write(&existing, b"keep").unwrap();

    let out = odl(work.path(), &list, &existing);

    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("\"kind\":\"cli\""), "{stderr}");
    assert_eq!(std::fs::read(&existing).unwrap(), b"keep");
}
