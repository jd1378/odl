//! End-to-end verification for long URLs / filenames on Windows (Issue #3).
//!
//! Verifies that downloads with very long filenames (e.g. 300+ character PlantUML URLs)
//! successfully execute without triggering Windows MAX_PATH (260 char limit) errors.

use std::process::{Command, Stdio};

const BODY: &[u8] = b"plantuml image mock payload";
const LONG_SEGMENT: &str = "ROwzJiD048JxVOfLUhz02YinXee29O9-EGzmqkNEsTr3ojiZd_bHKDhv-MPsvg9UJuaaU55-DYZDeXv3d2KxR_RLF_W8_Om16nRZHRYEZEBoAhRQuq2qKBZhMtJBZ-KzPZxWN65EZTVrF0vRVe76jmlVAIPaZvACne6xtsRZBFJlLjStwGzfSya68adEnne2p8YP-Vh0lXdyfCKH7DDIB5K3MlOV-iTRTv4C_20nLSKDyW6kb_NDBTQzQb52dcY7FDLJ-W80.png";

#[test]
fn long_url_download_succeeds_without_max_path_error() {
    let mut server = mockito::Server::new();
    let url = format!("{}/plantuml/png/{}", server.url(), LONG_SEGMENT);

    let _head = server
        .mock("HEAD", format!("/plantuml/png/{}", LONG_SEGMENT).as_str())
        .with_status(200)
        .with_header("content-length", &BODY.len().to_string())
        .with_header("accept-ranges", "bytes")
        .create();
    let _get = server
        .mock("GET", format!("/plantuml/png/{}", LONG_SEGMENT).as_str())
        .with_status(206)
        .with_header(
            "content-range",
            &format!("bytes 0-{}/{}", BODY.len() - 1, BODY.len()),
        )
        .with_body(BODY)
        .create();

    let data_dir = tempfile::tempdir().unwrap();
    let save_dir = tempfile::tempdir().unwrap();

    let status = Command::new(env!("CARGO_BIN_EXE_odl"))
        .current_dir(save_dir.path())
        .arg(&url)
        .arg("--download-dir")
        .arg(data_dir.path())
        .arg("--format")
        .arg("json")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to spawn odl binary");

    assert_eq!(status.code(), Some(0), "download with long URL must succeed");

    let entries: Vec<_> = std::fs::read_dir(save_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(entries.len(), 1, "expected 1 saved file in save_dir");

    // Also check that the data_dir metadata directory name was properly bounded
    let metadata_dirs: Vec<_> = std::fs::read_dir(data_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    for dir_entry in metadata_dirs {
        let name = dir_entry.file_name().to_string_lossy().into_owned();
        assert!(name.len() <= 49, "internal metadata dir name '{}' too long (len {})", name, name.len());
    }
}
