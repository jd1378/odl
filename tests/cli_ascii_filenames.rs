//! `--ascii-filenames` end to end: the name odl actually writes to disk is the
//! transliterated one, and the default is left byte-identical.

use std::process::{Command, Stdio};

const BODY: &[u8] = b"ascii filename test payload";
const NAME: &str = "Café Münster 日本語.bin";

/// Serve `BODY` under a non-ASCII filename and return what odl saved it as.
fn saved_name(extra: &[&str]) -> String {
    let mut server = mockito::Server::new();
    let url = format!("{}/f", server.url());
    // RFC 5987 form: the only one that carries non-ASCII, and what real
    // servers send for a name like this.
    let disposition =
        "attachment; filename*=UTF-8\'\'Caf%C3%A9%20M%C3%BCnster%20%E6%97%A5%E6%9C%AC%E8%AA%9E.bin";

    let _head = server
        .mock("HEAD", "/f")
        .with_status(200)
        .with_header("content-length", &BODY.len().to_string())
        .with_header("accept-ranges", "bytes")
        .with_header("content-disposition", disposition)
        .create();
    let _get = server
        .mock("GET", "/f")
        .with_status(206)
        .with_header(
            "content-range",
            &format!("bytes 0-{}/{}", BODY.len() - 1, BODY.len()),
        )
        .with_body(BODY)
        .create();

    let data_dir = tempfile::tempdir().unwrap();
    let save_dir = tempfile::tempdir().unwrap();

    // No `-o`: for a single URL it names the output file, and this test is
    // about the name odl derives. The save directory is the working directory.
    let status = Command::new(env!("CARGO_BIN_EXE_odl"))
        .current_dir(save_dir.path())
        .arg(&url)
        .arg("--download-dir")
        .arg(data_dir.path())
        .arg("--format")
        .arg("json")
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to spawn odl binary");
    assert_eq!(status.code(), Some(0), "download should succeed");

    let mut names: Vec<String> = std::fs::read_dir(save_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names.len(), 1, "expected exactly one saved file: {names:?}");
    names.pop().unwrap()
}

#[test]
fn the_default_keeps_the_name_the_server_gave() {
    // Turning transliteration on by accident would rename every download
    // directory and strand partial data, so the default is pinned.
    assert_eq!(saved_name(&[]), NAME);
}

#[test]
fn the_flag_transliterates_every_script() {
    let name = saved_name(&["--ascii-filenames"]);
    assert!(name.is_ascii(), "still not ASCII: {name}");
    // Note the space deunicode leaves after the last CJK syllable: it
    // separates syllables, and the extension follows one. Harmless — only a
    // name *ending* in a space is a problem, and the sanitiser trims that.
    assert_eq!(name, "Cafe Munster Ri Ben Yu .bin");
}
