//! A part request's response is data only if it answers the question that was
//! asked. These pin the two ways a server can answer something else — an
//! error page, or the whole file where a slice was requested — because both
//! used to be written into the part file and completed with a zero exit code.

use std::process::{Command, Stdio};

/// Large enough to be split across several parts (`MIN_PART_SIZE` is 300 KB),
/// which is what makes a whole-file response land at the wrong offset.
const SIZE: usize = 4 * 1024 * 1024;

fn body() -> Vec<u8> {
    (0..SIZE).map(|i| (i % 251) as u8).collect()
}

/// Serve `HEAD` as a resumable file of `SIZE` bytes, answer every `GET` with
/// `get`, and return odl's exit code alongside the file it produced.
fn run_against(
    get: impl FnOnce(&mut mockito::Server) -> mockito::Mock,
    connections: &str,
) -> (Option<i32>, Option<Vec<u8>>) {
    let mut server = mockito::Server::new();
    let url = format!("{}/file", server.url());

    let _head = server
        .mock("HEAD", "/file")
        .with_status(200)
        .with_header("content-length", &SIZE.to_string())
        .with_header("accept-ranges", "bytes")
        .with_header("etag", "rangeetag")
        .create();
    let _get = get(&mut server);

    let data_dir = tempfile::tempdir().unwrap();
    let save_dir = tempfile::tempdir().unwrap();
    let out = save_dir.path().join("file");

    let status = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg(&url)
        .arg("-o")
        .arg(&out)
        .arg("--download-dir")
        .arg(data_dir.path())
        .arg("--max-connections")
        .arg(connections)
        .arg("--max-retries")
        .arg("0")
        .arg("--format")
        .arg("json")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to spawn odl binary");

    (status.code(), std::fs::read(&out).ok())
}

#[test]
fn a_whole_file_answer_to_a_ranged_request_is_refused() {
    // A server that quietly stopped honouring `Range` answers 200 with the
    // entire file. Each part would write the *first* part-size bytes of it at
    // its own offset, producing a file of exactly the right length whose
    // contents are wrong — the one failure nothing downstream can catch.
    let (code, file) = run_against(
        |s| {
            s.mock("GET", "/file")
                .expect_at_least(1)
                .with_status(200)
                .with_header("content-length", &SIZE.to_string())
                .with_body(body())
                .create()
        },
        "4",
    );
    assert_eq!(code, Some(4), "must surface as a conflict, not success");
    assert!(
        file.is_none(),
        "no file may be delivered from a bad response"
    );
}

#[test]
fn an_error_page_is_not_written_as_part_data() {
    // Previously this looped forever, appending the error page to the parts
    // and counting it as progress, with `--max-retries 0` ignored.
    let (code, file) = run_against(
        |s| {
            s.mock("GET", "/file")
                .expect_at_least(1)
                .with_status(500)
                .with_body("<html>internal server error</html>")
                .create()
        },
        "2",
    );
    assert_ne!(code, Some(0), "a failed transfer must not report success");
    assert!(
        file.is_none_or(|f| f.iter().all(|b| *b == 0)),
        "an error page must never reach the output file"
    );
}

#[test]
fn a_single_connection_download_still_works_without_range_support() {
    // The one case where a 200 is usable: the part *is* the whole file and
    // nothing has been written, so the body is exactly what was asked for.
    // Refusing here would break every server that ignores `Range`.
    let (code, file) = run_against(
        |s| {
            s.mock("GET", "/file")
                .expect_at_least(1)
                .with_status(200)
                .with_header("content-length", &SIZE.to_string())
                .with_body(body())
                .create()
        },
        "1",
    );
    assert_eq!(code, Some(0), "a whole-file answer to one part is valid");
    assert_eq!(file.as_deref(), Some(body().as_slice()));
}
