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
    let (code, file, _) = run_capturing(get, connections);
    (code, file)
}

/// As [`run_against`], also returning odl's JSON output so a test can assert
/// on which failure was reported, not merely that one was.
fn run_capturing(
    get: impl FnOnce(&mut mockito::Server) -> mockito::Mock,
    connections: &str,
) -> (Option<i32>, Option<Vec<u8>>, String) {
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
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn odl binary");

    let reported = String::from_utf8_lossy(&status.stderr).into_owned()
        + &String::from_utf8_lossy(&status.stdout);
    (status.status.code(), std::fs::read(&out).ok(), reported)
}

#[test]
fn a_transfer_failure_is_reported_as_one() {
    // Every part fails, so nothing is left in flight while parts are still
    // queued. That used to end the run as `Ok`, leaving the assembler to
    // notice — and it reported "part file shorter than recorded size", an I/O
    // error for what was plainly a failed transfer.
    let (code, file, reported) = run_capturing(
        |s| {
            s.mock("GET", "/file")
                .expect_at_least(1)
                .with_status(500)
                .with_body("boom")
                .create()
        },
        "4",
    );
    assert!(
        !reported.contains("shorter than recorded size"),
        "the assembler should not be the one to notice: {reported}"
    );
    // The cause survives all the way out: a caller that retries on network
    // errors must see the 500, not a generic failure it would give up on.
    assert_eq!(code, Some(3), "a 500 is a network failure: {reported}");
    assert!(
        reported.contains("HTTP 500"),
        "expected the status in the error, got: {reported}"
    );
    assert!(
        file.is_none(),
        "a failed download must leave no output file"
    );
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
    // Not merely "not the error page": no file at all. Assembly sizes the
    // destination up front, so failing part-way used to leave a full-length
    // run of zeros looking like a finished download.
    assert!(
        file.is_none(),
        "a failed download must leave no output file"
    );
}

#[test]
fn a_chunked_answer_without_a_length_is_accepted() {
    // A 200 with `Transfer-Encoding: chunked` carries no `Content-Length`.
    // Reading that absence as "the length disagrees" refuses a download that
    // is perfectly good — a server may answer HEAD with a length and stream
    // the GET.
    let (code, file) = run_against(
        |s| {
            s.mock("GET", "/file")
                .expect_at_least(1)
                .with_status(200)
                .with_chunked_body(|w| w.write_all(&body()))
                .create()
        },
        "1",
    );
    assert_eq!(code, Some(0), "a chunked whole-file answer is valid");
    assert_eq!(file.as_deref(), Some(body().as_slice()));
}

#[test]
fn a_200_may_carry_content_range_and_is_judged_on_it() {
    // RFC 9110 14.4 gives `Content-Range` no meaning outside 206 and 416, so
    // it is never used to place the body. It is still worth distrusting: a
    // 200 announcing a window that does not start at zero is not the whole
    // file, whatever the status line says.
    let starting_at_zero = run_against(
        |s| {
            s.mock("GET", "/file")
                .expect_at_least(1)
                .with_status(200)
                .with_header("content-range", &format!("bytes 0-{}/{}", SIZE - 1, SIZE))
                .with_header("content-length", &SIZE.to_string())
                .with_body(body())
                .create()
        },
        "1",
    );
    assert_eq!(
        starting_at_zero.0,
        Some(0),
        "the whole file, plainly stated"
    );
    assert_eq!(starting_at_zero.1.as_deref(), Some(body().as_slice()));

    let starting_elsewhere = run_against(
        |s| {
            s.mock("GET", "/file")
                .expect_at_least(1)
                .with_status(200)
                .with_header(
                    "content-range",
                    &format!("bytes 1024-{}/{}", SIZE - 1, SIZE),
                )
                .with_chunked_body(|w| w.write_all(&body()[1024..]))
                .create()
        },
        "1",
    );
    assert_eq!(
        starting_elsewhere.0,
        Some(4),
        "a slice is not the whole file"
    );
    assert!(starting_elsewhere.1.is_none(), "no output from a refusal");
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
