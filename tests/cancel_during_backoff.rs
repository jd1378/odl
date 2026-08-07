//! Stopping a download that is waiting to retry.
//!
//! `wait_for_retry` returns `false` for two different endings: the retry
//! budget is spent, or the wait was interrupted. A part whose wait is
//! interrupted therefore reports the same `Failed` as one that genuinely ran
//! out of attempts — and if it is the last part in flight, the run ends as
//! "All parts failed" with exit 1 instead of cancelled with exit 130.
//!
//! That matters beyond the exit code: a caller that auto-retries failures
//! would restart a job the user just paused.

#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};

const SIZE: usize = 8 * 1024 * 1024;

/// Answer `HEAD` as a resumable file and refuse every part with 503, so odl
/// settles into a retry backoff and stays there.
fn spawn_always_busy_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            std::thread::spawn(move || serve(stream));
        }
    });
    format!("http://{addr}/file")
}

fn serve(mut stream: TcpStream) {
    let mut req = Vec::new();
    let mut buf = [0u8; 4096];
    while !req.windows(4).any(|w| w == b"\r\n\r\n") {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => req.extend_from_slice(&buf[..n]),
        }
    }
    let _ = if req.starts_with(b"HEAD") {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {SIZE}\r\nAccept-Ranges: bytes\r\nETag: \"c\"\r\nConnection: close\r\n\r\n"
        )
    } else {
        write!(
            stream,
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbusy"
        )
    };
}

#[test]
fn interrupting_a_retry_wait_reports_cancelled_not_failed() {
    let url = spawn_always_busy_server();

    // One connection, so the interrupted part is the only thing in flight —
    // the case where its `Failed` becomes the whole download's verdict. A long
    // backoff guarantees the interrupt lands inside the wait rather than
    // between attempts.
    //
    // Repeated because the original defect was a race: the run loop's
    // `select!` had to pick the cancel branch over the part result, and it
    // reproduced roughly one run in ten.
    for attempt in 1..=6 {
        let data_dir = tempfile::tempdir().unwrap();
        let save_dir = tempfile::tempdir().unwrap();

        let mut child = Command::new(env!("CARGO_BIN_EXE_odl"))
            .arg(&url)
            .arg("-o")
            .arg(save_dir.path().join("file"))
            .arg("--download-dir")
            .arg(data_dir.path())
            .arg("--max-connections")
            .arg("1")
            .arg("--max-retries")
            .arg("5")
            .arg("--wait-between-retries")
            .arg("20s")
            .arg("--format")
            .arg("json")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn odl binary");

        // Long enough to be inside the backoff, not merely started.
        std::thread::sleep(std::time::Duration::from_millis(2500));
        unsafe { libc::kill(child.id() as i32, libc::SIGINT) };

        let mut out = String::new();
        if let Some(mut stdout) = child.stdout.take() {
            let _ = stdout.read_to_string(&mut out);
        }
        let status = child.wait().expect("odl should exit");

        assert_eq!(
            status.code(),
            Some(130),
            "run {attempt}: a stopped download must report cancelled, got: {out}"
        );
        assert!(
            !out.contains("All parts failed"),
            "run {attempt}: cancellation surfaced as a part failure: {out}"
        );
    }
}
