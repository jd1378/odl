//! A link that dies without saying so.
//!
//! Every failure mode odl already handled announces itself: the socket is
//! refused, reset, or closed, and something returns an error. The one that
//! does not is a server that keeps the connection open and simply stops
//! speaking: it never answers the request, or answers it and then goes
//! quiet mid-body. Nothing below the application layer reports that, so the
//! only evidence is the absence of bytes, and a download manager that does
//! not measure silence waits on it for as long as the process lives.
//!
//! These tests hold connections open deliberately and assert that odl gives
//! up on its own.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SIZE: usize = 4 * 1024 * 1024;
/// Bytes the server hands over before the link goes quiet. Large enough that
/// the parts are unmistakably under way when it happens.
const SERVED_BEFORE_DEATH: usize = 64 * 1024;

/// What the server does once it has read a request.
#[derive(Clone, Copy, PartialEq)]
enum Behavior {
    /// Never answer anything. The connection stays open, unanswered.
    SayNothing,
    /// Answer `HEAD` normally; serve the first `n` part `GET`s some data and
    /// then stop, and answer every later `GET` with headers and nothing else.
    /// Both cases hold the connection open rather than closing it.
    DieAfterServing(usize),
}

struct Server {
    addr: SocketAddr,
    /// Part `GET`s the server has been asked for, which is what says whether
    /// odl kept reopening connections after giving up on them.
    gets: Arc<AtomicUsize>,
}

/// Start a server on an ephemeral port. Its threads are detached: they hold
/// their sockets open until the test process exits, which is the point.
fn spawn(behavior: Behavior) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let gets = Arc::new(AtomicUsize::new(0));
    let gets_for_thread = Arc::clone(&gets);

    std::thread::spawn(move || {
        // Sockets answered so far. Dropping one would close it, which reads
        // as a clean end-of-body, the very signal these tests must withhold.
        let held: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let gets = Arc::clone(&gets_for_thread);
            let held = Arc::clone(&held);
            std::thread::spawn(move || {
                let Some(request) = read_request(&mut stream) else {
                    return;
                };
                if behavior == Behavior::SayNothing {
                    held.lock().unwrap().push(stream);
                    return;
                }
                let Behavior::DieAfterServing(alive_for) = behavior else {
                    return;
                };
                if request.starts_with("HEAD") {
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {SIZE}\r\nAccept-Ranges: bytes\r\n\
                         ETag: \"deadlink\"\r\nConnection: keep-alive\r\n\r\n"
                    );
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.flush();
                    held.lock().unwrap().push(stream);
                    return;
                }
                let nth = gets.fetch_add(1, Ordering::SeqCst);
                // Chunked, so the body is only over when a zero-length chunk
                // arrives, which never does. Without that, hyper would see
                // a complete body and report EOF instead of stalling.
                let head = "HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\n\
                            Accept-Ranges: bytes\r\nConnection: keep-alive\r\n\r\n";
                if stream.write_all(head.as_bytes()).is_err() {
                    return;
                }
                if nth < alive_for {
                    let mut chunk = format!("{SERVED_BEFORE_DEATH:x}\r\n").into_bytes();
                    chunk.extend(std::iter::repeat_n(0u8, SERVED_BEFORE_DEATH));
                    chunk.extend_from_slice(b"\r\n");
                    if stream.write_all(&chunk).is_err() {
                        return;
                    }
                }
                let _ = stream.flush();
                held.lock().unwrap().push(stream);
            });
        }
    });

    Server { addr, gets }
}

/// Read request headers up to the blank line. `None` if the peer hung up.
fn read_request(stream: &mut TcpStream) -> Option<String> {
    let mut acc = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        let n = stream.read(&mut buf).ok()?;
        if n == 0 {
            return None;
        }
        acc.extend_from_slice(&buf[..n]);
        if acc.windows(4).any(|w| w == b"\r\n\r\n") {
            return Some(String::from_utf8_lossy(&acc).into_owned());
        }
    }
}

struct Run {
    code: Option<i32>,
    /// stdout and stderr together: the NDJSON event stream goes to one and
    /// the final error object to the other.
    output: String,
    elapsed: Duration,
}

/// Run odl against `url` and wait at most `patience` for it to finish. A run
/// that outlives that is the bug these tests exist for, so it is a failure,
/// not a slow pass.
fn run_odl(url: &str, extra: &[&str], patience: Duration) -> Run {
    let data_dir = tempfile::tempdir().unwrap();
    let save_dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_odl"));
    cmd.arg(url)
        .arg("-o")
        .arg(save_dir.path().join("file"))
        .arg("--download-dir")
        .arg(data_dir.path())
        .arg("--format")
        .arg("json")
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let started = Instant::now();
    let mut child = cmd.spawn().expect("spawn odl");
    // Drained on their own threads: a pipe left unread fills up and blocks
    // the child, which would look exactly like the hang under test.
    let out = drain(child.stdout.take().expect("stdout"));
    let err = drain(child.stderr.take().expect("stderr"));

    let code = match wait_for(&mut child, patience) {
        Some(status) => status,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "odl was still running {patience:?} after the link went dead; \
                 it never noticed the server had stopped sending"
            );
        }
    };
    Run {
        code,
        output: format!(
            "{}{}",
            out.join().unwrap_or_default(),
            err.join().unwrap_or_default()
        ),
        elapsed: started.elapsed(),
    }
}

fn drain<R: Read + Send + 'static>(mut pipe: R) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut s = String::new();
        let _ = pipe.read_to_string(&mut s);
        s
    })
}

/// `Child::wait` with a deadline. `None` means it is still running.
fn wait_for(child: &mut Child, patience: Duration) -> Option<Option<i32>> {
    let deadline = Instant::now() + patience;
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => return Some(status.code()),
            None if Instant::now() >= deadline => return None,
            None => std::thread::sleep(Duration::from_millis(25)),
        }
    }
}

/// Exit 3 is the network class. A dead link is a network fault, not a
/// conflict or a local error, and a caller branching on the code needs it to
/// say so.
fn assert_reported_as_network_failure(run: &Run) {
    assert_eq!(run.code, Some(3), "output was:\n{}", run.output);
    assert!(
        run.output
            .lines()
            .any(|l| l.contains("\"type\":\"error\"") && l.contains("\"kind\":\"network\"")),
        "no machine-readable network error in:\n{}",
        run.output
    );
}

/// The stock configuration has to be the safe one: someone who never heard of
/// the option is exactly who this protects.
#[test]
fn a_server_that_never_answers_is_given_up_on_by_default() {
    let server = spawn(Behavior::SayNothing);
    let url = format!("http://{}/file", server.addr);

    // No `--read-timeout`: whatever ships as the default has to bound this.
    let run = run_odl(&url, &["--max-retries", "0"], Duration::from_secs(90));

    assert_reported_as_network_failure(&run);
    assert_eq!(
        server.gets.load(Ordering::SeqCst),
        0,
        "the download never got past the probe, so no part should have been requested"
    );
}

/// The connect timeout only covers reaching the server. Past that, the wait
/// for a response is unbounded unless something bounds it.
#[test]
fn a_server_that_never_answers_is_given_up_on_promptly() {
    let server = spawn(Behavior::SayNothing);
    let url = format!("http://{}/file", server.addr);

    let run = run_odl(
        &url,
        &["--read-timeout", "1s", "--max-retries", "1"],
        Duration::from_secs(60),
    );

    assert_reported_as_network_failure(&run);
    assert!(
        run.elapsed < Duration::from_secs(30),
        "took {:?}, which is far longer than the read timeout allows",
        run.elapsed
    );
}

/// The reported case: the transfer starts, runs, and then the server stops
/// sending without closing anything.
#[test]
fn a_transfer_that_goes_quiet_mid_body_fails_instead_of_waiting() {
    let server = spawn(Behavior::DieAfterServing(2));
    let url = format!("http://{}/file", server.addr);

    let run = run_odl(
        &url,
        &[
            "--read-timeout",
            "1s",
            "--max-retries",
            "1",
            "--max-connections",
            "2",
        ],
        Duration::from_secs(60),
    );

    assert_reported_as_network_failure(&run);
    assert!(
        server.gets.load(Ordering::SeqCst) >= 2,
        "the parts should have started before the link died"
    );
}

/// A part handed back unfinished is requeued so a server that only tolerates
/// a few connections still gets served one part at a time. The retry budget
/// lives inside the part task, though, so every requeue used to hand out a
/// fresh one: with more than one part in play neither was ever the last to
/// fail, each kept requeueing the other, and `--max-retries` bounded nothing
/// at all. `--max-retries 0` says "do not retry", and the count of requests
/// the server sees is what shows whether that was honoured.
#[test]
fn a_stalled_part_is_not_rescheduled_without_end() {
    let server = spawn(Behavior::DieAfterServing(0));
    let url = format!("http://{}/file", server.addr);

    let run = run_odl(
        &url,
        &[
            "--max-retries",
            "0",
            "--max-connections",
            "2",
            // Ramped opening happens to stop a failing batch on its own.
            // Without it there is nothing between a requeue and the next
            // scheduling, which is where the missing budget shows.
            "--rampup",
            "false",
        ],
        Duration::from_secs(90),
    );

    assert_reported_as_network_failure(&run);
    let gets = server.gets.load(Ordering::SeqCst);
    assert!(
        gets <= 4,
        "the server was asked for parts {gets} times across 2 parts with retries turned off; \
         the retry budget is not bounding reschedules"
    );
}
