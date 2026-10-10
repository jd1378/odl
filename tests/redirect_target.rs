//! The probe and the parts against a link that redirects somewhere else.
//!
//! Both failures here were met on GitHub. A release asset URL redirects to a
//! signed link on another host. For a signed-in `HEAD` it redirects to one
//! that answers 401, while a `GET` gets one that serves the file, so the
//! probe asks with `GET`. And the signed link expires after five minutes,
//! answering 618 from then on, so a part that reconnects after that never
//! gets its bytes from it again.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use odl::config::{ConfigBuilder, DownloadOptions, DownloadOptionsBuilder};
use odl::conflict::{
    FileChangedResolution, FinalFileExistsResolution, NotResumableResolution,
    SameDownloadExistsResolution, SaveConflictResolver, ServerConflict, ServerConflictResolver,
};
use odl::download_manager::{DownloadManager, DownloadRequest, EvaluateRequest};
use odl::error::{ConflictError, OdlError};
use url::Url;

/// Large enough for four parts.
const SIZE: usize = 4 * 1024 * 1024;
const ETAG: &str = "\"asset-etag\"";

fn body() -> Vec<u8> {
    (0..SIZE).map(|i| (i % 251) as u8).collect()
}

struct Request {
    method: String,
    path: String,
    /// Lowercased names.
    headers: Vec<(String, String)>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// `(first, last)` of a `Range: bytes=first-last` header, `last`
    /// defaulting to the end of the file.
    fn range(&self) -> Option<(usize, usize)> {
        let spec = self.header("range")?.strip_prefix("bytes=")?;
        let (first, last) = spec.split_once('-')?;
        let first = first.parse().ok()?;
        let last = last.parse().unwrap_or(SIZE - 1).min(SIZE - 1);
        Some((first, last))
    }

    /// The probe asks for byte zero alone; parts ask for their windows.
    fn is_part(&self) -> bool {
        self.method == "GET" && self.header("range") != Some("bytes=0-0")
    }
}

struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    /// Close the connection after this many body bytes.
    cut_after: Option<usize>,
}

impl Response {
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: vec![],
            body: vec![],
            cut_after: None,
        }
    }

    fn redirect(to: &str) -> Self {
        let mut r = Self::status(302);
        r.headers.push(("Location".into(), to.into()));
        r
    }

    /// The file, or the slice of it `req` asks for.
    fn file(req: &Request, data: &[u8], etag: &str) -> Self {
        let mut r = Self::status(200);
        r.headers.push(("Accept-Ranges".into(), "bytes".into()));
        r.headers.push(("ETag".into(), etag.into()));
        r.body = match req.range() {
            Some((first, last)) => {
                r.status = 206;
                r.headers.push((
                    "Content-Range".into(),
                    format!("bytes {first}-{last}/{}", data.len()),
                ));
                data[first..=last].to_vec()
            }
            None => data.to_vec(),
        };
        r
    }
}

type Log = Arc<Mutex<Vec<String>>>;

/// An HTTP/1.1 server answering each request with `handler`. Returns its
/// base URL and a log of `"METHOD path"` lines, with ` cookie` appended to
/// requests that carried one.
fn serve(handler: impl Fn(&Request) -> Response + Send + Sync + 'static) -> (String, Log) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    let log: Log = Arc::default();
    let seen = log.clone();
    let handler = Arc::new(handler);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            let (handler, seen) = (handler.clone(), seen.clone());
            std::thread::spawn(move || answer(stream, &*handler, &seen));
        }
    });
    (base, log)
}

fn answer(mut stream: TcpStream, handler: &dyn Fn(&Request) -> Response, log: &Log) {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(1) => head.push(byte[0]),
            _ => return,
        }
    }
    let head = String::from_utf8_lossy(&head);
    let mut lines = head.split("\r\n");
    let mut start = lines.next().unwrap_or("").split(' ');
    let req = Request {
        method: start.next().unwrap_or("").to_owned(),
        path: start.next().unwrap_or("").to_owned(),
        headers: lines
            .filter_map(|l| l.split_once(':'))
            .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_owned()))
            .collect(),
    };
    let mut entry = format!("{} {}", req.method, req.path);
    if let Some(range) = req.header("range") {
        entry.push_str(&format!(" {range}"));
    }
    if req.header("cookie").is_some() {
        entry.push_str(" cookie");
    }
    log.lock().unwrap().push(entry);

    let resp = handler(&req);
    let mut out = format!("HTTP/1.1 {} X\r\n", resp.status);
    for (n, v) in &resp.headers {
        out.push_str(&format!("{n}: {v}\r\n"));
    }
    out.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        resp.body.len()
    ));
    if stream.write_all(out.as_bytes()).is_err() || req.method == "HEAD" {
        return;
    }
    let sent = resp.cut_after.unwrap_or(resp.body.len());
    let _ = stream.write_all(&resp.body[..sent]);
}

struct Accept;
#[async_trait::async_trait]
impl SaveConflictResolver for Accept {
    async fn final_file_exists(&self, _: &odl::Download) -> FinalFileExistsResolution {
        FinalFileExistsResolution::ReplaceAndContinue
    }
    async fn same_download_exists(&self, _: &odl::Download) -> SameDownloadExistsResolution {
        SameDownloadExistsResolution::Resume
    }
}
#[async_trait::async_trait]
impl ServerConflictResolver for Accept {
    async fn resolve_file_changed(&self, _: &odl::Download) -> FileChangedResolution {
        FileChangedResolution::Abort
    }
    async fn resolve_not_resumable(&self, _: &odl::Download) -> NotResumableResolution {
        NotResumableResolution::Abort
    }
}

fn options(connections: u64) -> DownloadOptions {
    DownloadOptionsBuilder::default()
        .max_connections(connections)
        .max_retries(1)
        .wait_between_retries(Duration::from_millis(10))
        .headers(Some(
            [("Cookie".to_owned(), "user_session=SECRET".to_owned())]
                .into_iter()
                .collect(),
        ))
        .build()
        .unwrap()
}

/// Evaluate and download `url` the way an embedder does. Returns the file.
async fn fetch(url: &str, opts: &DownloadOptions) -> Result<Vec<u8>, OdlError> {
    let data_dir = tempfile::tempdir().unwrap();
    let save_dir = tempfile::tempdir().unwrap();
    let config = ConfigBuilder::default()
        .download_dir(data_dir.path().to_path_buf())
        .download(opts.clone())
        .build()
        .unwrap();
    let dlm = DownloadManager::new(config);
    let instruction = dlm
        .evaluate(
            EvaluateRequest::new(Url::parse(url).unwrap(), save_dir.path(), &Accept).options(opts),
        )
        .await?;
    let path = dlm
        .download(DownloadRequest::new(instruction, &Accept).options(opts))
        .await?;
    Ok(std::fs::read(path).unwrap())
}

fn entries(log: &Log) -> Vec<String> {
    log.lock().unwrap().clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_probe_asks_the_way_parts_do() {
    let data = Arc::new(body());
    let served = data.clone();
    let (assets, assets_log) = serve(move |req| match req.path.as_str() {
        // Where a signed-in `HEAD` was sent.
        "/legacy" => Response::status(401),
        _ => Response::file(req, &served, ETAG),
    });
    let (site, _) = serve(move |req| {
        if req.method == "HEAD" {
            Response::redirect(&format!("{assets}/legacy"))
        } else {
            Response::redirect(&format!("{assets}/signed"))
        }
    });

    let file = fetch(&format!("{site}/file"), &options(4))
        .await
        .expect("a server that serves GET serves the download");
    assert!(file == *data, "the file came out wrong");

    let log = entries(&assets_log);
    assert_eq!(
        log.first().map(String::as_str),
        Some("GET /signed bytes=0-0"),
        "the probe should have asked for the first byte: {log:?}"
    );
    assert!(
        log.iter().all(|l| !l.starts_with("HEAD ")),
        "no HEAD should be sent: {log:?}"
    );
    assert!(
        log.iter().all(|l| !l.ends_with(" cookie")),
        "the session followed the redirect: {log:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_file_is_asked_for_whole() {
    // An empty file has no first byte, and says so with 416.
    let (site, log) = serve(|req| match req.range() {
        Some(_) => {
            let mut r = Response::status(416);
            r.headers.push(("Content-Range".into(), "bytes */0".into()));
            r
        }
        _ => {
            let mut r = Response::status(200);
            r.headers.push(("Accept-Ranges".into(), "bytes".into()));
            r
        }
    });

    let file = fetch(&format!("{site}/empty"), &options(1))
        .await
        .expect("an empty file is still a file");
    assert!(file.is_empty());
    let log = entries(&log);
    assert_eq!(
        log[..2],
        ["GET /empty bytes=0-0 cookie", "GET /empty cookie"],
        "{log:?}"
    );
}

/// A site that redirects `/file` to a new signed link on `assets` each time
/// it is asked: `/signed/0`, then `/signed/1`, and so on.
fn signing_site(assets: String) -> (String, Log) {
    let issued = Mutex::new(0u32);
    serve(move |_| {
        let mut n = issued.lock().unwrap();
        let link = format!("{assets}/signed/{n}");
        *n += 1;
        Response::redirect(&link)
    })
}

fn token(req: &Request) -> u32 {
    req.path
        .strip_prefix("/signed/")
        .and_then(|t| t.parse().ok())
        .unwrap_or(u32::MAX)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_link_that_expired_before_the_parts_began_is_renewed_once_for_all() {
    // The probe's link has expired by the time parts ask for it, as it does
    // when a download starts minutes after it was evaluated.
    let data = Arc::new(body());
    let served = data.clone();
    let (assets, assets_log) = serve(move |req| {
        if req.is_part() && token(req) == 0 {
            Response::status(618)
        } else {
            Response::file(req, &served, ETAG)
        }
    });
    let (site, site_log) = signing_site(assets);

    let file = fetch(&format!("{site}/file"), &options(4))
        .await
        .expect("an expired link is renewed");
    assert!(file == *data, "the file came out wrong");

    let site_log = entries(&site_log);
    assert_eq!(
        site_log,
        ["GET /file bytes=0-0 cookie", "GET /file bytes=0-0 cookie"],
        "one probe and one renewal, however many parts were refused"
    );
    let assets_log = entries(&assets_log);
    assert!(
        assets_log.iter().all(|l| !l.ends_with(" cookie")),
        "the session followed the redirect: {assets_log:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_part_cut_off_after_its_link_expired_resumes_from_a_fresh_one() {
    // The link works for one part request. That one is cut off halfway, and
    // the reconnect finds the link expired.
    let data = Arc::new(body());
    let served = data.clone();
    let used = Mutex::new(Vec::<u32>::new());
    let (assets, assets_log) = serve(move |req| {
        if !req.is_part() {
            return Response::file(req, &served, ETAG);
        }
        let mut used = used.lock().unwrap();
        if used.contains(&token(req)) {
            return Response::status(618);
        }
        used.push(token(req));
        let mut r = Response::file(req, &served, ETAG);
        if token(req) == 0 {
            r.cut_after = Some(SIZE / 2);
        }
        r
    });
    let (site, _) = signing_site(assets);

    let file = fetch(&format!("{site}/file"), &options(1))
        .await
        .expect("the part resumes from a fresh link");
    assert!(file == *data, "the file came out wrong");

    let log = entries(&assets_log);
    let resumed = format!("GET /signed/1 bytes={}-{}", SIZE / 2, SIZE - 1);
    assert!(
        log.contains(&resumed),
        "the part should have resumed where it stopped: {log:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_renewed_link_on_another_origin_is_not_used() {
    // Parts were scoped to the first asset host's origin, so a link on
    // another one would need headers worked out afresh. The refusal stands.
    let data = Arc::new(body());
    let served = data.clone();
    let (elsewhere, elsewhere_log) = serve(move |req| Response::file(req, &served, ETAG));
    let (assets, _) = serve(|req| {
        if req.is_part() {
            Response::status(618)
        } else {
            Response::file(req, &body(), ETAG)
        }
    });
    let first = Mutex::new(true);
    let (site, _) = serve(move |_| {
        let mut first = first.lock().unwrap();
        let host = if *first { &assets } else { &elsewhere };
        *first = false;
        Response::redirect(&format!("{host}/signed/0"))
    });

    let result = fetch(&format!("{site}/file"), &options(1)).await;
    assert!(
        result.is_err(),
        "the refused link cannot finish the download"
    );
    let log = entries(&elsewhere_log);
    assert!(
        log.iter().all(|l| l == "GET /signed/0 bytes=0-0"),
        "no part may be fetched from another origin: {log:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_renewed_link_to_another_file_is_a_changed_file() {
    let data = Arc::new(body());
    let served = data.clone();
    let (assets, assets_log) = serve(move |req| match token(req) {
        0 if req.is_part() => Response::status(618),
        0 => Response::file(req, &served, ETAG),
        _ => Response::file(req, &served, "\"a-new-upload\""),
    });
    let (site, _) = signing_site(assets);

    let result = fetch(&format!("{site}/file"), &options(1)).await;
    assert!(
        matches!(
            result,
            Err(OdlError::Conflict(ConflictError::Server {
                conflict: ServerConflict::FileChanged
            }))
        ),
        "expected a changed file, got {result:?}"
    );
    let log = entries(&assets_log);
    assert!(
        !log.iter()
            .any(|l| l.starts_with("GET /signed/1") && l != "GET /signed/1 bytes=0-0"),
        "no part may be fetched from the new file: {log:?}"
    );
}
