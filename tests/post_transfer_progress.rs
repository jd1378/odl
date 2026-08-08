//! What the stages after the transfer report.
//!
//! Assembly used to restart the aggregate `Progress` from zero, so a consumer
//! that did not special-case the phase showed the download falling back to 0%
//! at the finish line. Verification reported nothing at all: the one stage
//! where a bar could only be parked at 100% and hoped over.

use std::sync::{Arc, Mutex};

use odl::config::{ConfigBuilder, DownloadOptionsBuilder};
use odl::conflict::{
    FileChangedResolution, FinalFileExistsResolution, NotResumableResolution,
    SameDownloadExistsResolution, SaveConflictResolver, ServerConflictResolver,
};
use odl::download_manager::{DownloadManager, DownloadRequest, EvaluateRequest};
use odl::progress::{ASSEMBLY_ULID, DownloadContext, ProgressEvent, ProgressReporter, VERIFY_ULID};

/// Large enough to be split across several parts, so assembly has real work.
const SIZE: usize = 4 * 1024 * 1024;

fn body() -> Vec<u8> {
    (0..SIZE).map(|i| (i % 251) as u8).collect()
}

fn sha256_of(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[derive(Default)]
struct Recorder(Arc<Mutex<Vec<ProgressEvent>>>);

impl ProgressReporter for Recorder {
    fn on_event(&self, event: ProgressEvent) {
        self.0.lock().unwrap().push(event);
    }
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
        FileChangedResolution::Restart
    }
    async fn resolve_not_resumable(&self, _: &odl::Download) -> NotResumableResolution {
        NotResumableResolution::Restart
    }
}

/// Download a file the server publishes a SHA-256 for, and keep every event.
async fn run_and_record() -> Vec<ProgressEvent> {
    let mut server = mockito::Server::new_async().await;
    let url = format!("{}/file", server.url());
    let data = body();

    let _head = server
        .mock("HEAD", "/file")
        .with_status(200)
        .with_header("content-length", &SIZE.to_string())
        .with_header("accept-ranges", "bytes")
        .with_header("etag", "stages")
        // Server-advertised digest, so verification actually hashes the file.
        .with_header(
            "digest",
            &format!("sha-256={}", {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.encode(
                    (0..32)
                        .map(|i| {
                            u8::from_str_radix(&sha256_of(&data)[i * 2..i * 2 + 2], 16).unwrap()
                        })
                        .collect::<Vec<u8>>(),
                )
            }),
        )
        .expect_at_least(1)
        .create();
    let _get = server
        .mock("GET", "/file")
        .with_status(206)
        .with_body_from_request(move |req| {
            let range = req
                .header("range")
                .first()
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("bytes="))
                .and_then(|v| v.split_once('-'))
                .and_then(|(a, b)| Some((a.parse::<usize>().ok()?, b.parse::<usize>().ok()?)))
                .unwrap_or((0, SIZE - 1));
            data[range.0..=range.1.min(SIZE - 1)].to_vec()
        })
        .expect_at_least(1)
        .create();

    let data_dir = tempfile::tempdir().unwrap();
    let save_dir = tempfile::tempdir().unwrap();
    let config = ConfigBuilder::default()
        .download_dir(data_dir.path().to_path_buf())
        .build()
        .unwrap();
    let manager = DownloadManager::new(config);
    let opts = DownloadOptionsBuilder::default()
        .max_connections(4)
        .build()
        .unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let ctx = DownloadContext::new().with_reporter(Arc::new(Recorder(Arc::clone(&events))));
    let instruction = manager
        .evaluate(
            EvaluateRequest::new(url.parse().unwrap(), save_dir.path(), &Accept)
                .options(&opts)
                .ctx(&ctx),
        )
        .await
        .expect("evaluate");
    assert!(
        !instruction.checksums().is_empty(),
        "the server's digest must reach the download, or nothing gets hashed"
    );
    manager
        .download(
            DownloadRequest::new(instruction, &Accept)
                .options(&opts)
                .ctx(&ctx),
        )
        .await
        .expect("download");

    events.lock().unwrap().clone()
}

/// The aggregate byte count belongs to the transfer. Assembly copies the same
/// bytes a second time; reporting that as download progress made the bar fall
/// back to zero once the download was already complete.
#[tokio::test(flavor = "multi_thread")]
async fn the_aggregate_progress_never_falls_back_after_the_transfer() {
    let events = run_and_record().await;

    let mut high_water = 0u64;
    for event in &events {
        if let ProgressEvent::Progress { downloaded, .. } = event {
            assert!(
                *downloaded >= high_water,
                "aggregate progress went backwards: {high_water} then {downloaded}"
            );
            high_water = *downloaded;
        }
    }
    assert_eq!(
        high_water, SIZE as u64,
        "the transfer's own progress must still reach the full size"
    );
}

/// Assembly keeps its own row: the aggregate stopped carrying it, so this is
/// the only place a consumer can see the copy move.
#[tokio::test(flavor = "multi_thread")]
async fn assembly_reports_on_its_own_row() {
    let events = run_and_record().await;

    assert!(
        events.iter().any(|e| matches!(
            e,
            ProgressEvent::PartAdded { ulid, .. } if ulid == ASSEMBLY_ULID
        )),
        "assembly must announce a row of its own"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            ProgressEvent::PartFinished { ulid } if ulid == ASSEMBLY_ULID
        )),
        "assembly must finish its row"
    );
}

/// Hashing a large file takes long enough to look like a hang. It used to run
/// to completion silently, leaving a consumer nothing to show.
#[tokio::test(flavor = "multi_thread")]
async fn verification_reports_progress_and_finishes() {
    let events = run_and_record().await;

    let announced = events.iter().any(|e| matches!(
        e,
        ProgressEvent::PartAdded { ulid, size, .. } if ulid == VERIFY_ULID && *size == SIZE as u64
    ));
    assert!(
        announced,
        "verification must announce a row sized like the file"
    );

    let landed = events.iter().any(|e| {
        matches!(
            e,
            ProgressEvent::PartProgress { ulid, downloaded, total }
                if ulid == VERIFY_ULID && downloaded == total && *total == SIZE as u64
        )
    });
    assert!(
        landed,
        "verification must report reaching the end of the file"
    );

    assert!(
        events.iter().any(|e| matches!(
            e,
            ProgressEvent::PartFinished { ulid } if ulid == VERIFY_ULID
        )),
        "verification must finish its row"
    );

    // Ordering: nothing may claim verification finished before it was
    // announced, or a consumer keyed by ulid drops an event it never opened.
    let added = events
        .iter()
        .position(|e| matches!(e, ProgressEvent::PartAdded { ulid, .. } if ulid == VERIFY_ULID));
    let finished = events
        .iter()
        .position(|e| matches!(e, ProgressEvent::PartFinished { ulid } if ulid == VERIFY_ULID));
    assert!(added < finished, "the row must be added before it finishes");
}
