//! A finished part must also read as a full one.
//!
//! `PartFinished` carries no size, so a UI rendering "downloaded / total" has
//! only the sampler's last word. For a part that was already complete on disk
//! when the download resumed there is no such word — it is announced finished
//! having reported nothing — so the row shows complete and empty at once.

use std::sync::{Arc, Mutex};

use odl::config::{ConfigBuilder, DownloadOptionsBuilder};
use odl::conflict::{
    FileChangedResolution, FinalFileExistsResolution, NotResumableResolution,
    SameDownloadExistsResolution, SaveConflictResolver, ServerConflictResolver,
};
use odl::download_manager::{DownloadManager, DownloadRequest, EvaluateRequest};
use odl::progress::{DownloadContext, ProgressEvent, ProgressReporter};

const SIZE: usize = 4 * 1024 * 1024;

fn body() -> Vec<u8> {
    (0..SIZE).map(|i| (i % 251) as u8).collect()
}

/// Keeps every event so a test can assert on their order.
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

#[tokio::test(flavor = "multi_thread")]
async fn a_part_already_complete_on_disk_reports_its_bytes_before_finishing() {
    let mut server = mockito::Server::new_async().await;
    let url = format!("{}/file", server.url());
    let data = body();

    let _head = server
        .mock("HEAD", "/file")
        .with_status(200)
        .with_header("content-length", &SIZE.to_string())
        .with_header("accept-ranges", "bytes")
        .with_header("etag", "parts")
        .expect_at_least(1)
        .create();
    let _get = server
        .mock("GET", "/file")
        .with_status(206)
        .with_body_from_request(move |req| {
            // Honour whatever range is asked for, so the download can finish.
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

    // First run: download it fully, so every part file is complete on disk.
    let instruction = manager
        .evaluate(
            EvaluateRequest::new(url.parse().unwrap(), save_dir.path(), &Accept).options(&opts),
        )
        .await
        .expect("evaluate");
    manager
        .download(DownloadRequest::new(instruction.clone(), &Accept).options(&opts))
        .await
        .expect("first download");

    // A successful run deletes its part files, so the state this is about —
    // parts complete on disk, metadata saying they are not — has to be built
    // rather than left behind. Write each part's bytes back and clear its
    // finished flag.
    std::fs::remove_file(instruction.final_file_path()).ok();
    let meta_path = instruction.download_dir().join("metadata.pb");
    let mut meta: odl::download_metadata::DownloadMetadata =
        prost::Message::decode_length_delimited(&*std::fs::read(&meta_path).unwrap()).unwrap();
    let whole = body();
    for part in meta.parts.values_mut() {
        let start = part.offset as usize;
        let end = start + part.size as usize;
        std::fs::write(
            instruction
                .download_dir()
                .join(format!("{}.part", part.ulid)),
            &whole[start..end.min(whole.len())],
        )
        .unwrap();
        part.finished = false;
    }
    meta.finished = false;
    let mut out = Vec::new();
    prost::Message::encode_length_delimited(&meta, &mut out).unwrap();
    std::fs::write(&meta_path, out).unwrap();

    // Second run: every part is already complete, so each finishes without a
    // single byte transferred.
    let events = Arc::new(Mutex::new(Vec::new()));
    let ctx = DownloadContext::new().with_reporter(Arc::new(Recorder(Arc::clone(&events))));
    let instruction = manager
        .evaluate(
            EvaluateRequest::new(url.parse().unwrap(), save_dir.path(), &Accept)
                .options(&opts)
                .ctx(&ctx),
        )
        .await
        .expect("re-evaluate");
    manager
        .download(
            DownloadRequest::new(instruction, &Accept)
                .options(&opts)
                .ctx(&ctx),
        )
        .await
        .expect("second download");

    let events = events.lock().unwrap();

    // Every part that finished must have said how big it is first, with
    // downloaded == total. Without that a UI has nothing to render but zero.
    let mut finished = 0;
    for (i, event) in events.iter().enumerate() {
        let ProgressEvent::PartFinished { ulid } = event else {
            continue;
        };
        finished += 1;
        let stated_full = events[..i].iter().rev().any(|e| {
            matches!(
                e,
                ProgressEvent::PartProgress { ulid: u, downloaded, total }
                    if u == ulid && downloaded == total && *total > 0
            )
        });
        assert!(
            stated_full,
            "part {ulid} was announced finished having never reported its bytes"
        );
    }
    assert!(finished > 0, "the run should have finished some parts");

    // Every part must have finished. The scheduler waits for each part in a
    // ramp batch to report a first chunk before opening the next, and a part
    // with nothing to transfer sends none — so the batch wait used to drain
    // the task set and return with the rest of the queue unopened. The
    // download then failed with every one of its bytes already on disk.
    let added: Vec<&String> = events
        .iter()
        .filter_map(|e| match e {
            ProgressEvent::PartAdded { ulid, .. } => Some(ulid),
            _ => None,
        })
        .collect();
    assert_eq!(
        finished,
        added.len(),
        "{} part(s) were added but never finished",
        added.len() - finished
    );
}
