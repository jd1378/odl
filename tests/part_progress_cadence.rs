//! A silent part still reports.
//!
//! `PartProgress` is the only signal a consumer has for "this part is on a
//! connection right now" — parts leave the wire to be re-scheduled without an
//! event of their own. That only works if the sampler keeps emitting for a
//! part whose bytes have stopped arriving, so a stalled transfer must not look
//! the same as a part that was pulled.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use odl::config::{ConfigBuilder, DownloadOptionsBuilder};
use odl::conflict::{
    FileChangedResolution, FinalFileExistsResolution, NotResumableResolution,
    SameDownloadExistsResolution, SaveConflictResolver, ServerConflictResolver,
};
use odl::download_manager::{DownloadManager, DownloadRequest, EvaluateRequest};
use odl::progress::{
    ASSEMBLY_ULID, DownloadContext, ProgressEvent, ProgressReporter, SAMPLE_INTERVAL, VERIFY_ULID,
};

const SIZE: usize = 64 * 1024;
/// Long enough that a sampler ticking at 125 ms has to speak several times
/// while nothing at all arrives.
const STALL: Duration = Duration::from_millis(900);

fn body() -> Vec<u8> {
    (0..SIZE).map(|i| (i % 251) as u8).collect()
}

/// Records when each part sample arrived, not just that it did.
#[derive(Clone)]
struct Timeline(Arc<Mutex<Vec<(Instant, String, u64)>>>);

impl ProgressReporter for Timeline {
    fn on_event(&self, event: ProgressEvent) {
        if let ProgressEvent::PartProgress {
            ulid, downloaded, ..
        } = event
        {
            self.0
                .lock()
                .unwrap()
                .push((Instant::now(), ulid, downloaded));
        }
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
async fn an_in_flight_part_keeps_sampling_while_no_bytes_arrive() {
    let mut server = mockito::Server::new_async().await;
    let url = format!("{}/file", server.url());
    let data = body();
    let stalled = Arc::new(AtomicBool::new(false));

    let _head = server
        .mock("HEAD", "/file")
        .with_status(200)
        .with_header("content-length", &SIZE.to_string())
        .with_header("accept-ranges", "bytes")
        .with_header("etag", "stall")
        .expect_at_least(1)
        .create();
    let _get = server
        .mock("GET", "/file")
        .with_status(206)
        .with_body_from_request(move |req| {
            // The first (and only) transfer holds its connection open saying
            // nothing, which is what the sampler has to survive.
            if !stalled.swap(true, Ordering::SeqCst) {
                std::thread::sleep(STALL);
            }
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
    // One connection: the single part is the one being stalled, so nothing
    // else can be mistaken for it.
    let opts = DownloadOptionsBuilder::default()
        .max_connections(1)
        .build()
        .unwrap();

    let samples = Arc::new(Mutex::new(Vec::new()));
    let ctx = DownloadContext::new().with_reporter(Arc::new(Timeline(Arc::clone(&samples))));
    let instruction = manager
        .evaluate(
            EvaluateRequest::new(url.parse().unwrap(), save_dir.path(), &Accept)
                .options(&opts)
                .ctx(&ctx),
        )
        .await
        .expect("evaluate");
    manager
        .download(
            DownloadRequest::new(instruction, &Accept)
                .options(&opts)
                .ctx(&ctx),
        )
        .await
        .expect("download");

    let samples = samples.lock().unwrap();
    // Assembly and verification borrow part events; only transfer parts stall.
    let transfer: Vec<&(Instant, String, u64)> = samples
        .iter()
        .filter(|(_, ulid, _)| ulid != ASSEMBLY_ULID && ulid != VERIFY_ULID)
        .collect();
    assert!(
        !transfer.is_empty(),
        "the run reported no part progress at all"
    );

    // Longest run of consecutive samples for one part with an unchanged byte
    // count — the stall as the consumer sees it.
    let mut best: Option<(Duration, usize)> = None;
    let mut run_start = 0usize;
    for i in 1..=transfer.len() {
        let continues = i < transfer.len()
            && transfer[i].1 == transfer[run_start].1
            && transfer[i].2 == transfer[run_start].2;
        if continues {
            continue;
        }
        let span = transfer[i - 1].0.duration_since(transfer[run_start].0);
        let count = i - run_start;
        if best.is_none_or(|(b, _)| span > b) {
            best = Some((span, count));
        }
        run_start = i;
    }

    let (span, count) = best.expect("no samples to measure");
    // Half the stall, sampled at least three times: loose enough for a loaded
    // CI box, tight enough that a data-driven sampler cannot pass.
    assert!(
        span >= STALL / 2 && count >= 3,
        "a part that received nothing for {STALL:?} reported {count} sample(s) over {span:?}; \
         in-flight parts must be sampled every {SAMPLE_INTERVAL:?} regardless of byte arrival"
    );
}
