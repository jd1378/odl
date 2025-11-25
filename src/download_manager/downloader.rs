use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use reqwest::{
    Client,
    header::{HeaderValue, RANGE, USER_AGENT},
};
use tokio::{
    fs,
    io::{AsyncWriteExt, BufWriter},
    sync::{Mutex, Notify},
    task::JoinSet,
    time::{self, Duration, Instant},
};
use tracing::{Instrument, Span, info_span};
use tracing_indicatif::span_ext::IndicatifSpanExt;
use ulid::Ulid;

use prost::Message;

use crate::{
    download::Download,
    download_manager::io::persist_encoded_metadata,
    download_metadata::{DownloadMetadata, PartDetails},
    error::{MetadataError, OdlError},
    user_agents::random_user_agent,
};

/// Minimum chunk size we keep on a single task before attempting another split.
const MIN_DYNAMIC_SPLIT_SIZE: u64 = 3 * 1024 * 1024; // 3 MB
/// Minimum eta needed for dynamic split to happen. any eta less than this will skip creating more chunks
/// as it will be inefficient
const MIN_DYNAMIC_SPLIT_ETA: Duration = Duration::from_secs(60); // 1 minutes

#[cfg(not(test))]
const STALE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const STALE_CONNECTION_TIMEOUT: Duration = Duration::from_millis(100);

/// Coordinates how parts are downloaded, including dynamic splitting to keep
/// all available connections busy.
pub struct Downloader {
    instruction: Arc<Download>,
    metadata: Arc<Mutex<DownloadMetadata>>,
    client: Arc<Client>,
    randomize_user_agent: bool,
    concurrency_limit: usize,
    aggregator_span: Span,
    speed_limiter: Option<Arc<BandwidthLimiter>>,
    persist_mutex: Arc<Mutex<()>>,
}

impl Downloader {
    pub fn new(
        instruction: Arc<Download>,
        metadata: DownloadMetadata,
        client: Client,
        randomize_user_agent: bool,
        aggregator_span: Span,
        speed_limit: Option<u64>,
    ) -> Self {
        let concurrency_limit = metadata.max_connections as usize;
        let speed_limiter = speed_limit
            .filter(|limit| *limit > 0)
            .map(|limit| Arc::new(BandwidthLimiter::new(limit)));
        Self {
            instruction,
            metadata: Arc::new(Mutex::new(metadata)),
            client: Arc::new(client),
            randomize_user_agent,
            concurrency_limit: if concurrency_limit == 0 {
                1
            } else {
                concurrency_limit
            },
            aggregator_span,
            speed_limiter,
            persist_mutex: Arc::new(Mutex::new(())),
        }
    }

    pub async fn run(self) -> Result<DownloadMetadata, OdlError> {
        let mut pending = self.pending_parts().await;
        let mut active: HashMap<String, ActiveTask> = HashMap::new();
        let mut join_set: JoinSet<Result<PartEvent, OdlError>> = JoinSet::new();

        // Schedule a single probe connection first. Once it begins receiving data
        // we'll expand to fill the full concurrency capacity.
        if let Some(first_part) = pending.pop_front() {
            let probe = Arc::new(Notify::new());
            self.schedule_part(first_part, &mut active, &mut join_set, Some(probe.clone()))
                .await?;

            // Wait until either the probe signals it has started receiving data,
            // or the task finishes (e.g., zero-length part completes immediately).
            tokio::select! {
                _ = probe.notified() => {
                    // probe started; continue to fill capacity below
                }
                maybe_res = join_set.join_next() => {
                    if let Some(res) = maybe_res {
                        self.handle_join_result_item(res, &mut pending, &mut active, &mut join_set).await?;
                    }
                }
            }
        }

        // Now fill remaining capacity up to configured concurrency.
        self.fill_capacity(&mut pending, &mut active, &mut join_set)
            .await?;

        while let Some(result) = join_set.join_next().await {
            self.handle_join_result_item(result, &mut pending, &mut active, &mut join_set)
                .await?;
            self.fill_capacity(&mut pending, &mut active, &mut join_set)
                .await?;
        }

        let metadata_mutex = Arc::try_unwrap(self.metadata).map_err(|_| {
            OdlError::MetadataError(MetadataError::Other {
                message: "Failed to unwrap metadata Arc".to_string(),
            })
        })?;
        Ok(metadata_mutex.into_inner())
    }

    async fn pending_parts(&self) -> VecDeque<PartDetails> {
        let metadata = self.metadata.lock().await;
        metadata
            .parts
            .values()
            .filter(|p| !p.finished)
            .cloned()
            .collect()
    }

    async fn fill_capacity(
        &self,
        pending: &mut VecDeque<PartDetails>,
        active: &mut HashMap<String, ActiveTask>,
        join_set: &mut JoinSet<Result<PartEvent, OdlError>>,
    ) -> Result<(), OdlError> {
        if self.concurrency_limit == 0 {
            return Ok(());
        }

        self.ensure_pending_pool(pending, active).await?;

        while active.len() < self.concurrency_limit {
            if let Some(part) = pending.pop_front() {
                self.schedule_part(part, active, join_set, None).await?;
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn handle_join_result_item(
        &self,
        res: Result<Result<PartEvent, OdlError>, tokio::task::JoinError>,
        pending: &mut VecDeque<PartDetails>,
        active: &mut HashMap<String, ActiveTask>,
        join_set: &mut JoinSet<Result<PartEvent, OdlError>>,
    ) -> Result<(), OdlError> {
        match res {
            Ok(Ok(event)) => match event {
                PartEvent::Completed(outcome) => {
                    active.remove(&outcome.ulid);
                    self.mark_part_finished(&outcome).await?;
                }
                PartEvent::NeedsReschedule { ulid } => {
                    if let Some(task) = active.remove(&ulid) {
                        pending.push_back(task.details);
                    }
                }
            },
            Ok(Err(e)) => {
                join_set.shutdown().await;
                return Err(e);
            }
            Err(join_err) => {
                join_set.shutdown().await;
                return Err(OdlError::Other {
                    message: "Download task panicked".to_string(),
                    origin: Box::new(join_err),
                });
            }
        }
        Ok(())
    }

    async fn ensure_pending_pool(
        &self,
        pending: &mut VecDeque<PartDetails>,
        active: &mut HashMap<String, ActiveTask>,
    ) -> Result<(), OdlError> {
        // Only attempt to create enough pending parts to fill the spare capacity
        // (i.e. `concurrency_limit - active.len()`)
        let spare_capacity = self.concurrency_limit.saturating_sub(active.len());
        while pending.len() < spare_capacity {
            if !self.try_split_active(active, pending).await? {
                break;
            }
        }
        Ok(())
    }

    async fn schedule_part(
        &self,
        part: PartDetails,
        active: &mut HashMap<String, ActiveTask>,
        join_set: &mut JoinSet<Result<PartEvent, OdlError>>,
        probe_notify: Option<Arc<Notify>>,
    ) -> Result<(), OdlError> {
        let initial_downloaded = self.detect_existing_size(&part).await?;
        let controller = Arc::new(PartController::new(part.size, initial_downloaded));
        let task_part = part.clone();
        let controller_clone = Arc::clone(&controller);
        let client = Arc::clone(&self.client);
        let instruction = Arc::clone(&self.instruction);
        let aggregator_span = self.aggregator_span.clone();
        let randomize_user_agent = self.randomize_user_agent;
        let speed_limiter = self.speed_limiter.clone();
        let span_ulid = task_part.ulid.clone();
        let part_span = info_span!("part", ulid = span_ulid.as_str());

        // Pass through the optional probe notifier to the download task. The notifier
        // will be signalled when the task starts receiving data (first chunk).
        let probe_for_task = probe_notify.clone();
        join_set.spawn(
            async move {
                download_part(
                    client,
                    instruction,
                    task_part,
                    controller_clone,
                    randomize_user_agent,
                    aggregator_span,
                    speed_limiter,
                    probe_for_task,
                )
                .await
            }
            .instrument(part_span.clone()),
        );

        active.insert(
            part.ulid.clone(),
            ActiveTask {
                details: part,
                controller,
                span: part_span.clone(),
            },
        );

        Ok(())
    }

    async fn detect_existing_size(&self, part: &PartDetails) -> Result<u64, OdlError> {
        let part_path = self.instruction.part_path(&part.ulid);
        match fs::metadata(&part_path).await {
            Ok(meta) => Ok(meta.len()),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(0)
                } else {
                    Err(OdlError::StdIoError {
                        e,
                        extra_info: Some(format!(
                            "Failed to inspect download part at {}",
                            part_path.display(),
                        )),
                    })
                }
            }
        }
    }

    async fn try_split_active(
        &self,
        active: &mut HashMap<String, ActiveTask>,
        pending: &mut VecDeque<PartDetails>,
    ) -> Result<bool, OdlError> {
        let candidate = active
            .iter()
            .filter(|(_, task)| task.remaining_bytes() >= MIN_DYNAMIC_SPLIT_SIZE * 2)
            .max_by_key(|(_, task)| task.remaining_bytes())
            .map(|(ulid, task)| SplitCandidate {
                ulid: ulid.clone(),
                controller: Arc::clone(&task.controller),
            });

        let Some(candidate) = candidate else {
            return Ok(false);
        };

        let split_result = self.split_task(&candidate).await?;
        if let Some((new_part, new_limit)) = split_result {
            if let Some(task) = active.get_mut(&candidate.ulid) {
                task.details.size = new_limit;
                task.span.pb_set_length(new_limit);
            }
            pending.push_back(new_part);
            return Ok(true);
        }
        Ok(false)
    }

    async fn split_task(
        &self,
        candidate: &SplitCandidate,
    ) -> Result<Option<(PartDetails, u64)>, OdlError> {
        // If estimated time to finish entire download is <= 60s, avoid splitting as it will be inefficient
        if self.aggregator_span.pb_eta() <= MIN_DYNAMIC_SPLIT_ETA {
            return Ok(None);
        }

        let downloaded = candidate.controller.downloaded();
        let current_limit = candidate.controller.limit();
        if current_limit <= downloaded + MIN_DYNAMIC_SPLIT_SIZE * 2 {
            return Ok(None);
        }

        let remaining = current_limit - downloaded;
        let split_size = remaining / 2;
        if split_size < MIN_DYNAMIC_SPLIT_SIZE {
            return Ok(None);
        }

        let new_limit = current_limit - split_size;
        candidate.controller.set_limit(new_limit);

        let (new_part, encoded_metadata) = {
            let mut metadata = self.metadata.lock().await;
            let part_entry = metadata.parts.get_mut(&candidate.ulid).ok_or_else(|| {
                OdlError::MetadataError(MetadataError::Other {
                    message: format!("Part with ulid {} not found", candidate.ulid),
                })
            })?;
            part_entry.size = new_limit;
            let new_part_offset = part_entry.offset + new_limit;
            let new_ulid = Ulid::new().to_string();
            let new_part = PartDetails {
                offset: new_part_offset,
                size: split_size,
                ulid: new_ulid.clone(),
                finished: false,
            };
            metadata.parts.insert(new_ulid, new_part.clone());
            let encoded = metadata.encode_length_delimited_to_vec();
            (new_part, encoded)
        };

        self.persist_metadata_bytes(encoded_metadata).await?;

        Ok(Some((new_part, new_limit)))
    }

    async fn mark_part_finished(&self, outcome: &PartOutcome) -> Result<(), OdlError> {
        let maybe_encoded = {
            let mut metadata = self.metadata.lock().await;
            if let Some(part) = metadata.parts.get_mut(&outcome.ulid) {
                part.finished = true;
                part.size = outcome.final_size;
                Some(metadata.encode_length_delimited_to_vec())
            } else {
                None
            }
        };

        if let Some(encoded) = maybe_encoded {
            self.persist_metadata_bytes(encoded).await?;
        }
        Ok(())
    }

    async fn persist_metadata_bytes(&self, encoded: Vec<u8>) -> Result<(), OdlError> {
        let _guard = self.persist_mutex.lock().await;
        persist_encoded_metadata(&encoded, &self.instruction)
            .await
            .map_err(|e| OdlError::StdIoError {
                e,
                extra_info: Some(format!(
                    "Failed to persist metadata at {}",
                    self.instruction.metadata_path().display()
                )),
            })
    }
}

struct ActiveTask {
    details: PartDetails,
    controller: Arc<PartController>,
    span: Span,
}
struct SplitCandidate {
    ulid: String,
    controller: Arc<PartController>,
}

impl ActiveTask {
    fn remaining_bytes(&self) -> u64 {
        self.controller
            .limit()
            .saturating_sub(self.controller.downloaded())
    }
}

struct PartController {
    downloaded: AtomicU64,
    limit: AtomicU64,
}

impl PartController {
    fn new(limit: u64, initial_downloaded: u64) -> Self {
        Self {
            downloaded: AtomicU64::new(initial_downloaded),
            limit: AtomicU64::new(limit),
        }
    }

    fn record_progress(&self, delta: u64) -> u64 {
        self.downloaded.fetch_add(delta, Ordering::SeqCst) + delta
    }

    fn downloaded(&self) -> u64 {
        self.downloaded.load(Ordering::SeqCst)
    }

    fn limit(&self) -> u64 {
        self.limit.load(Ordering::SeqCst)
    }

    fn set_limit(&self, new_limit: u64) {
        self.limit.store(new_limit, Ordering::SeqCst);
    }
}

struct PartOutcome {
    ulid: String,
    final_size: u64,
}

enum PartEvent {
    Completed(PartOutcome),
    NeedsReschedule { ulid: String },
}

struct BandwidthLimiter {
    rate: f64,
    state: Mutex<LimiterState>,
    seq: AtomicU64,
}

struct LimiterState {
    available: f64,
    last_refill: Instant,
    queue: VecDeque<u64>,
}

impl BandwidthLimiter {
    fn new(bytes_per_second: u64) -> Self {
        let rate = bytes_per_second.max(1) as f64;
        Self {
            rate,
            state: Mutex::new(LimiterState {
                available: rate,
                last_refill: Instant::now(),
                queue: VecDeque::new(),
            }),
            seq: AtomicU64::new(1),
        }
    }

    async fn acquire(&self, amount: u64) {
        let amount = amount as f64;

        // Get a sequence number and enqueue ourselves to ensure FIFO fairness.
        let my_seq = self.seq.fetch_add(1, Ordering::SeqCst);
        {
            let mut state = self.state.lock().await;
            state.queue.push_back(my_seq);
        }

        loop {
            let mut state = self.state.lock().await;
            state.refill(self.rate);

            // If we're at the front of the queue and enough tokens are available, consume and go.
            if let Some(&front) = state.queue.front() {
                if front == my_seq && state.available >= amount {
                    state.available -= amount;
                    state.queue.pop_front();
                    return;
                }
            }

            // Compute a sensible sleep time. If there's a deficit for our request, wait
            // the time required to refill that deficit. Otherwise yield to the scheduler
            // to let the front of the queue make progress without waiting real time.
            let sleep_duration = if state.available < amount {
                let deficit = amount - state.available;
                let wait_secs = deficit / self.rate;
                let dur = match Duration::try_from_secs_f64(wait_secs) {
                    Ok(d) => d.max(Duration::from_millis(1)),
                    Err(_) => Duration::from_millis(1),
                };
                Some(dur)
            } else {
                None
            };

            drop(state);
            if let Some(dur) = sleep_duration {
                time::sleep(dur).await;
            } else {
                // Yield to the scheduler; does NOT advance tokio::time (important for time-paused tests)
                tokio::task::yield_now().await;
            }
        }
    }
}

impl LimiterState {
    fn refill(&mut self, rate: f64) {
        let now = Instant::now();
        let elapsed = now - self.last_refill;
        self.last_refill = now;
        let replenished = elapsed.as_secs_f64() * rate;
        self.available = (self.available + replenished).min(rate);
    }
}

#[allow(clippy::too_many_arguments)]
async fn download_part(
    client: Arc<Client>,
    instruction: Arc<Download>,
    part: PartDetails,
    controller: Arc<PartController>,
    randomize_user_agent: bool,
    aggregator_span: Span,
    speed_limiter: Option<Arc<BandwidthLimiter>>,
    probe_notify: Option<Arc<Notify>>,
) -> Result<PartEvent, OdlError> {
    let PartDetails {
        offset, size, ulid, ..
    } = part;
    let part_path = instruction.part_path(&ulid);
    let url = instruction.url().clone();
    let mut current_size = controller.downloaded();
    let target_size = controller.limit();

    let current_span = Span::current();
    current_span.pb_set_length(target_size);
    current_span.pb_set_position(current_size);
    current_span.pb_reset_eta();

    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&part_path)
        .await?;

    if current_size >= target_size {
        return Ok(PartEvent::Completed(PartOutcome {
            ulid,
            final_size: target_size,
        }));
    }

    let mut file = BufWriter::new(file);

    let mut req = client.get(url);
    let range_header = format!("bytes={}-{}", offset + current_size, offset + size - 1,);
    let range_value = HeaderValue::from_str(&range_header).map_err(|e| OdlError::Other {
        message: "Internal Error: Invalid range header".to_string(),
        origin: Box::new(e),
    })?;
    req = req.header(RANGE, range_value);
    if randomize_user_agent {
        req = req.header(USER_AGENT, random_user_agent())
    }

    let mut resp = req.send().await.map_err(OdlError::from)?;

    let mut started_notified = false;
    loop {
        let allow_until = controller.limit();
        if controller.downloaded() >= allow_until {
            break;
        }

        let chunk_result = time::timeout(STALE_CONNECTION_TIMEOUT, resp.chunk()).await;
        let maybe_chunk = match chunk_result {
            Ok(chunk) => chunk.map_err(OdlError::from)?,
            Err(_) => {
                file.flush().await?;
                return Ok(PartEvent::NeedsReschedule { ulid });
            }
        };

        let mut chunk = match maybe_chunk {
            Some(chunk) => chunk,
            None => break,
        };

        // Signal that we've started receiving data for this probe connection.
        if !started_notified {
            if let Some(n) = probe_notify.as_ref() {
                n.notify_one();
            }
            started_notified = true;
        }

        let downloaded = controller.downloaded();
        let remaining = allow_until.saturating_sub(downloaded);
        if chunk.len() as u64 > remaining {
            chunk = chunk.split_to(remaining as usize);
        }

        let len = chunk.len() as u64;
        if let Some(limiter) = speed_limiter.as_ref() {
            limiter.acquire(len).await;
        }
        file.write_all(&chunk).await?;
        aggregator_span.pb_inc(len);
        current_span.pb_inc(len);
        controller.record_progress(len);
        current_size += len;
    }

    file.flush().await?;

    if controller.downloaded() >= controller.limit() {
        Ok(PartEvent::Completed(PartOutcome {
            ulid,
            final_size: controller.limit(),
        }))
    } else {
        Ok(PartEvent::NeedsReschedule { ulid })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::DownloadBuilder;
    use futures::FutureExt;
    use mockito::{Matcher, Server};
    use reqwest::Url;
    use tempfile::tempdir;
    use tokio::{fs, time};
    use tracing_indicatif::IndicatifLayer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    const TEST_FILENAME: &str = "test.bin";

    fn make_part(ulid: &str, offset: u64, size: u64) -> PartDetails {
        PartDetails {
            offset,
            size,
            ulid: ulid.to_string(),
            finished: false,
        }
    }

    async fn create_instruction(
        download_dir: &std::path::Path,
        save_dir: &std::path::Path,
        url: &str,
        size: u64,
        parts: HashMap<String, PartDetails>,
        max_connections: u64,
    ) -> Arc<Download> {
        let download = DownloadBuilder::default()
            .download_dir(download_dir.to_path_buf())
            .save_dir(save_dir.to_path_buf())
            .filename(TEST_FILENAME.to_string())
            .url(Url::parse(url).expect("valid url"))
            .size(Some(size))
            .parts(parts)
            .max_connections(max_connections)
            .is_resumable(true)
            .build()
            .expect("build download");
        Arc::new(download)
    }

    async fn read_metadata(instruction: &Download) -> DownloadMetadata {
        let bytes = fs::read(instruction.metadata_path())
            .await
            .expect("metadata file present");
        DownloadMetadata::decode_length_delimited(&*bytes).expect("decode metadata")
    }

    #[tokio::test]
    async fn test_downloader_downloads_single_part() -> Result<(), Box<dyn std::error::Error>> {
        let file_content = b"HelloDownloader";
        let mut server = Server::new_async().await;
        let base = server.url();
        let get_mock = server
            .mock("GET", "/file")
            .match_header(
                "range",
                Matcher::Exact(format!("bytes=0-{}", file_content.len() - 1)),
            )
            .with_status(206)
            .with_body(file_content)
            .create_async()
            .await;

        let tmp = tempdir()?;
        let download_dir = tmp.path().join("download");
        let save_dir = tmp.path().join("save");
        fs::create_dir_all(&download_dir).await?;
        fs::create_dir_all(&save_dir).await?;

        let mut parts = HashMap::new();
        parts.insert(
            "part1".to_string(),
            make_part("part1", 0, file_content.len() as u64),
        );

        let instruction = create_instruction(
            &download_dir,
            &save_dir,
            &format!("{}/file", base),
            file_content.len() as u64,
            parts,
            1,
        )
        .await;

        let metadata = instruction.as_metadata();
        let downloader = Downloader::new(
            Arc::clone(&instruction),
            metadata,
            reqwest::Client::builder().build()?,
            false,
            Span::current(),
            None,
        );
        let updated_metadata = downloader.run().await?;

        let part_bytes = fs::read(instruction.part_path("part1")).await?;
        assert_eq!(part_bytes, file_content);
        assert!(
            updated_metadata
                .parts
                .get("part1")
                .map(|p| p.finished)
                .unwrap_or(false)
        );
        assert!(fs::try_exists(instruction.metadata_path()).await?);
        get_mock.assert_async().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_downloader_split_persists_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempdir()?;
        let download_dir = tmp.path().join("download");
        let save_dir = tmp.path().join("save");
        fs::create_dir_all(&download_dir).await?;
        fs::create_dir_all(&save_dir).await?;

        // Ensure an IndicatifLayer is registered so progress-bar methods have an effect
        // in tests. Use `try_init()` and ignore the result so repeated test runs don't panic
        // if a global subscriber is already set.
        tracing_subscriber::registry()
            .with(IndicatifLayer::new())
            .init();

        let mut parts = HashMap::new();
        let original_size = MIN_DYNAMIC_SPLIT_SIZE * 4;
        parts.insert("orig".to_string(), make_part("orig", 0, original_size));

        let instruction = create_instruction(
            &download_dir,
            &save_dir,
            "http://example.com/file",
            original_size,
            parts,
            2,
        )
        .await;
        let metadata = instruction.as_metadata();
        let aggregator_span = info_span!("aggregator");
        let downloader = Downloader::new(
            Arc::clone(&instruction),
            metadata,
            reqwest::Client::builder().build()?,
            false,
            aggregator_span.clone(),
            None,
        );
        // make eta larger than 1 min
        aggregator_span.pb_start();
        // Choose a large total length and a small progress so the computed ETA
        // will be much larger than `MIN_DYNAMIC_SPLIT_ETA` (used by `split_task`).
        aggregator_span.pb_set_length(120_000);
        aggregator_span.pb_inc(1);
        // give the progress bar a tiny moment to record elapsed time so ETA can be computed
        time::sleep(Duration::from_millis(100)).await;
        assert!(aggregator_span.pb_eta() > MIN_DYNAMIC_SPLIT_ETA);

        let controller = Arc::new(PartController::new(original_size, 0));
        let candidate = SplitCandidate {
            ulid: "orig".to_string(),
            controller: Arc::clone(&controller),
        };

        let split_result = downloader.split_task(&candidate).await?;
        assert!(split_result.is_some());
        let persisted = read_metadata(&instruction).await;
        assert_eq!(persisted.parts.len(), 2);
        assert!(persisted.parts.values().any(|p| p.ulid != "orig"));
        Ok(())
    }

    #[tokio::test]
    async fn test_downloader_mark_part_finished_persists() -> Result<(), Box<dyn std::error::Error>>
    {
        let tmp = tempdir()?;
        let download_dir = tmp.path().join("download");
        let save_dir = tmp.path().join("save");
        fs::create_dir_all(&download_dir).await?;
        fs::create_dir_all(&save_dir).await?;

        let mut parts = HashMap::new();
        parts.insert("p1".to_string(), make_part("p1", 0, 1024));
        let instruction = create_instruction(
            &download_dir,
            &save_dir,
            "http://example.com/file",
            1024,
            parts,
            1,
        )
        .await;
        let metadata = instruction.as_metadata();
        let downloader = Downloader::new(
            Arc::clone(&instruction),
            metadata,
            reqwest::Client::builder().build()?,
            false,
            Span::current(),
            None,
        );

        let outcome = PartOutcome {
            ulid: "p1".to_string(),
            final_size: 1024,
        };
        downloader.mark_part_finished(&outcome).await?;
        let persisted = read_metadata(&instruction).await;
        let part = persisted.parts.get("p1").expect("part exists");
        assert!(part.finished);
        assert_eq!(part.size, 1024);
        Ok(())
    }

    #[tokio::test]
    async fn test_download_part_returns_reschedule_on_short_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempdir()?;
        let download_dir = tmp.path().join("download");
        let save_dir = tmp.path().join("save");
        fs::create_dir_all(&download_dir).await?;
        fs::create_dir_all(&save_dir).await?;

        let mut server = Server::new_async().await;
        let base = server.url();
        let file_content = b"12"; // intentionally shorter than requested
        let get_mock = server
            .mock("GET", "/partial")
            .match_header("range", Matcher::Exact("bytes=0-4".into()))
            .with_status(206)
            .with_body(file_content)
            .create_async()
            .await;

        let mut parts = HashMap::new();
        parts.insert("part".to_string(), make_part("part", 0, 5));
        let instruction = create_instruction(
            &download_dir,
            &save_dir,
            &format!("{}/partial", base),
            5,
            parts,
            1,
        )
        .await;

        let metadata = instruction.as_metadata();
        let part = metadata.parts.get("part").unwrap().clone();
        let controller = Arc::new(PartController::new(part.size, 0));
        let event = download_part(
            Arc::new(reqwest::Client::builder().build()?),
            Arc::clone(&instruction),
            part,
            controller,
            false,
            Span::current(),
            None,
            None,
        )
        .await?;

        match event {
            PartEvent::NeedsReschedule { ulid } => assert_eq!(ulid, "part"),
            PartEvent::Completed(_) => panic!("expected reschedule"),
        }
        get_mock.assert_async().await;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_bandwidth_limiter_enforces_limit() {
        let limiter = BandwidthLimiter::new(1024);
        limiter.acquire(1024).await;

        let second = limiter.acquire(1024);
        tokio::pin!(second);
        assert!(second.as_mut().now_or_never().is_none());

        time::advance(Duration::from_millis(900)).await;
        assert!(second.as_mut().now_or_never().is_none());

        time::advance(Duration::from_millis(200)).await;
        assert!(second.as_mut().now_or_never().is_some());
    }
}
