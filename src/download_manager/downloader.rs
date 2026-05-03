use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use reqwest::{
    Client,
    header::{HeaderValue, RANGE, USER_AGENT},
};
use tokio::{
    fs,
    sync::{Mutex, Notify, mpsc},
    task::JoinSet,
    time::{self, Duration, Instant},
};
use tracing::{Instrument, info_span};
use ulid::Ulid;

use prost::Message;

use crate::progress::{
    DownloadContext, ProgressEvent, ProgressTracker, SAMPLE_INTERVAL, speed_window_rate,
    trim_speed_window,
};
use crate::retry_policies::{FixedThenExponentialRetry, wait_for_retry};
use crate::{
    download::Download,
    download_manager::io::persist_encoded_metadata,
    download_metadata::{DownloadMetadata, PartDetails},
    error::{MetadataError, OdlError},
    user_agents::random_user_agent,
};

/// Sliding window length used by the speed sampler. Chosen long enough
/// to bridge normal chunk-arrival jitter (TCP windowing, head-of-line
/// reads on a multiplexed connection) so the rendered rate stays
/// stable, short enough to react quickly when the network actually
/// changes.
const SPEED_WINDOW: Duration = Duration::from_millis(1500);

/// Minimum chunk size we keep on a single task before attempting another split.
const MIN_DYNAMIC_SPLIT_SIZE: u64 = 3 * 1024 * 1024; // 3 MB
/// Minimum eta needed for dynamic split to happen. any eta less than this will skip creating more chunks
/// as it will be inefficient
#[cfg(not(test))]
const MIN_DYNAMIC_SPLIT_ETA: Duration = Duration::from_secs(60);
#[cfg(test)]
const MIN_DYNAMIC_SPLIT_ETA: Duration = Duration::from_secs(0);

#[cfg(not(test))]
const MIN_DYNAMIC_SPLIT_ELAPSED: Duration = Duration::from_secs(15);
#[cfg(test)]
const MIN_DYNAMIC_SPLIT_ELAPSED: Duration = Duration::from_millis(0);

#[cfg(not(test))]
const STALE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const STALE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Coordinates how parts are downloaded, including dynamic splitting to keep
/// all available connections busy.
pub struct Downloader {
    instruction: Arc<Download>,
    metadata: Arc<Mutex<DownloadMetadata>>,
    client: Arc<Client>,
    randomize_user_agent: bool,
    concurrency_limit: Arc<AtomicUsize>,
    speed_limiter: Option<Arc<BandwidthLimiter>>,
    retry_policy: FixedThenExponentialRetry,
    persist_mutex: Arc<Mutex<()>>,
    ctx: DownloadContext,
    tracker: Arc<ProgressTracker>,
    /// Snapshot of currently scheduled parts, shared with the speed
    /// sampler so it can emit per-part speed/progress on a fixed cadence
    /// (independent of the per-chunk hot path).
    active_parts: Arc<std::sync::Mutex<HashMap<String, Arc<PartController>>>>,
}

impl Downloader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instruction: Arc<Download>,
        metadata: DownloadMetadata,
        client: Client,
        randomize_user_agent: bool,
        speed_limit: Option<u64>,
        retry_policy: FixedThenExponentialRetry,
        ctx: DownloadContext,
    ) -> Self {
        let concurrency_limit = metadata.max_connections as usize;
        let speed_limiter = speed_limit
            .filter(|limit| *limit > 0)
            .map(|limit| Arc::new(BandwidthLimiter::new(limit)));
        let total = metadata.size;
        let tracker = Arc::new(ProgressTracker::new(total));
        // seed tracker with bytes already on disk for parts marked finished
        let already_done: u64 = metadata
            .parts
            .values()
            .filter(|p| p.finished)
            .map(|p| p.size)
            .sum();
        if already_done > 0 {
            tracker.advance(already_done);
        }
        Self {
            instruction,
            metadata: Arc::new(Mutex::new(metadata)),
            client: Arc::new(client),
            randomize_user_agent,
            concurrency_limit: Arc::new(AtomicUsize::new(if concurrency_limit == 0 {
                1
            } else {
                concurrency_limit
            })),
            speed_limiter,
            retry_policy,
            persist_mutex: Arc::new(Mutex::new(())),
            ctx,
            tracker,
            active_parts: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    pub async fn run(self) -> Result<DownloadMetadata, OdlError> {
        // Pre-seed tracker with on-disk bytes for unfinished parts so the
        // sampler's first emission already reflects resumed state and the
        // UI never flashes backwards. `Downloader::new` only counted
        // bytes from `finished` parts.
        self.seed_tracker_with_unfinished_parts().await;
        // Spawn a fast progress sampler that emits raw, un-smoothed speed
        // and aggregate progress at SAMPLE_INTERVAL cadence (8 Hz). Decoupled
        // from per-chunk hot path so CPU stays low even with thousands of
        // tiny chunks per second.
        let sampler_handle = self.spawn_speed_sampler();
        let result = self.run_inner().await;
        sampler_handle.abort();
        result
    }

    async fn seed_tracker_with_unfinished_parts(&self) {
        let parts: Vec<PartDetails> = {
            let metadata = self.metadata.lock().await;
            metadata
                .parts
                .values()
                .filter(|p| !p.finished)
                .cloned()
                .collect()
        };
        let mut total_existing: u64 = 0;
        for p in parts {
            if let Ok(existing) = self.detect_existing_size(&p).await {
                total_existing = total_existing.saturating_add(existing.min(p.size));
            }
        }
        if total_existing > 0 {
            self.tracker.advance(total_existing);
            self.ctx.emit(ProgressEvent::Progress {
                downloaded: self.tracker.downloaded(),
                total: self.tracker.total(),
            });
        }
    }

    fn spawn_speed_sampler(&self) -> tokio::task::JoinHandle<()> {
        let tracker = Arc::clone(&self.tracker);
        let ctx = self.ctx.clone();
        let active = Arc::clone(&self.active_parts);
        tokio::spawn(async move {
            // Sliding window of (timestamp, cumulative_bytes) snapshots.
            // Speed is computed across the whole window so chunk-arrival
            // jitter at the 125 ms tick boundary doesn't pulse the
            // displayed rate down to 0 between bursts.
            let mut agg_window: VecDeque<(std::time::Instant, u64)> = VecDeque::new();
            let mut part_windows: HashMap<String, VecDeque<(std::time::Instant, u64)>> =
                HashMap::new();
            agg_window.push_back((std::time::Instant::now(), tracker.downloaded()));
            loop {
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return,
                    _ = time::sleep(SAMPLE_INTERVAL) => {}
                }
                let now = std::time::Instant::now();
                let cur = tracker.downloaded();
                agg_window.push_back((now, cur));
                trim_speed_window(&mut agg_window, now, SPEED_WINDOW);
                if let Some(bps) = speed_window_rate(&agg_window) {
                    ctx.emit(ProgressEvent::Speed {
                        bytes_per_second: bps,
                    });
                }
                ctx.emit(ProgressEvent::Progress {
                    downloaded: cur,
                    total: tracker.total(),
                });

                // Per-part snapshot. Emit at sampler cadence so the per-chunk
                // hot path stays cheap and the UI gets a steady update rate.
                let snapshot: Vec<(String, Arc<PartController>)> = {
                    let map = active.lock().unwrap();
                    map.iter()
                        .map(|(k, v)| (k.clone(), Arc::clone(v)))
                        .collect()
                };
                let mut seen_parts = std::collections::HashSet::with_capacity(snapshot.len());
                for (ulid, controller) in snapshot {
                    let part_cur = controller.downloaded();
                    let part_lim = controller.limit();
                    let win = part_windows.entry(ulid.clone()).or_default();
                    win.push_back((now, part_cur));
                    trim_speed_window(win, now, SPEED_WINDOW);
                    if let Some(bps) = speed_window_rate(win) {
                        ctx.emit(ProgressEvent::PartSpeed {
                            ulid: ulid.clone(),
                            bytes_per_second: bps,
                        });
                    }
                    ctx.emit(ProgressEvent::PartProgress {
                        ulid: ulid.clone(),
                        downloaded: part_cur,
                        total: part_lim,
                    });
                    seen_parts.insert(ulid);
                }
                // Drop windows for parts no longer active so the map
                // doesn't grow unbounded across long downloads with
                // dynamic splits.
                part_windows.retain(|k, _| seen_parts.contains(k));
            }
        })
    }

    async fn run_inner(self) -> Result<DownloadMetadata, OdlError> {
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
            // or the task finishes (e.g., zero-length part completes immediately),
            // or the caller cancels the download.
            tokio::select! {
                _ = probe.notified() => {}
                maybe_res = join_set.join_next() => {
                    if let Some(res) = maybe_res {
                        self.handle_join_result_item(res, &mut pending, &mut active, &mut join_set).await?;
                    }
                }
                _ = self.ctx.cancel.cancelled() => {
                    join_set.shutdown().await;
                    return Err(OdlError::Cancelled);
                }
            }
        }

        // Now fill remaining capacity up to configured concurrency.
        self.fill_capacity(&mut pending, &mut active, &mut join_set)
            .await?;

        loop {
            tokio::select! {
                _ = self.ctx.cancel.cancelled() => {
                    join_set.shutdown().await;
                    return Err(OdlError::Cancelled);
                }
                next = join_set.join_next() => {
                    let Some(result) = next else { break };
                    self.handle_join_result_item(result, &mut pending, &mut active, &mut join_set)
                        .await?;
                    self.fill_capacity(&mut pending, &mut active, &mut join_set)
                        .await?;
                }
            }
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
        let cap = self.concurrency_limit.load(Ordering::SeqCst);
        if cap == 0 {
            return Ok(());
        }

        self.ensure_pending_pool(pending, active).await?;

        while active.len() < cap {
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
                    self.active_parts.lock().unwrap().remove(&outcome.ulid);
                    self.mark_part_finished(&outcome).await?;
                }
                PartEvent::NeedsReschedule { ulid } => {
                    if let Some(task) = active.remove(&ulid) {
                        self.active_parts.lock().unwrap().remove(&ulid);
                        pending.push_back(task.details);
                    }
                }
                PartEvent::Failed { ulid, attempts } => {
                    // Remove from active and attempt to reschedule this part
                    // later if there are other unfinished parts. If this was
                    // the last unfinished part, fail the overall download.
                    self.active_parts.lock().unwrap().remove(&ulid);
                    if let Some(task) = active.remove(&ulid) {
                        if pending.is_empty() && active.is_empty() {
                            // No other work to do — all parts have failed
                            join_set.shutdown().await;
                            return Err(OdlError::Other {
                                message: format!(
                                    "All parts failed; last part {} failed after {} attempts",
                                    ulid, attempts
                                ),
                                origin: Box::new(std::io::Error::other("all parts failed")),
                            });
                        } else {
                            // There are other pending/active parts; requeue this
                            // failed part so it will be retried later (one-by-one
                            // as capacity frees up when other parts finish).
                            pending.push_back(task.details);
                            // Reduce concurrency to avoid scheduling too many
                            // simultaneous connections if the server only
                            // allows a small number. Ensure minimum of 1.
                            let _ = self.concurrency_limit.fetch_update(
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                |cur| {
                                    if cur > 1 { Some(cur - 1) } else { Some(1) }
                                },
                            );
                        }
                    } else {
                        // If the task wasn't in `active`, still check whether
                        // everything else is done and fail if so.
                        if pending.is_empty() && active.is_empty() {
                            join_set.shutdown().await;
                            return Err(OdlError::Other {
                                message: format!(
                                    "All parts failed; last part {} failed after {} attempts",
                                    ulid, attempts
                                ),
                                origin: Box::new(std::io::Error::other("all parts failed")),
                            });
                        }
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
        let spare_capacity = self
            .concurrency_limit
            .load(Ordering::SeqCst)
            .saturating_sub(active.len());
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
        // NOTE: bytes already on disk are counted into the aggregate
        // tracker by `seed_tracker_with_unfinished_parts` at run start.
        // Per-chunk `tracker.advance` in `download_part` covers everything
        // downloaded after that, so do not advance here on (re)schedule.
        self.ctx.emit(ProgressEvent::PartAdded {
            ulid: part.ulid.clone(),
            offset: part.offset,
            size: part.size,
        });
        let controller = Arc::new(PartController::new(part.size, initial_downloaded));
        let task_part = part.clone();
        let controller_clone = Arc::clone(&controller);
        let client = Arc::clone(&self.client);
        let instruction = Arc::clone(&self.instruction);
        let randomize_user_agent = self.randomize_user_agent;
        let speed_limiter = self.speed_limiter.clone();
        let span_ulid = task_part.ulid.clone();
        let part_span = info_span!("part", ulid = span_ulid.as_str());
        let ctx = self.ctx.clone();
        let tracker = Arc::clone(&self.tracker);
        let retry_policy = self.retry_policy;

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
                    speed_limiter,
                    probe_for_task,
                    retry_policy,
                    ctx,
                    tracker,
                )
                .await
            }
            .instrument(part_span),
        );

        self.active_parts
            .lock()
            .unwrap()
            .insert(part.ulid.clone(), Arc::clone(&controller));
        active.insert(
            part.ulid.clone(),
            ActiveTask {
                details: part,
                controller,
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
        // If estimated time to finish entire download is <= 60s,
        // Or if elapsed time is under 15 seconds
        // avoid splitting as it will be inefficient
        if self.tracker.elapsed() <= MIN_DYNAMIC_SPLIT_ELAPSED
            || self.tracker.eta() <= MIN_DYNAMIC_SPLIT_ETA
        {
            return Ok(None);
        }

        let downloaded = candidate.controller.downloaded();
        let current_limit = candidate.controller.limit();
        if current_limit <= downloaded + MIN_DYNAMIC_SPLIT_SIZE * 2 {
            return Ok(None);
        }

        let remaining = current_limit - downloaded;
        // Round new_limit down to a cluster boundary so the new part's absolute
        // offset stays aligned for reflink-based assembly.
        let mask = Download::ASSEMBLY_CLUSTER_SIZE - 1;
        let new_limit = (downloaded + remaining / 2) & !mask;
        if new_limit <= downloaded {
            return Ok(None);
        }
        let split_size = current_limit - new_limit;
        if split_size < MIN_DYNAMIC_SPLIT_SIZE || new_limit - downloaded < MIN_DYNAMIC_SPLIT_SIZE {
            return Ok(None);
        }
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

        self.ctx.emit(ProgressEvent::PartAdded {
            ulid: new_part.ulid.clone(),
            offset: new_part.offset,
            size: new_part.size,
        });

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
    Failed { ulid: String, attempts: u32 },
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
            if let Some(&front) = state.queue.front()
                && front == my_seq
                && state.available >= amount
            {
                state.available -= amount;
                state.queue.pop_front();
                return;
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

/// Buffer size for the per-part writer thread.
///
/// On Windows we use 256 KiB: each `WriteFile` re-enters the kernel and
/// passes through filter drivers (Defender, EDR, indexer), so a small
/// buffer at high throughput burns measurable CPU. 256 KiB cuts syscall
/// rate ~16× vs 16 KiB while keeping crash-loss bounded.
///
/// On Unix the page cache absorbs small writes cheaply (no per-write
/// filter pipeline), so we use 32 KiB (4× tokio's `BufWriter` default
/// of 8 KiB) — enough to coalesce most chunks while keeping crash-loss
/// tighter than the Windows value.
#[cfg(windows)]
const PART_WRITER_BUF_SIZE: usize = 256 * 1024;
#[cfg(not(windows))]
const PART_WRITER_BUF_SIZE: usize = 32 * 1024;

/// Bound on the in-flight chunk channel between the async receive loop
/// and the blocking writer thread. Provides backpressure: if the writer
/// can't keep up the receiver awaits, throttling the network read.
///
/// Per-part memory ceiling is roughly `PART_WRITER_CHANNEL_CAP *
/// max_chunk_size` (reqwest chunks are typically ≤ 16 KiB → ~1 MiB per
/// part), multiplied by the number of concurrently downloading parts.
const PART_WRITER_CHANNEL_CAP: usize = 64;

/// Owns a dedicated blocking thread that drains chunks from `tx` and
/// writes them to a `std::fs::File` via a large `BufWriter`. This keeps
/// file IO entirely off the async runtime — no `tokio::fs` `spawn_blocking`
/// hop per chunk — which on Windows is the dominant cost (each
/// `tokio::fs` call is a thread bounce; with thousands of small chunks
/// per second across multiple parts the blocking pool serializes the
/// hot path).
struct PartFileWriter {
    tx: Option<mpsc::Sender<Bytes>>,
    handle: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl PartFileWriter {
    /// Open the part file, seek to end (resume), and start the writer thread.
    async fn open(part_path: std::path::PathBuf) -> std::io::Result<Self> {
        let file = tokio::task::spawn_blocking(move || -> std::io::Result<std::fs::File> {
            use std::io::Seek;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&part_path)?;
            f.seek(std::io::SeekFrom::End(0))?;
            Ok(f)
        })
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))??;

        let (tx, mut rx) = mpsc::channel::<Bytes>(PART_WRITER_CHANNEL_CAP);
        let handle = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            use std::io::Write;
            let mut writer = std::io::BufWriter::with_capacity(PART_WRITER_BUF_SIZE, file);
            while let Some(chunk) = rx.blocking_recv() {
                writer.write_all(&chunk)?;
            }
            writer.flush()?;
            Ok(())
        });

        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
        })
    }

    async fn write(&mut self, chunk: Bytes) -> std::io::Result<()> {
        let tx = self
            .tx
            .as_ref()
            .expect("PartFileWriter::write after finish");
        if tx.send(chunk).await.is_err() {
            // Writer thread ended (likely an IO error). Surface it via finish.
            return self.finish().await;
        }
        Ok(())
    }

    /// Close the channel and await the writer thread, returning any IO
    /// error it produced. Safe to call multiple times; subsequent calls
    /// are no-ops.
    async fn finish(&mut self) -> std::io::Result<()> {
        self.tx.take();
        if let Some(h) = self.handle.take() {
            match h.await {
                Ok(r) => r,
                Err(e) => Err(std::io::Error::other(e.to_string())),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for PartFileWriter {
    fn drop(&mut self) {
        // All exit paths in `download_part` are expected to call `finish().await`
        // so that any IO error from the writer thread is surfaced rather than
        // swallowed. Dropping with `tx`/`handle` still set means a path was
        // missed — flag in debug builds.
        debug_assert!(
            self.tx.is_none() && self.handle.is_none(),
            "PartFileWriter dropped without finish(); writer thread errors will be swallowed"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn download_part(
    client: Arc<Client>,
    instruction: Arc<Download>,
    part: PartDetails,
    controller: Arc<PartController>,
    randomize_user_agent: bool,
    speed_limiter: Option<Arc<BandwidthLimiter>>,
    probe_notify: Option<Arc<Notify>>,
    policy: FixedThenExponentialRetry,
    ctx: DownloadContext,
    tracker: Arc<ProgressTracker>,
) -> Result<PartEvent, OdlError> {
    if ctx.is_cancelled() {
        return Err(OdlError::Cancelled);
    }
    let PartDetails {
        offset, size, ulid, ..
    } = part;
    let part_path = instruction.part_path(&ulid);
    let url = instruction.url().clone();
    let mut current_size;
    let target_size = controller.limit();

    let mut attempts: u32 = 0;

    loop {
        // Recompute current size (in case previous attempts wrote some bytes)
        current_size = controller.downloaded();

        // Open file for this attempt. We delegate all IO to a dedicated
        // blocking writer thread (`PartFileWriter`) to keep file writes
        // entirely off the async runtime — on Windows each `tokio::fs`
        // call is a thread bounce, and with thousands of small chunks
        // per second the blocking pool serializes the hot path.
        // No `append`: only one writer per part, so O_APPEND atomicity
        // is unneeded, and Windows `FILE_APPEND_DATA` re-resolves EOF on
        // every WriteFile. Seek-to-end once on open handles resume.
        let mut file = match PartFileWriter::open(part_path.clone()).await {
            Ok(w) => w,
            Err(e) => {
                return Err(OdlError::StdIoError {
                    e,
                    extra_info: Some(format!("Failed to open part file {}", part_path.display())),
                });
            }
        };

        if current_size >= target_size {
            file.finish().await?;
            ctx.emit(ProgressEvent::PartFinished { ulid: ulid.clone() });
            return Ok(PartEvent::Completed(PartOutcome {
                ulid,
                final_size: target_size,
            }));
        }

        // build request for the remaining range
        let mut req = client.get(url.clone());
        let range_header = format!("bytes={}-{}", offset + current_size, offset + size - 1,);
        let range_value = match HeaderValue::from_str(&range_header) {
            Ok(v) => v,
            Err(e) => {
                let _ = file.finish().await;
                return Err(OdlError::Other {
                    message: "Internal Error: Invalid range header".to_string(),
                    origin: Box::new(e),
                });
            }
        };
        req = req.header(RANGE, range_value);
        if randomize_user_agent {
            req = req.header(USER_AGENT, random_user_agent())
        }

        // send request. wrap send in a timeout so network/connect hangs are
        // treated like other transient network errors and retried by policy.
        let send_result = time::timeout(STALE_CONNECTION_TIMEOUT, req.send()).await;

        let mut resp = match send_result {
            // request completed and returned a response
            Ok(Ok(r)) => r,
            // request completed but returned an error (network error)
            Ok(Err(_e)) => {
                file.finish().await?;
                match retry_sleep_or_fail_part(&policy, attempts, attempts + 1, &ctx, &ulid).await {
                    Ok(()) => {
                        attempts = attempts.saturating_add(1);
                        continue;
                    }
                    Err(failed) => return Ok(failed),
                }
            }
            // send timed out
            Err(_) => {
                // flush any partial progress to disk before retrying
                file.finish().await?;
                match retry_sleep_or_fail_part(&policy, attempts, attempts + 1, &ctx, &ulid).await {
                    Ok(()) => {
                        attempts = attempts.saturating_add(1);
                        continue;
                    }
                    Err(failed) => return Ok(failed),
                }
            }
        };

        let mut started_notified = false;
        let mut saw_eof = false;
        loop {
            let allow_until = controller.limit();
            if controller.downloaded() >= allow_until {
                break;
            }

            let chunk_result = time::timeout(STALE_CONNECTION_TIMEOUT, resp.chunk()).await;
            let maybe_chunk = match chunk_result {
                Ok(chunk_res) => match chunk_res.map_err(OdlError::from) {
                    Ok(opt) => opt,
                    Err(_e) => {
                        // network/body error -> consider retrying
                        file.finish().await?;
                        match retry_sleep_or_fail_part(&policy, attempts, attempts + 1, &ctx, &ulid)
                            .await
                        {
                            Ok(()) => {
                                attempts = attempts.saturating_add(1);
                                break;
                            }
                            Err(failed) => return Ok(failed),
                        }
                    }
                },
                Err(_) => {
                    // timeout reading chunk -> retry according to policy
                    file.finish().await?;
                    match retry_sleep_or_fail_part(&policy, attempts, attempts + 1, &ctx, &ulid)
                        .await
                    {
                        Ok(()) => {
                            attempts = attempts.saturating_add(1);
                            break;
                        }
                        Err(failed) => return Ok(failed),
                    }
                }
            };

            let mut chunk = match maybe_chunk {
                Some(chunk) => chunk,
                None => {
                    // EOF / short body — do not attempt automatic retry here;
                    // match previous behavior and allow caller to reschedule.
                    saw_eof = true;
                    break;
                }
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
                tokio::select! {
                    _ = limiter.acquire(len) => {}
                    _ = ctx.cancel.cancelled() => {
                        let _ = file.finish().await;
                        return Err(OdlError::Cancelled);
                    }
                }
            }
            file.write(chunk).await?;
            controller.record_progress(len);
            tracker.advance(len);
            // Per-chunk progress events are intentionally NOT emitted here:
            // the sampler emits both aggregate and per-part progress at a
            // fixed cadence, which keeps the hot path cheap and the UI
            // update rate predictable on fast networks.
            if ctx.is_cancelled() {
                let _ = file.finish().await;
                return Err(OdlError::Cancelled);
            }
        }

        file.finish().await?;

        if controller.downloaded() >= controller.limit() {
            ctx.emit(ProgressEvent::PartFinished { ulid: ulid.clone() });
            return Ok(PartEvent::Completed(PartOutcome {
                ulid,
                final_size: controller.limit(),
            }));
        }

        // If we observed EOF (server closed the connection with less data
        // than requested), follow previous behavior: return NeedsReschedule
        // immediately so scheduler can handle rescheduling.
        if saw_eof {
            return Ok(PartEvent::NeedsReschedule { ulid });
        }

        // If we get here, it means we broke the inner loop to retry; loop again
        // The attempts counter may have been incremented in the branches above.
        // If no retry happened (shouldn't happen), increment to avoid infinite loop.
        attempts = attempts.saturating_add(1);
        match retry_sleep_or_fail_part(&policy, attempts, attempts, &ctx, &ulid).await {
            Ok(()) => continue,
            Err(failed) => return Ok(failed),
        }
    }
}

// Apply the retry policy: if it says `Retry` this sleeps until the retry
// time then returns `Ok(())`. If the policy says `DoNotRetry` it returns
// a `PartEvent::Failed` for the caller to surface/handle.
async fn retry_sleep_or_fail_part(
    policy: &FixedThenExponentialRetry,
    _attempts_for_policy: u32,
    attempts_display: u32,
    ctx: &DownloadContext,
    ulid: &str,
) -> Result<(), PartEvent> {
    ctx.emit(ProgressEvent::PartRetrying {
        ulid: ulid.to_string(),
        attempt: attempts_display,
    });
    if wait_for_retry(policy, attempts_display, ctx).await {
        Ok(())
    } else {
        Err(PartEvent::Failed {
            ulid: ulid.to_string(),
            attempts: attempts_display,
        })
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
            None,
            FixedThenExponentialRetry::default(),
            DownloadContext::new(),
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
        let downloader = Downloader::new(
            Arc::clone(&instruction),
            metadata,
            reqwest::Client::builder().build()?,
            false,
            None,
            FixedThenExponentialRetry::default(),
            DownloadContext::new(),
        );
        // Seed tracker so it has computable ETA: pretend we made some progress.
        downloader.tracker.set_total(Some(120_000));
        downloader.tracker.advance(1);
        // give tracker a tiny moment to record elapsed time so ETA can be computed
        time::sleep(Duration::from_millis(100)).await;
        assert!(downloader.tracker.eta() > MIN_DYNAMIC_SPLIT_ETA);

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
            None,
            FixedThenExponentialRetry::default(),
            DownloadContext::new(),
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
            None,
            None,
            FixedThenExponentialRetry::default(),
            DownloadContext::new(),
            Arc::new(ProgressTracker::new(Some(5))),
        )
        .await?;

        match event {
            PartEvent::NeedsReschedule { ulid } => assert_eq!(ulid, "part"),
            PartEvent::Completed(_) => panic!("expected reschedule"),
            PartEvent::Failed { ulid, attempts } => panic!(
                "unexpected failed part {} after {} attempts",
                ulid, attempts
            ),
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
