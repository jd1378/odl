use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio_util::sync::CancellationToken;

use bytes::Bytes;
use reqwest::{
    Client, StatusCode,
    header::{ACCEPT_ENCODING, CONTENT_RANGE, HeaderValue, RANGE, RETRY_AFTER, USER_AGENT},
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
    conflict::ServerConflict,
    download::Download,
    download_manager::io::persist_encoded_metadata,
    download_metadata::{DownloadMetadata, PartDetails},
    error::{ConflictError, MetadataError, OdlError},
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

/// Controls staggered opening of new connections. Some servers cap
/// sudden bursts of simultaneous connections per IP, dropping or
/// resetting the excess. When `enabled`, the downloader opens at most
/// `batch_size` new connections per round and waits a random delay in
/// `[delay_min, delay_max]` before opening the next round.
#[derive(Debug, Clone, Copy)]
pub struct RampupConfig {
    pub enabled: bool,
    pub batch_size: u64,
    pub delay_min: Duration,
    pub delay_max: Duration,
}

fn sample_rampup_delay(min: Duration, max: Duration) -> Duration {
    use rand::RngExt;
    if max <= min {
        return min;
    }
    let lo = min.as_nanos().min(u64::MAX as u128) as u64;
    let hi = max.as_nanos().min(u64::MAX as u128) as u64;
    let n = rand::rng().random_range(lo..=hi);
    Duration::from_nanos(n)
}

impl RampupConfig {
    /// Helper for tests / callers that want the legacy behavior of
    /// opening all available capacity at once.
    #[cfg(test)]
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            batch_size: 1,
            delay_min: Duration::ZERO,
            delay_max: Duration::ZERO,
        }
    }
}

/// Coordinates how parts are downloaded, including dynamic splitting to keep
/// all available connections busy.
pub struct Downloader {
    instruction: Arc<Download>,
    metadata: Arc<Mutex<DownloadMetadata>>,
    client: Arc<Client>,
    randomize_user_agent: bool,
    /// Whether to attempt mid-flight subdivision of long-running parts.
    dynamic_split: bool,
    rampup: RampupConfig,
    speed_limiter: Option<Arc<BandwidthLimiter>>,
    retry_policy: FixedThenExponentialRetry,
    persist_mutex: Arc<Mutex<()>>,
    ctx: DownloadContext,
    tracker: Arc<ProgressTracker>,
    /// Snapshot of currently scheduled parts, shared with the speed
    /// sampler so it can emit per-part speed/progress on a fixed cadence
    /// (independent of the per-chunk hot path).
    active_parts: Arc<std::sync::Mutex<HashMap<String, Arc<PartController>>>>,
    /// Gates whether `fill_capacity` is allowed to open more than one
    /// connection per batch. The probe (1 connection, scheduled in
    /// `run_inner` before any ramping) must successfully begin
    /// receiving data before we trust the server with parallel opens.
    /// If the probe fails — or any subsequent batch part fails before
    /// notifying — this flips false and the ramp falls back to a
    /// strict one-at-a-time, probe-gated cadence.
    ramp_armed: std::sync::atomic::AtomicBool,
}

impl Downloader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instruction: Arc<Download>,
        metadata: DownloadMetadata,
        client: Client,
        randomize_user_agent: bool,
        speed_limit: Option<u64>,
        dynamic_split: bool,
        rampup: RampupConfig,
        retry_policy: FixedThenExponentialRetry,
        ctx: DownloadContext,
    ) -> Self {
        let concurrency_limit = metadata.max_connections as usize;
        // Seed the live cap from metadata when the caller hasn't set
        // anything yet. A caller that pre-set `ctx.live.set_max_connections`
        // before download wins (seed_if_unset is a no-op when non-zero).
        ctx.live.seed_if_unset(concurrency_limit.max(1));
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
            // Splitting a part means asking for a range, so a server that
            // does not serve ranges is left with the one part it can answer —
            // whatever the caller asked for, and however much spare capacity
            // the connection cap appears to leave. Read before `metadata`
            // moves into the field below.
            dynamic_split: dynamic_split && metadata.is_resumable,
            metadata: Arc::new(Mutex::new(metadata)),
            client: Arc::new(client),
            randomize_user_agent,
            rampup,
            speed_limiter,
            retry_policy,
            persist_mutex: Arc::new(Mutex::new(())),
            ctx,
            tracker,
            active_parts: Arc::new(std::sync::Mutex::new(HashMap::new())),
            ramp_armed: std::sync::atomic::AtomicBool::new(true),
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
        let tracker = Arc::clone(&self.tracker);
        let ctx = self.ctx.clone();
        let result = self.run_inner().await;
        sampler_handle.abort();
        // The sampler ticks at 125 ms, so its last word almost always lands
        // short of the end — and nothing follows it, now that assembly reports
        // on its own row instead of restarting the aggregate. Say the transfer
        // is complete once, from the one place that knows it is.
        if result.is_ok() {
            ctx.emit(ProgressEvent::Progress {
                downloaded: tracker.total().unwrap_or_else(|| tracker.downloaded()),
                total: tracker.total(),
            });
        }
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
        let mut join_set: JoinSet<Result<PartEvent, OdlError>> = JoinSet::new();
        let outcome = self.drive_parts(&mut join_set).await;
        // No part task may outlive this call. Dropping the set only *asks*
        // them to stop, and a caller that reacts to the error by clearing the
        // work directory would then be racing tasks still writing into it.
        // `shutdown` aborts and joins, so by the time the error is returned
        // nothing is left holding a part.
        join_set.shutdown().await;
        outcome?;

        let metadata_mutex = Arc::try_unwrap(self.metadata).map_err(|_| {
            OdlError::MetadataError(MetadataError::Other {
                message: "Failed to unwrap metadata Arc".to_string(),
            })
        })?;
        Ok(metadata_mutex.into_inner())
    }

    /// Schedule and supervise every part until they are all finished, the
    /// caller cancels, or one of them fails terminally. Leaves teardown of
    /// `join_set` to [`Self::run_inner`], which does it on every path.
    async fn drive_parts(
        &self,
        join_set: &mut JoinSet<Result<PartEvent, OdlError>>,
    ) -> Result<(), OdlError> {
        let mut pending = self.pending_parts().await;
        let mut active: HashMap<String, ActiveTask> = HashMap::new();
        // Cause of the most recent exhausted part, so a download that ends
        // with work still queued reports what actually went wrong.
        let mut last_failure: Option<OdlError> = None;

        // Schedule a single probe connection first. Once it begins receiving data
        // we'll expand to fill the full concurrency capacity.
        if let Some(first_part) = pending.pop_front() {
            let probe = Arc::new(Notify::new());
            self.schedule_part(first_part, &mut active, join_set, Some(probe.clone()))
                .await?;

            // Wait until either the probe signals it has started receiving data,
            // or the task finishes (e.g., zero-length part completes immediately),
            // or the caller cancels the download.
            tokio::select! {
                _ = probe.notified() => {
                    // Probe is producing data — server is willing to
                    // serve us, so subsequent fill_capacity calls can
                    // ramp at the configured batch size.
                    self.ramp_armed.store(true, std::sync::atomic::Ordering::Relaxed);
                }
                maybe_res = join_set.join_next() => {
                    // Probe ended before notifying — treat the server
                    // as unhappy and force fill_capacity into strict
                    // one-at-a-time mode so we don't compound a bad
                    // situation with parallel opens.
                    self.ramp_armed.store(false, std::sync::atomic::Ordering::Relaxed);
                    if let Some(res) = maybe_res {
                        self.handle_join_result_item(res, &mut pending, &mut active, &mut last_failure).await?;
                    }
                }
                _ = self.ctx.cancel.cancelled() => {
                    return Err(OdlError::Cancelled);
                }
            }
        }

        // Now fill remaining capacity up to configured concurrency.
        self.fill_capacity(&mut pending, &mut active, join_set, &mut last_failure)
            .await?;

        loop {
            let live_changed = self.ctx.live.notified();
            tokio::pin!(live_changed);
            tokio::select! {
                _ = self.ctx.cancel.cancelled() => {
                    return Err(OdlError::Cancelled);
                }
                _ = &mut live_changed => {
                    self.apply_live_cap(&mut active);
                    self.fill_capacity(&mut pending, &mut active, join_set, &mut last_failure)
                        .await?;
                }
                next = join_set.join_next() => {
                    let Some(result) = next else { break };
                    self.handle_join_result_item(
                        result,
                        &mut pending,
                        &mut active,
                        &mut last_failure,
                    )
                    .await?;
                    self.fill_capacity(
                        &mut pending,
                        &mut active,
                        join_set,
                        &mut last_failure,
                    )
                    .await?;
                }
            }
        }

        // Nothing is in flight. That is only success if there is also nothing
        // left to do: when every running part exhausts its retries at once,
        // the requeued parts are still sitting in `pending` as the last task
        // drains the set. Ending there is right — the retry budget is the
        // caller's stated tolerance and it is spent — but reporting Ok was
        // not: it left the assembler to notice, which it did, as "part file
        // shorter than recorded size", an I/O error for a failed transfer.
        //
        // Cancellation is checked first rather than assumed away. The loop's
        // `select!` has no `biased`, so when the token fires with nothing left
        // in flight both arms are ready and tokio picks one at random — half
        // the time control arrives here instead of at the `Cancelled` arm.
        // Reporting a stopped download as a failure would be wrong twice over:
        // exit 1 instead of 130, and a caller that auto-retries failures would
        // restart what the user just stopped.
        if self.ctx.is_cancelled() {
            return Err(OdlError::Cancelled);
        }
        if !pending.is_empty() {
            // Report the transfer failure that got us here. It is the only
            // description with anything actionable in it — a caller routing on
            // the error kind needs to see the 503 or the timeout, not that odl
            // ran out of parts to schedule.
            let unfinished = pending.len();
            debug_assert!(last_failure.is_some(), "parts left over without a cause");
            return Err(last_failure.unwrap_or_else(|| OdlError::Other {
                message: format!("{unfinished} part(s) could not be downloaded"),
                origin: Box::new(std::io::Error::other("parts left unfinished")),
            }));
        }

        Ok(())
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
        last_failure: &mut Option<OdlError>,
    ) -> Result<(), OdlError> {
        if self.ctx.live.max_connections() == 0 {
            return Ok(());
        }

        self.ensure_pending_pool(pending, active).await?;

        if !self.rampup.enabled {
            // Legacy single-shot fill: open everything at once.
            while active.len() < self.ctx.live.max_connections() {
                let Some(part) = pending.pop_front() else {
                    return Ok(());
                };
                self.schedule_part(part, active, join_set, None).await?;
            }
            return Ok(());
        }

        // Ramped fill: open at most `batch_size` connections, wait for
        // every one of them to either signal "first chunk received" or
        // fail terminally, then sleep a random delay before opening
        // the next batch. Some servers throttle per-IP connection rate;
        // pacing + confirming each batch landed before the next gives
        // them a chance to settle. If any part fails before notifying,
        // we stop ramping for this round (the failed part is requeued
        // by `handle_join_result_item` and the main loop will retry).
        // Strict mode (after a failed probe / failed batch part) caps
        // every batch to a single connection so the next opens behave
        // like additional probes until something successfully starts.
        let batch_size = if self.ramp_armed.load(std::sync::atomic::Ordering::Relaxed) {
            self.rampup.batch_size.max(1)
        } else {
            1
        };
        loop {
            let cap = self.ctx.live.max_connections();
            if cap == 0 || active.len() >= cap {
                return Ok(());
            }

            let mut probes: Vec<Arc<Notify>> = Vec::new();
            let mut opened_in_batch: u64 = 0;
            while opened_in_batch < batch_size && active.len() < cap {
                let Some(part) = pending.pop_front() else {
                    break;
                };
                let probe = Arc::new(Notify::new());
                self.schedule_part(part, active, join_set, Some(probe.clone()))
                    .await?;
                probes.push(probe);
                opened_in_batch += 1;
            }
            if probes.is_empty() {
                return Ok(());
            }

            // Wait until every probe in this batch fires (or a part
            // fails / cancel arrives). Run the wait through a side
            // task + oneshot so the main `select!` can race it against
            // the shared `join_set`.
            let probes_for_task = probes.clone();
            let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
            tokio::spawn(async move {
                for p in probes_for_task.iter() {
                    p.notified().await;
                }
                let _ = tx.send(());
            });

            let mut batch_ok = false;
            loop {
                tokio::select! {
                    _ = &mut rx => {
                        batch_ok = true;
                        break;
                    }
                    res = join_set.join_next() => {
                        let Some(result) = res else {
                            return Ok(());
                        };
                        let is_failure = matches!(&result, Ok(Ok(PartEvent::Failed { .. })));
                        self.handle_join_result_item(result, pending, active, last_failure)
                            .await?;
                        if is_failure {
                            break;
                        }
                        // Completed / NeedsReschedule — keep waiting on
                        // the in-flight batch probes.
                    }
                    _ = self.ctx.cancel.cancelled() => {
                        return Ok(());
                    }
                }
            }
            if !batch_ok {
                // Disarm parallel ramping until something else confirms
                // the server is healthy again — the failed part is
                // already requeued by `handle_join_result_item` and the
                // main loop will retry, but the next attempt should
                // open only one connection at a time.
                self.ramp_armed
                    .store(false, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            }

            if pending.is_empty() || active.len() >= self.ctx.live.max_connections() {
                return Ok(());
            }

            let delay = sample_rampup_delay(self.rampup.delay_min, self.rampup.delay_max);
            if delay.is_zero() {
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = self.ctx.cancel.cancelled() => return Ok(()),
            }
        }
    }

    /// React to a runtime change in `ctx.live.max_connections()`. When the
    /// new cap is below the current `active.len()`, cancel the surplus
    /// in-flight tasks (chosen arbitrarily). Each cancelled task returns
    /// `PartEvent::NeedsReschedule`, which the existing handler requeues
    /// onto `pending`; they will resume later once capacity frees up. No
    /// progress is lost — partial bytes stay on disk and the controller is
    /// rebuilt from disk size on reschedule.
    fn apply_live_cap(&self, active: &mut HashMap<String, ActiveTask>) {
        let cap = self.ctx.live.max_connections();
        if cap == 0 || active.len() <= cap {
            return;
        }
        let surplus = active.len() - cap;
        let victims: Vec<String> = active.keys().take(surplus).cloned().collect();
        for ulid in victims {
            if let Some(task) = active.get(&ulid) {
                task.cancel.cancel();
            }
        }
    }

    async fn handle_join_result_item(
        &self,
        res: Result<Result<PartEvent, OdlError>, tokio::task::JoinError>,
        pending: &mut VecDeque<PartDetails>,
        active: &mut HashMap<String, ActiveTask>,
        last_failure: &mut Option<OdlError>,
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
                PartEvent::Failed {
                    ulid,
                    attempts,
                    cause,
                } => {
                    // A part whose retry wait was interrupted reports the same
                    // `Failed` as one that spent its budget: `wait_for_retry`
                    // returns false for both. Telling them apart matters — a
                    // paused job reported as Failed is one a caller may
                    // auto-restart, and the exit code flips 130 to 1.
                    if self.ctx.is_cancelled() {
                        return Err(OdlError::Cancelled);
                    }
                    // Kept so a download that ends with parts still queued can
                    // report the transfer failure that got it there, rather
                    // than a generic "some parts are unfinished".
                    *last_failure = Some(cause);
                    // Remove from active and attempt to reschedule this part
                    // later if there are other unfinished parts. If this was
                    // the last unfinished part, fail the overall download.
                    self.active_parts.lock().unwrap().remove(&ulid);
                    if let Some(task) = active.remove(&ulid) {
                        if pending.is_empty() && active.is_empty() {
                            // No other work to do — all parts have failed
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
                            self.ctx.live.shrink_by_one();
                        }
                    } else {
                        // If the task wasn't in `active`, still check whether
                        // everything else is done and fail if so.
                        if pending.is_empty() && active.is_empty() {
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
            Ok(Err(e)) => return Err(e),
            Err(join_err) => {
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
        // (i.e. `live.max_connections() - active.len()`)
        let spare_capacity = self.ctx.live.max_connections().saturating_sub(active.len());
        if !self.dynamic_split {
            return Ok(());
        }
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
        let task_cancel = CancellationToken::new();
        let task_cancel_for_task = task_cancel.clone();
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
                    task_cancel_for_task,
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
                cancel: task_cancel,
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
            .filter(|(_, task)| task.details.size != crate::download::Download::UNKNOWN_PART_SIZE)
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
        // Shared split geometry: cluster-aligned boundary at roughly the
        // midpoint of the remaining bytes. The bigger dynamic-split
        // threshold (3 MB) avoids splitting off a tail that wouldn't
        // outpace per-connection setup cost.
        let split =
            match Download::compute_split(0, current_limit, downloaded, MIN_DYNAMIC_SPLIT_SIZE) {
                Some(s) => s,
                None => return Ok(None),
            };
        candidate.controller.set_limit(split.new_left_size);
        // The left part just got smaller. Without this the previous total
        // stands until the next sampler tick, and if the part finishes or the
        // download pauses inside that window it never gets corrected.
        self.ctx.emit(ProgressEvent::PartProgress {
            ulid: candidate.ulid.clone(),
            downloaded: candidate.controller.downloaded().min(split.new_left_size),
            total: split.new_left_size,
        });

        let (new_part, encoded_metadata) = {
            let mut metadata = self.metadata.lock().await;
            let part_entry = metadata.parts.get_mut(&candidate.ulid).ok_or_else(|| {
                OdlError::MetadataError(MetadataError::Other {
                    message: format!("Part with ulid {} not found", candidate.ulid),
                })
            })?;
            let new_part_offset = part_entry.offset + split.new_left_size;
            part_entry.size = split.new_left_size;
            let new_ulid = Ulid::generate().to_string();
            let new_part = PartDetails {
                offset: new_part_offset,
                size: split.new_right_size,
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

        Ok(Some((new_part, split.new_left_size)))
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
    /// Per-task cancel; tripped when the live-cap is reduced and this
    /// task is selected as surplus. The task returns `NeedsReschedule`
    /// so the part's remaining bytes go back to `pending`.
    cancel: CancellationToken,
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
    NeedsReschedule {
        ulid: String,
    },
    /// Retries for this part are exhausted. `cause` is the failure that ended
    /// them, kept so the download can be reported as what actually went wrong
    /// rather than as a generic one.
    Failed {
        ulid: String,
        attempts: u32,
        cause: OdlError,
    },
}

struct BandwidthLimiter {
    rate: f64,
    state: std::sync::Mutex<LimiterState>,
    seq: AtomicU64,
}

struct LimiterState {
    available: f64,
    last_refill: Instant,
    queue: VecDeque<u64>,
}

/// Removes our sequence number from the FIFO queue if the acquire future
/// is dropped before consuming tokens (e.g. cancelled by `tokio::select!`).
/// Without this guard a cancelled acquire leaves a zombie seq at the head
/// of the queue, blocking every subsequent acquirer forever and stalling
/// throughput to zero.
struct QueueGuard<'a> {
    limiter: &'a BandwidthLimiter,
    seq: u64,
    consumed: bool,
}

impl Drop for QueueGuard<'_> {
    fn drop(&mut self) {
        if !self.consumed
            && let Ok(mut state) = self.limiter.state.lock()
        {
            state.queue.retain(|&s| s != self.seq);
        }
    }
}

impl BandwidthLimiter {
    fn new(bytes_per_second: u64) -> Self {
        let rate = bytes_per_second.max(1) as f64;
        Self {
            rate,
            state: std::sync::Mutex::new(LimiterState {
                available: rate,
                last_refill: Instant::now(),
                queue: VecDeque::new(),
            }),
            seq: AtomicU64::new(1),
        }
    }

    /// Acquire `amount` tokens, blocking via async sleeps until granted.
    /// Requests larger than the bucket capacity are split into rate-sized
    /// sub-acquires so an oversized chunk never deadlocks against the
    /// `available <= rate` cap.
    async fn acquire(&self, amount: u64) {
        let chunk_cap = self.rate as u64;
        let mut remaining = amount;
        while remaining > 0 {
            let take = remaining.min(chunk_cap);
            self.acquire_one(take).await;
            remaining -= take;
        }
    }

    async fn acquire_one(&self, amount: u64) {
        let amount_f = amount as f64;

        let my_seq = self.seq.fetch_add(1, Ordering::SeqCst);
        {
            let mut state = self.state.lock().expect("limiter mutex poisoned");
            state.queue.push_back(my_seq);
        }

        let mut guard = QueueGuard {
            limiter: self,
            seq: my_seq,
            consumed: false,
        };

        loop {
            let sleep_duration = {
                let mut state = self.state.lock().expect("limiter mutex poisoned");
                state.refill(self.rate);

                if let Some(&front) = state.queue.front()
                    && front == my_seq
                    && state.available >= amount_f
                {
                    state.available -= amount_f;
                    state.queue.pop_front();
                    guard.consumed = true;
                    return;
                }

                if state.available < amount_f {
                    let deficit = amount_f - state.available;
                    let wait_secs = deficit / self.rate;
                    match Duration::try_from_secs_f64(wait_secs) {
                        Ok(d) => Some(d.max(Duration::from_millis(1))),
                        Err(_) => Some(Duration::from_millis(1)),
                    }
                } else {
                    None
                }
            };

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
/// On Windows we use 1 MiB: each `WriteFile` re-enters the kernel and
/// passes through filter drivers (Defender, EDR, indexer), so a small
/// buffer at high throughput burns measurable CPU. 1 MiB cuts syscall
/// rate ~64× vs 16 KiB while keeping crash-loss bounded.
///
/// On Unix the page cache absorbs small writes cheaply (no per-write
/// filter pipeline), so we use 256 KiB (32× tokio's `BufWriter` default
/// of 8 KiB) — enough to coalesce most chunks while keeping crash-loss
/// tighter than the Windows value.
#[cfg(windows)]
const PART_WRITER_BUF_SIZE: usize = 1024 * 1024;
#[cfg(not(windows))]
const PART_WRITER_BUF_SIZE: usize = 256 * 1024;

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
        // Best-effort cleanup when the future is dropped without an explicit
        // `finish().await` (e.g. task cancelled mid-await on pause / cancel /
        // shutdown). Dropping `tx` closes the channel so the writer thread
        // drains pending chunks, flushes, and exits. The join handle is left
        // to complete in the background; any IO error it produces is
        // unobservable here, which is acceptable on the cancellation path.
        self.tx.take();
        self.handle.take();
    }
}

/// Announce a part's final byte count, then that it is finished.
///
/// [`ProgressEvent::PartFinished`] carries no size, so a consumer rendering
/// "downloaded / total" has only the sampler's last word to go on — which is
/// up to one 125 ms tick stale, and for a part that was already complete on
/// disk when the download resumed, never spoken at all. Such a part is
/// announced finished having reported zero bytes, so a UI shows it complete
/// and empty at the same time.
///
/// Stating the totals here makes "finished" and "full" agree at the source,
/// without changing the published shape of `PartFinished` — a field cannot be
/// added to it now that 2.0 is released.
fn emit_part_complete(ctx: &DownloadContext, ulid: &str, total: u64) {
    ctx.emit(ProgressEvent::PartProgress {
        ulid: ulid.to_owned(),
        downloaded: total,
        total,
    });
    ctx.emit(ProgressEvent::PartFinished {
        ulid: ulid.to_owned(),
    });
}

/// Whether a refusal is worth trying again.
///
/// The retry policy exists for transfers that fail *in transit*. A server that
/// answers a request correctly, with "no", is a different thing: no number of
/// attempts turns a 404 into a file, and spending the budget on one costs the
/// user seconds of backoff to reach a conclusion the first response already
/// gave. Worse, it ends with a retryable error class, telling whatever runs
/// odl to come back and do it again.
enum StatusVerdict {
    /// Settled. Fail the part now, with an error a caller will not retry.
    Terminal(OdlError),
    /// Might succeed later — the server is busy, throttling, or briefly broken.
    Transient(OdlError),
}

fn classify_part_status(status: StatusCode, url: &reqwest::Url) -> StatusVerdict {
    let as_network = || {
        OdlError::Network(crate::error::NetworkError::Status {
            status_code: status.as_u16(),
            reason: status.canonical_reason().map(str::to_owned),
            url: Some(url.to_string()),
        })
    };
    let conflict = |c: ServerConflict| OdlError::Conflict(ConflictError::Server { conflict: c });

    match status.as_u16() {
        // Credentials were accepted for the probe and are not accepted now,
        // or never were. Retrying sends the same ones again.
        401 | 403 | 407 => StatusVerdict::Terminal(conflict(ServerConflict::CredentialsInvalid)),
        // The resource is gone. `UrlBroken` says exactly that, and unlike a
        // network error it does not invite the caller to try again.
        404 | 410 => StatusVerdict::Terminal(conflict(ServerConflict::UrlBroken)),
        // Our range no longer fits the representation, which means the thing
        // on the server is not the thing we started downloading.
        416 => StatusVerdict::Terminal(conflict(ServerConflict::FileChanged)),
        // Explicitly "later": timeouts, early-data replay, and rate limits.
        408 | 425 | 429 => StatusVerdict::Transient(as_network()),
        // Any other client error is our request being wrong in a way that
        // repeating it will not fix.
        code if (400..500).contains(&code) => StatusVerdict::Terminal(OdlError::Other {
            message: format!("the server refused the request: {}", as_network()),
            origin: Box::new(std::io::Error::other("request refused")),
        }),
        // 5xx and anything unrecognised: assume the server can recover.
        _ => StatusVerdict::Transient(as_network()),
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
    task_cancel: CancellationToken,
) -> Result<PartEvent, OdlError> {
    if ctx.is_cancelled() {
        return Err(OdlError::Cancelled);
    }
    if task_cancel.is_cancelled() {
        return Ok(PartEvent::NeedsReschedule { ulid: part.ulid });
    }
    let PartDetails {
        offset, size, ulid, ..
    } = part;
    let part_path = instruction.part_path(&ulid);
    let url = instruction.url().clone();
    let mut current_size;
    let target_size = controller.limit();
    // Unknown total length: stream until the server closes the body.
    // We skip the Range header, never cap chunks, and treat EOF as a
    // successful completion (returning the actual downloaded byte count).
    let unknown_size = size == crate::download::Download::UNKNOWN_PART_SIZE;

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

        if !unknown_size && current_size >= target_size {
            // Nothing to transfer: the bytes were already on disk. Tell the
            // scheduler anyway. It waits for each part in a ramp batch to
            // report a first chunk before opening the next, and a part that
            // finishes without transferring never sends one — so the batch
            // wait drains the task set and returns, leaving the rest of the
            // queue unscheduled and the download reported as failed with
            // every byte of it present.
            if let Some(n) = probe_notify.as_ref() {
                n.notify_one();
            }
            file.finish().await?;
            emit_part_complete(&ctx, &ulid, target_size);
            return Ok(PartEvent::Completed(PartOutcome {
                ulid,
                final_size: target_size,
            }));
        }

        // build request — when total length is unknown we cannot construct
        // a meaningful Range, so issue a plain GET and stream until EOF.
        let mut req = client.get(url.clone());
        // Start of the window this attempt asks for; a resumed part picks up
        // after what is already on disk. Kept for validating the response.
        let part_window = offset + current_size;
        if !unknown_size {
            let range_header = format!("bytes={}-{}", part_window, offset + size - 1,);
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
            // Force identity transfer encoding on ranged requests. If the
            // server applies `Content-Encoding: gzip` to a ranged response,
            // RFC 9110 says the range is over the *compressed* bytes — but
            // reqwest's gzip/brotli decoders run per-response and cannot be
            // resumed across parts, so the assembled file would be a
            // corrupt mix of decoded fragments. Asking for identity avoids
            // the ambiguity entirely (servers that ignore us and gzip
            // anyway also tend to drop `Accept-Ranges`, which routes us
            // through the unknown-size single-stream path).
            req = req.header(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
        }
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
            Ok(Err(e)) => {
                file.finish().await?;
                let cause = OdlError::from(e);
                match retry_sleep_or_fail_part(
                    &policy,
                    attempts,
                    attempts + 1,
                    &ctx,
                    &ulid,
                    cause,
                    None,
                )
                .await
                {
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
                let cause = OdlError::Network(crate::error::NetworkError::Timeout);
                match retry_sleep_or_fail_part(
                    &policy,
                    attempts,
                    attempts + 1,
                    &ctx,
                    &ulid,
                    cause,
                    None,
                )
                .await
                {
                    Ok(()) => {
                        attempts = attempts.saturating_add(1);
                        continue;
                    }
                    Err(failed) => return Ok(failed),
                }
            }
        };

        // Nothing below this point looks at the response again, so it is
        // validated here, before a single byte of the body reaches the part
        // file. Without this an error page is written as if it were data and
        // a whole-file body is spliced in at a part's offset — both of which
        // produce a corrupt file that completes with a zero exit code.
        if !resp.status().is_success() {
            tracing::warn!(
                status = %resp.status(),
                ulid = %ulid,
                "part request answered with an error status"
            );
            file.finish().await?;
            // Carry the status through so the failure is reported as what it
            // was, and let it decide whether trying again is worth the user's
            // time at all.
            let cause = match classify_part_status(resp.status(), &url) {
                StatusVerdict::Terminal(cause) => {
                    return Ok(PartEvent::Failed {
                        ulid,
                        attempts: attempts.saturating_add(1),
                        cause,
                    });
                }
                StatusVerdict::Transient(cause) => cause,
            };
            // A server that says when to come back knows something odl's
            // backoff curve does not; racing it just earns another refusal.
            let retry_after = resp
                .headers()
                .get(RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(crate::retry_policies::parse_retry_after);
            match retry_sleep_or_fail_part(
                &policy,
                attempts,
                attempts + 1,
                &ctx,
                &ulid,
                cause,
                retry_after,
            )
            .await
            {
                Ok(()) => {
                    attempts = attempts.saturating_add(1);
                    continue;
                }
                Err(failed) => return Ok(failed),
            }
        }
        if !unknown_size && let Some(conflict) = range_mismatch(&resp, &instruction, part_window) {
            file.finish().await?;
            tracing::warn!(
                status = %resp.status(),
                ulid = %ulid,
                "server stopped honouring Range; refusing to write the response as part data"
            );
            return Err(OdlError::Conflict(ConflictError::Server { conflict }));
        }

        let mut started_notified = false;
        let mut saw_eof = false;
        loop {
            let allow_until = controller.limit();
            if !unknown_size && controller.downloaded() >= allow_until {
                break;
            }

            let chunk_result = tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => {
                    let _ = file.finish().await;
                    return Err(OdlError::Cancelled);
                }
                _ = task_cancel.cancelled() => {
                    let _ = file.finish().await;
                    return Ok(PartEvent::NeedsReschedule { ulid });
                }
                r = time::timeout(STALE_CONNECTION_TIMEOUT, resp.chunk()) => r,
            };
            let maybe_chunk = match chunk_result {
                Ok(chunk_res) => match chunk_res.map_err(OdlError::from) {
                    Ok(opt) => opt,
                    Err(e) => {
                        // network/body error -> consider retrying
                        file.finish().await?;
                        match retry_sleep_or_fail_part(
                            &policy,
                            attempts,
                            attempts + 1,
                            &ctx,
                            &ulid,
                            e,
                            None,
                        )
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
                    let cause = OdlError::Network(crate::error::NetworkError::Timeout);
                    match retry_sleep_or_fail_part(
                        &policy,
                        attempts,
                        attempts + 1,
                        &ctx,
                        &ulid,
                        cause,
                        None,
                    )
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

            if !unknown_size {
                let downloaded = controller.downloaded();
                let remaining = allow_until.saturating_sub(downloaded);
                if chunk.len() as u64 > remaining {
                    chunk = chunk.split_to(remaining as usize);
                }
            }

            let len = chunk.len() as u64;
            if let Some(limiter) = speed_limiter.as_ref() {
                tokio::select! {
                    _ = limiter.acquire(len) => {}
                    _ = ctx.cancel.cancelled() => {
                        let _ = file.finish().await;
                        return Err(OdlError::Cancelled);
                    }
                    _ = task_cancel.cancelled() => {
                        let _ = file.finish().await;
                        return Ok(PartEvent::NeedsReschedule { ulid });
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

        // Unknown-size stream: EOF is the only valid completion signal.
        // The recorded `final_size` becomes the byte count we actually
        // received, which `mark_part_finished` persists to PartDetails.
        if unknown_size && saw_eof {
            let final_size = controller.downloaded();
            emit_part_complete(&ctx, &ulid, final_size);
            return Ok(PartEvent::Completed(PartOutcome { ulid, final_size }));
        }

        if controller.downloaded() >= controller.limit() {
            emit_part_complete(&ctx, &ulid, controller.limit());
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
        let cause = OdlError::Network(crate::error::NetworkError::Other {
            message: "the transfer ended before the part was complete".to_owned(),
        });
        match retry_sleep_or_fail_part(&policy, attempts, attempts, &ctx, &ulid, cause, None).await
        {
            Ok(()) => continue,
            Err(failed) => return Ok(failed),
        }
    }
}

// Apply the retry policy: if it says `Retry` this sleeps until the retry
// time then returns `Ok(())`. If the policy says `DoNotRetry` it returns
// a `PartEvent::Failed` for the caller to surface/handle.
/// First byte covered by a `Content-Range: bytes START-END/TOTAL` header.
///
/// A malformed or absent header yields `None`, which the caller treats as
/// "unverifiable" rather than "wrong": the status code already established
/// that this is a range response, and some servers omit the header.
fn content_range_start(resp: &reqwest::Response) -> Option<u64> {
    let value = resp.headers().get(CONTENT_RANGE)?.to_str().ok()?;
    let (unit, spec) = value.split_once(' ')?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }
    spec.split_once('-')?.0.trim().parse().ok()
}

/// Why a successful response to a ranged request cannot be used as part data.
///
/// `200` means the server ignored `Range` and is sending the file from byte
/// zero. That body is usable only when the part *is* the whole file and
/// nothing has been written yet — the single-connection first attempt, where
/// what arrives is exactly what was asked for. In every other case the bytes
/// belong at offset zero and would be written somewhere else.
///
/// A `206` whose window starts anywhere but where we asked is the same
/// problem wearing a correct status code.
fn range_mismatch(
    resp: &reqwest::Response,
    instruction: &Download,
    want_start: u64,
) -> Option<ServerConflict> {
    match resp.status() {
        StatusCode::PARTIAL_CONTENT => match content_range_start(resp) {
            Some(start) if start != want_start => Some(ServerConflict::FileChanged),
            _ => None,
        },
        StatusCode::OK => {
            // RFC 9110 14.4: `Content-Range` has no defined meaning on a 200,
            // and 14.2 lets any server ignore `Range` and answer one. So it is
            // never used to *place* the body — only, when it is present and
            // points somewhere other than the start, as a reason to distrust
            // a response that claims to begin at byte zero.
            let body_starts_at_zero = content_range_start(resp).is_none_or(|start| start == 0);
            // An absent length is ordinary: a chunked 200 carries none, and
            // reading that as disagreement would refuse a perfectly good
            // download. Only a length that is present *and* disagrees with
            // what the probe reported means this is not the whole file.
            let length_agrees = resp
                .content_length()
                .is_none_or(|len| Some(len) == instruction.size());
            if want_start == 0 && body_starts_at_zero && length_agrees {
                None
            } else {
                Some(ServerConflict::NotResumable)
            }
        }
        // Any other 2xx answers a question we did not ask.
        _ => Some(ServerConflict::NotResumable),
    }
}

async fn retry_sleep_or_fail_part(
    policy: &FixedThenExponentialRetry,
    _attempts_for_policy: u32,
    attempts_display: u32,
    ctx: &DownloadContext,
    ulid: &str,
    cause: OdlError,
    retry_after: Option<Duration>,
) -> Result<(), PartEvent> {
    ctx.emit(ProgressEvent::PartRetrying {
        ulid: ulid.to_string(),
        attempt: attempts_display,
    });
    if wait_for_retry(policy, attempts_display, ctx, Some(ulid), retry_after).await {
        Ok(())
    } else {
        Err(PartEvent::Failed {
            ulid: ulid.to_string(),
            attempts: attempts_display,
            cause,
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
            true,
            RampupConfig::disabled(),
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
    async fn test_downloader_streams_unknown_size_until_eof()
    -> Result<(), Box<dyn std::error::Error>> {
        // Regression: when the server reports no total length (typical
        // chunked/gzipped HTML), `determine_parts` emits a single part
        // tagged with UNKNOWN_PART_SIZE. The downloader must skip the
        // Range header and drain the body until EOF, then record the
        // actual byte count on the part — not produce an empty file.
        let file_content = b"<html><body>hello world</body></html>";
        let mut server = Server::new_async().await;
        let base = server.url();
        let get_mock = server
            .mock("GET", "/page")
            .match_header("range", Matcher::Missing)
            .with_status(200)
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
            make_part("part1", 0, crate::download::Download::UNKNOWN_PART_SIZE),
        );

        // Build an instruction with size=None to mirror the real
        // unknown-length path; `create_instruction` always sets a Some
        // size, so construct directly here.
        let download = DownloadBuilder::default()
            .download_dir(download_dir.clone())
            .save_dir(save_dir.clone())
            .filename(TEST_FILENAME.to_string())
            .url(Url::parse(&format!("{}/page", base))?)
            .size(None)
            .parts(parts)
            .max_connections(1)
            .is_resumable(false)
            .build()?;
        let instruction = Arc::new(download);

        let metadata = instruction.as_metadata();
        let downloader = Downloader::new(
            Arc::clone(&instruction),
            metadata,
            reqwest::Client::builder().build()?,
            false,
            None,
            true,
            RampupConfig::disabled(),
            FixedThenExponentialRetry::default(),
            DownloadContext::new(),
        );
        let updated_metadata = downloader.run().await?;

        let part_bytes = fs::read(instruction.part_path("part1")).await?;
        assert_eq!(part_bytes, file_content);
        let part = updated_metadata
            .parts
            .get("part1")
            .expect("part1 present after run");
        assert!(part.finished);
        assert_eq!(part.size, file_content.len() as u64);
        get_mock.assert_async().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_live_cap_cancels_surplus() -> Result<(), Box<dyn std::error::Error>> {
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
            3,
        )
        .await;
        let metadata = instruction.as_metadata();
        let downloader = Downloader::new(
            Arc::clone(&instruction),
            metadata,
            reqwest::Client::builder().build()?,
            false,
            None,
            true,
            RampupConfig::disabled(),
            FixedThenExponentialRetry::default(),
            DownloadContext::new(),
        );

        let make_task = |size: u64| ActiveTask {
            details: make_part("x", 0, size),
            controller: Arc::new(PartController::new(size, 0)),
            cancel: CancellationToken::new(),
        };
        let mut active: HashMap<String, ActiveTask> = HashMap::new();
        active.insert("a".to_string(), make_task(1024));
        active.insert("b".to_string(), make_task(1024));
        active.insert("c".to_string(), make_task(1024));

        // No-op when cap >= active.
        downloader.ctx.live.set_max_connections(3);
        downloader.apply_live_cap(&mut active);
        assert_eq!(
            active.values().filter(|t| t.cancel.is_cancelled()).count(),
            0
        );

        // Shrink to 1 — two should be cancelled.
        downloader.ctx.live.set_max_connections(1);
        downloader.apply_live_cap(&mut active);
        assert_eq!(
            active.values().filter(|t| t.cancel.is_cancelled()).count(),
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_live_controls_seed_and_set() {
        let ctx = DownloadContext::new();
        assert_eq!(ctx.live.max_connections(), 0);
        ctx.live.seed_if_unset(4);
        assert_eq!(ctx.live.max_connections(), 4);
        // seed is a no-op when already set
        ctx.live.seed_if_unset(8);
        assert_eq!(ctx.live.max_connections(), 4);
        // explicit set wins, clamped to >=1
        ctx.live.set_max_connections(0);
        assert_eq!(ctx.live.max_connections(), 1);
        ctx.live.set_max_connections(6);
        assert_eq!(ctx.live.max_connections(), 6);
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
            true,
            RampupConfig::disabled(),
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
            true,
            RampupConfig::disabled(),
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
            CancellationToken::new(),
        )
        .await?;

        match event {
            PartEvent::NeedsReschedule { ulid } => assert_eq!(ulid, "part"),
            PartEvent::Completed(_) => panic!("expected reschedule"),
            PartEvent::Failed {
                ulid,
                attempts,
                cause,
            } => panic!("unexpected failed part {ulid} after {attempts} attempts: {cause}"),
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

    #[tokio::test(start_paused = true)]
    async fn test_bandwidth_limiter_dropped_acquire_does_not_block_queue() {
        // Drain the initial bucket so the next acquire must wait.
        let limiter = BandwidthLimiter::new(1024);
        limiter.acquire(1024).await;

        // Start an acquire, poll once to enqueue the seq, then drop it
        // (simulating tokio::select! cancellation). Without the QueueGuard
        // this leaves a zombie at the head of the queue.
        {
            let pending = limiter.acquire(1024);
            tokio::pin!(pending);
            assert!(pending.as_mut().now_or_never().is_none());
        }

        // After enough refill, a fresh acquire must complete; if the
        // dropped seq were still in the queue, this would hang.
        time::advance(Duration::from_millis(1100)).await;
        let third = limiter.acquire(1024);
        tokio::pin!(third);
        assert!(third.as_mut().now_or_never().is_some());
    }

    #[tokio::test]
    async fn test_bandwidth_limiter_handles_amount_larger_than_rate() {
        // Chunk larger than the per-second rate must not deadlock against
        // the bucket capacity cap; acquire splits it into sub-acquires.
        let limiter = Arc::new(BandwidthLimiter::new(8192));
        // 32 KiB at 8 KiB/s → ~3s. Bound test under a generous timeout to
        // catch deadlocks without flaking on slow CI.
        tokio::time::timeout(Duration::from_secs(10), limiter.acquire(32 * 1024))
            .await
            .expect("acquire must not deadlock for amount > rate");
    }

    #[test]
    fn sample_rampup_delay_clamps_when_max_le_min() {
        let min = Duration::from_millis(500);
        let max = Duration::from_millis(200);
        assert_eq!(sample_rampup_delay(min, max), min);
        // Equal bounds: deterministic.
        assert_eq!(sample_rampup_delay(min, min), min);
    }

    #[test]
    fn sample_rampup_delay_stays_within_bounds() {
        let min = Duration::from_millis(500);
        let max = Duration::from_millis(1000);
        for _ in 0..2000 {
            let d = sample_rampup_delay(min, max);
            assert!(d >= min && d <= max, "delay {:?} out of bounds", d);
        }
    }

    /// Hand-rolled HTTP server that responds to one range GET with
    /// headers + a single body byte, then holds the connection open
    /// forever. Lets fill_capacity see an "active" download without the
    /// task ever completing, so we can count how many connections were
    /// opened over time. Returns (address, counter, listener-task
    /// abort-handle).
    async fn spawn_hanging_http_server() -> (
        std::net::SocketAddr,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                counter_clone.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    // Read the request headers (until \r\n\r\n).
                    let mut buf = [0u8; 4096];
                    let mut acc = Vec::new();
                    loop {
                        let n = match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        acc.extend_from_slice(&buf[..n]);
                        if acc.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    // Parse Range to know the part length so we can
                    // advertise a matching Content-Length / Content-Range.
                    let req = String::from_utf8_lossy(&acc);
                    let (start, end) = req
                        .lines()
                        .find_map(|l| {
                            let l = l.trim();
                            let rest = l.strip_prefix("Range:")?.trim();
                            let rest = rest.strip_prefix("bytes=")?;
                            let mut it = rest.split('-');
                            let s: u64 = it.next()?.trim().parse().ok()?;
                            let e: u64 = it.next()?.trim().parse().ok()?;
                            Some((s, e))
                        })
                        .unwrap_or((0, 0));
                    let _ = (start, end);
                    // Use Transfer-Encoding: chunked. Send a single
                    // 1-byte chunk then hold the connection open
                    // without writing the terminating zero-chunk —
                    // hyper will treat the body as "more bytes coming"
                    // so it blocks on the next `chunk()` instead of
                    // returning EOF, while still surfacing the first
                    // byte to the probe.
                    let header = "HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\nAccept-Ranges: bytes\r\nConnection: keep-alive\r\n\r\n";
                    let mut out = header.as_bytes().to_vec();
                    out.extend_from_slice(b"1\r\n\x00\r\n");
                    if sock.write_all(&out).await.is_err() {
                        return;
                    }
                    let _ = sock.flush().await;
                    // Hold the connection open until the test tears
                    // down — reqwest shuts its write half after sending
                    // the GET, so reading would observe EOF immediately.
                    std::future::pending::<()>().await;
                    drop(sock);
                });
            }
        });

        (addr, counter, handle)
    }

    /// Poll the counter (real time) up to `timeout` until `pred()` holds.
    async fn wait_for<F>(label: &str, timeout: Duration, mut pred: F)
    where
        F: FnMut() -> bool,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if pred() {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out after {:?}: {}", timeout, label);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn build_rampup_test_downloader(
        addr: std::net::SocketAddr,
        n_parts: u64,
        rampup: RampupConfig,
    ) -> (Arc<Download>, Downloader, tempfile::TempDir) {
        let tmp = tempdir().expect("tmp");
        let download_dir = tmp.path().join("download");
        let save_dir = tmp.path().join("save");
        fs::create_dir_all(&download_dir).await.expect("mkdir dl");
        fs::create_dir_all(&save_dir).await.expect("mkdir save");

        // Each part advertises 1 MiB so they stay nominally "in flight"
        // after the server's single body byte; sizes are arbitrary, the
        // test cares about open-count, not bytes transferred.
        let part_size: u64 = 1024 * 1024;
        let total = part_size * n_parts;
        let mut parts = HashMap::new();
        for i in 0..n_parts {
            let ulid = format!("p{i}");
            parts.insert(ulid.clone(), make_part(&ulid, i * part_size, part_size));
        }

        let url = format!("http://{}/file", addr);
        let instruction =
            create_instruction(&download_dir, &save_dir, &url, total, parts, n_parts).await;
        let metadata = instruction.as_metadata();

        let downloader = Downloader::new(
            Arc::clone(&instruction),
            metadata,
            reqwest::Client::builder().build().expect("client"),
            false,
            None,
            false, // dynamic_split off — keep part count stable for counting
            rampup,
            FixedThenExponentialRetry::default(),
            DownloadContext::new(),
        );
        (instruction, downloader, tmp)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fill_capacity_ramps_connections_in_batches() {
        use std::sync::atomic::Ordering;

        const DELAY: Duration = Duration::from_millis(300);

        let (addr, counter, server_task) = spawn_hanging_http_server().await;
        let (_instruction, downloader, _tmp) = build_rampup_test_downloader(
            addr,
            7,
            RampupConfig {
                enabled: true,
                batch_size: 2,
                delay_min: DELAY,
                delay_max: DELAY,
            },
        )
        .await;

        let cancel = downloader.ctx.cancel.clone();
        let dl_task = tokio::spawn(async move {
            let _ = downloader.run().await;
        });

        // Probe + first batch (2) land back-to-back, well within one
        // delay window.
        wait_for(
            "counter >= 3 (probe + first batch)",
            Duration::from_secs(5),
            || counter.load(Ordering::SeqCst) >= 3,
        )
        .await;
        // Before the inter-batch delay elapses, no more connections
        // should be opened.
        tokio::time::sleep(DELAY / 3).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            3,
            "rampup must wait for inter-batch delay before opening more"
        );

        // Second batch fires after the first delay.
        wait_for("counter >= 5 (second batch)", DELAY * 3, || {
            counter.load(Ordering::SeqCst) >= 5
        })
        .await;
        tokio::time::sleep(DELAY / 3).await;
        assert_eq!(counter.load(Ordering::SeqCst), 5);

        // Third batch fires after the second delay; cap (7) reached.
        wait_for("counter == 7 (cap reached)", DELAY * 3, || {
            counter.load(Ordering::SeqCst) >= 7
        })
        .await;
        assert_eq!(counter.load(Ordering::SeqCst), 7);

        // No further connections should be opened once cap is reached.
        tokio::time::sleep(DELAY * 2).await;
        assert_eq!(counter.load(Ordering::SeqCst), 7);

        cancel.cancel();
        let _ = dl_task.await;
        server_task.abort();
    }

    /// TCP listener that accepts connections, increments a counter,
    /// then immediately drops the socket (server-side RST/FIN). Used
    /// to simulate a server that drops every batch connection so we
    /// can assert rampup stops opening more after a failure.
    async fn spawn_drop_server() -> (
        std::net::SocketAddr,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let handle = tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    return;
                };
                counter_clone.fetch_add(1, Ordering::SeqCst);
                drop(sock);
            }
        });
        (addr, counter, handle)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fill_capacity_aborts_ramp_when_batch_part_fails() {
        // With a drop-every-connection server, the probe + the first
        // rampup batch will all fail. After retries are exhausted the
        // failed parts surface a `PartEvent::Failed` from the join
        // set — at which point fill_capacity must stop ramping for
        // this round, leaving any remaining pending parts unscheduled.
        // We assert that not all 10 parts get opened in a single tight
        // burst: the failure-abort path keeps the connection count
        // bounded by the first batch (plus the probe).
        use std::sync::atomic::Ordering;

        let (addr, counter, server_task) = spawn_drop_server().await;

        // Short retry policy so failures surface fast.
        let tmp = tempdir().expect("tmp");
        let download_dir = tmp.path().join("download");
        let save_dir = tmp.path().join("save");
        fs::create_dir_all(&download_dir).await.expect("mkdir dl");
        fs::create_dir_all(&save_dir).await.expect("mkdir save");
        let n_parts: u64 = 10;
        let part_size: u64 = 1024 * 1024;
        let total = part_size * n_parts;
        let mut parts = HashMap::new();
        for i in 0..n_parts {
            let ulid = format!("p{i}");
            parts.insert(ulid.clone(), make_part(&ulid, i * part_size, part_size));
        }
        let url = format!("http://{}/file", addr);
        let instruction =
            create_instruction(&download_dir, &save_dir, &url, total, parts, n_parts).await;
        let metadata = instruction.as_metadata();
        let downloader = Downloader::new(
            Arc::clone(&instruction),
            metadata,
            reqwest::Client::builder().build().expect("client"),
            false,
            None,
            false,
            RampupConfig {
                enabled: true,
                batch_size: 2,
                delay_min: Duration::from_millis(50),
                delay_max: Duration::from_millis(50),
            },
            FixedThenExponentialRetry {
                max_n_retries: 1,
                wait_time: Duration::from_millis(20),
                n_fixed_retries: 1,
            },
            DownloadContext::new(),
        );

        let cancel = downloader.ctx.cancel.clone();
        let dl_task = tokio::spawn(async move {
            let _ = downloader.run().await;
        });

        // Let things run long enough that, without the failure-abort,
        // every one of the 10 parts would have been opened multiple
        // times (retries + further batches). Each retry opens a fresh
        // TCP connection, so the counter is monotonic but the test
        // tolerates retries within a bounded window.
        tokio::time::sleep(Duration::from_millis(800)).await;
        cancel.cancel();
        let _ = dl_task.await;

        let opened = counter.load(Ordering::SeqCst);
        // With batch_size=2, immediate failures, and ramp abort on
        // failure: the run should not have managed to open all 10
        // parts in this window. Even allowing retries on the probe
        // and one batch, we expect comfortably fewer than 10 unique
        // parts' worth (ignoring retries). Bound generously to keep
        // the test stable on slow CI while still catching the bug
        // where ramp keeps marching past failures.
        assert!(
            opened < 30,
            "rampup did not throttle on failures: {} connections opened",
            opened
        );
        server_task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fill_capacity_no_rampup_opens_all_at_once() {
        use std::sync::atomic::Ordering;

        let (addr, counter, server_task) = spawn_hanging_http_server().await;
        let (_instruction, downloader, _tmp) =
            build_rampup_test_downloader(addr, 6, RampupConfig::disabled()).await;

        let cancel = downloader.ctx.cancel.clone();
        let dl_task = tokio::spawn(async move {
            let _ = downloader.run().await;
        });

        wait_for(
            "counter == 6 (all open at once)",
            Duration::from_secs(5),
            || counter.load(Ordering::SeqCst) >= 6,
        )
        .await;
        assert_eq!(counter.load(Ordering::SeqCst), 6);

        cancel.cancel();
        let _ = dl_task.await;
        server_task.abort();
    }
}
