//! Progress reporting and cancellation primitives.
//!
//! GUI / library consumers that want live progress and the ability to
//! cancel a running download integrate through this module. The download
//! manager is fully decoupled from any specific progress UI: it emits
//! [`ProgressEvent`]s to a user-supplied [`ProgressReporter`] and observes
//! a `tokio_util::sync::CancellationToken` for stop requests.
//!
//! Built-ins:
//! - [`NoopReporter`] — discards all events. Default when no reporter is
//!   supplied.
//! - [`ChannelReporter`] — forwards events through a `tokio::sync::mpsc`
//!   channel; pair with [`channel_reporter`] which returns the receiver.
//!
//! GUIs typically construct one [`ChannelReporter`] per download (or one
//! shared reporter that disambiguates by URL/handle) and drive their UI
//! from the receiver in a long-running task.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

/// Ulid carried by the part events that report final-file assembly.
///
/// Assembly is not a real part, but it is reported through the same
/// `Part*` events so a consumer can render it with the machinery it
/// already has. Match on this to tell the two apart — for instance to
/// label the bar differently, or to keep assembly out of a per-connection
/// count.
pub const ASSEMBLY_ULID: &str = "_assemble";

/// Ulid carried by the part events that report checksum verification.
///
/// The same idea as [`ASSEMBLY_ULID`]: verification is not a part, but
/// hashing a large file takes long enough to need a bar of its own, so it is
/// reported through the machinery a consumer already has.
pub const VERIFY_ULID: &str = "_verify";

/// Sampling cadence for speed / progress events emitted by the lib.
///
/// 8 Hz (~125 ms): high enough that bars animate smoothly, low enough
/// that the per-window byte delta still reflects current network speed
/// reactively (no EWMA needed).
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(125);

use reqwest::Url;
use tokio::sync::mpsc;

pub use tokio_util::sync::CancellationToken;

/// High-level lifecycle phase a download is currently in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// Engines bring phases the built-in downloader has no equivalent for, so this
// stays open rather than breaking every consumer each time one is added.
#[non_exhaustive]
pub enum Phase {
    /// Probing the server (HEAD request, redirect resolution, etc.).
    Evaluating,
    /// Resolving save / server conflicts before download begins.
    ResolvingConflicts,
    /// Actively downloading file parts.
    Downloading,
    /// Work an external tool performs on the downloaded data before it is
    /// usable — muxing separate video and audio streams, for instance.
    PostProcessing,
    /// Concatenating / reflinking parts into the final file.
    Assembling,
    /// `fsync`ing the final file to durable storage.
    Flushing,
    /// Verifying checksum of the assembled file (when known).
    Verifying,
}

/// Events emitted by the download pipeline.
///
/// The `total` field on [`ProgressEvent::Progress`] is `None` when the server
/// did not advertise content length.
#[derive(Debug, Clone)]
// Engines report things their predecessors had no notion of — a torrent has
// peers and pieces where an HTTP download has neither. Leaving this open means
// adding one does not break every consumer's `match`.
#[non_exhaustive]
pub enum ProgressEvent {
    /// Lifecycle phase changed.
    PhaseChanged(Phase),
    /// Filename for the download was determined (after `evaluate`).
    FilenameResolved(String),
    /// Aggregate byte-count progress for the whole download.
    Progress { downloaded: u64, total: Option<u64> },
    /// Speed sample in bytes/second over the last sampler window. Raw
    /// window rate (`delta_bytes / delta_time`), no smoothing. Emitted
    /// at [`SAMPLE_INTERVAL`] cadence whenever a download or assembly
    /// is in progress.
    Speed { bytes_per_second: f64 },
    /// A new part was added (initial split or dynamic split).
    PartAdded {
        ulid: String,
        offset: u64,
        size: u64,
    },
    /// A part advanced.
    PartProgress {
        ulid: String,
        downloaded: u64,
        total: u64,
    },
    /// A part finished successfully.
    PartFinished { ulid: String },
    /// Every part announced so far is gone: the download was restarted and
    /// re-split, so the ulids already reported name nothing. Consumers that
    /// keep per-part state (a row, a bar) must drop all of it — no
    /// [`ProgressEvent::PartFinished`] is coming for those parts, because
    /// they did not finish. New [`ProgressEvent::PartAdded`] events follow.
    PartsCleared,
    /// Latest sampled bytes-per-second for a single part. Emitted on the
    /// same cadence as aggregate [`ProgressEvent::Speed`]. Raw window
    /// rate, no smoothing.
    PartSpeed { ulid: String, bytes_per_second: f64 },
    /// A part is being retried.
    PartRetrying { ulid: String, attempt: u32 },
    /// A retry is scheduled: the next attempt begins after `delay`.
    ///
    /// Emitted once, when the wait starts, so a UI can show *when* the
    /// download resumes rather than only that it is waiting. The wait is
    /// interruptible, so treat this as the current plan rather than a promise.
    RetryScheduled {
        /// The part this retry belongs to, when it belongs to one. `None` for
        /// retries of a whole-download step such as the initial probe.
        ulid: Option<String>,
        attempt: u32,
        max_attempts: u32,
        delay: Duration,
        /// The delay is the server's own `Retry-After`, not odl's backoff.
        /// Worth distinguishing: a UI can say the server asked for the wait,
        /// and a caller knows shortening it will not help.
        server_requested: bool,
    },
    /// Free-form status message (e.g. "Warming up", "Waiting for retry…").
    Message(String),
    /// Download finished successfully and final file is at `path`.
    /// `already_complete` is `true` when the download was a no-op because
    /// the assembled final file was already on disk from a prior run.
    Completed {
        path: std::path::PathBuf,
        already_complete: bool,
    },
    /// Download was cancelled via the cancellation token.
    Cancelled,
    /// Download failed; `message` is human-readable.
    Failed { message: String },
}

/// Sink for [`ProgressEvent`]s.
///
/// Rate depends on the engine, but neither one calls per chunk:
///
/// - The built-in multipart downloader samples on a timer, emitting
///   [`ProgressEvent::Progress`], [`ProgressEvent::Speed`] and the per-part
///   equivalents every [`SAMPLE_INTERVAL`] (8 Hz) regardless of how chunks
///   arrive. A stalled transfer keeps ticking with an unchanged byte count.
/// - An engine that delegates to an external downloader forwards whatever
///   that tool reports, capped at a comparable rate. Those events are
///   data-driven rather than clock-driven, so a stalled transfer goes quiet
///   instead of reporting zero — detecting a stall needs the consumer's own
///   wall clock.
///
/// Lifecycle events ([`ProgressEvent::PhaseChanged`],
/// [`ProgressEvent::PartFinished`], [`ProgressEvent::Completed`], …) are
/// emitted when they occur, on top of the sampled ones.
///
/// Implementations should still return promptly: `on_event` runs on the task
/// driving the download, so blocking here — a mutex, a redraw, a disk write —
/// back-pressures the transfer. Wrap anything non-trivial in
/// [`AsyncReporter`], which hands events to a worker task and returns
/// immediately.
pub trait ProgressReporter: Send + Sync + 'static {
    /// Receive an event.
    fn on_event(&self, event: ProgressEvent);
}

/// Reporter that discards every event. Used when the caller does not care
/// about progress.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopReporter;

impl ProgressReporter for NoopReporter {
    fn on_event(&self, _event: ProgressEvent) {}
}

/// `mpsc`-backed reporter. Build with [`channel_reporter`].
pub struct ChannelReporter {
    tx: mpsc::UnboundedSender<ProgressEvent>,
}

impl ProgressReporter for ChannelReporter {
    fn on_event(&self, event: ProgressEvent) {
        // Best-effort: if the receiver has been dropped, silently discard.
        let _ = self.tx.send(event);
    }
}

/// Returns a paired (reporter, receiver). The reporter can be cloned via
/// `Arc::clone` and shared; events arrive on the receiver in send order.
pub fn channel_reporter() -> (Arc<ChannelReporter>, mpsc::UnboundedReceiver<ProgressEvent>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Arc::new(ChannelReporter { tx }), rx)
}

/// Wraps any [`ProgressReporter`] so that every `on_event` call hands the
/// event off through a lock-free `tokio::sync::mpsc` and returns
/// immediately. The wrapped reporter is driven on a dedicated worker task,
/// so slow / locking work in the consumer (Mutex hops, redraws, GUI state
/// stores) cannot back-pressure the download machinery.
///
/// Use this whenever the downstream reporter does any non-trivial work.
/// `NoopReporter` and the raw `ChannelReporter` are already
/// non-blocking — wrapping them adds no value.
///
/// Events are queued unbounded. If the consumer is permanently slower
/// than the producer, memory grows. The lib's emission rate is bounded
/// (sampler at 8 Hz + a handful of lifecycle events), so this is a
/// non-issue in practice.
///
/// On drop the channel sender closes; the worker drains the remaining
/// queued events and exits naturally, so terminal events emitted just
/// before drop (e.g. `Completed` / `Failed` / `Cancelled`) are not lost.
pub struct AsyncReporter {
    tx: mpsc::UnboundedSender<ProgressEvent>,
    /// Events sent but not yet handed to the wrapped reporter.
    queued: Arc<AtomicUsize>,
    /// Signalled whenever `queued` reaches zero, so [`Self::drained`] can
    /// wait rather than poll.
    idle: Arc<Notify>,
    /// Worker handle is kept so the task is owned by this struct.
    /// Dropping the JoinHandle detaches (does not abort) the task —
    /// after `tx` is dropped the channel closes and the worker exits
    /// after draining.
    _worker: tokio::task::JoinHandle<()>,
}

impl AsyncReporter {
    /// Spawn a worker task that forwards events to `inner`. Returns an
    /// `Arc<AsyncReporter>` ready to plug into a [`DownloadContext`].
    pub fn spawn<R: ProgressReporter>(inner: R) -> Arc<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel::<ProgressEvent>();
        let queued = Arc::new(AtomicUsize::new(0));
        let idle = Arc::new(Notify::new());
        let queued_for_worker = Arc::clone(&queued);
        let idle_for_worker = Arc::clone(&idle);
        let worker = tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                inner.on_event(ev);
                if queued_for_worker.fetch_sub(1, Ordering::AcqRel) == 1 {
                    idle_for_worker.notify_waiters();
                }
            }
        });
        Arc::new(Self {
            tx,
            queued,
            idle,
            _worker: worker,
        })
    }

    /// Wait until every event sent so far has reached the wrapped reporter.
    ///
    /// The hand-off is what makes emitting cheap for the download tasks, but
    /// it also means an event can still be in flight when the work that
    /// produced it has returned. A caller that is about to say something else
    /// about the same download — or to exit — waits here first, so the two do
    /// not arrive out of order or, at exit, not at all.
    pub async fn drained(&self) {
        loop {
            // Registered before the check: an event completing in between
            // would otherwise notify nobody and leave this waiting forever.
            let idle = self.idle.notified();
            if self.queued.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }
}

impl ProgressReporter for AsyncReporter {
    fn on_event(&self, event: ProgressEvent) {
        // Lock-free: tokio's UnboundedSender uses an atomic intrusive
        // queue, no Mutex on the producer path.
        self.queued.fetch_add(1, Ordering::AcqRel);
        if self.tx.send(event).is_err() {
            // Nobody will ever take it off the queue; not counting it keeps
            // `drained` from waiting for a worker that is gone.
            if self.queued.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.idle.notify_waiters();
            }
        }
    }
}

/// Runtime knob to change the number of live connections of a running
/// download. Cheap to clone (single `Arc` inside). Increases let the
/// downloader split unfinished parts to fill the new capacity (subject to
/// `dynamic_split`). Decreases cancel surplus in-flight parts; their
/// remaining bytes go back to the pending queue and resume later as
/// capacity frees up.
///
/// A fresh instance reports `max_connections() == 0` (unset); the
/// downloader seeds it from `metadata.max_connections` on first run.
#[derive(Clone, Default)]
pub struct LiveControls {
    inner: Arc<LiveControlsInner>,
}

#[derive(Default)]
struct LiveControlsInner {
    max_connections: AtomicUsize,
    notify: Notify,
}

impl LiveControls {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the desired number of live connections. `0` is clamped to `1`.
    /// Effective on a running download as soon as the run loop observes
    /// the notification (next iteration).
    pub fn set_max_connections(&self, n: usize) {
        self.inner.max_connections.store(n.max(1), Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    /// Current live-connection target. `0` means unset (downloader will
    /// seed from `metadata.max_connections` on first run).
    pub fn max_connections(&self) -> usize {
        self.inner.max_connections.load(Ordering::SeqCst)
    }

    /// Atomically initialize the cap if still unset; returns the post-call
    /// value. Used by the downloader on startup.
    pub(crate) fn seed_if_unset(&self, n: usize) -> usize {
        let _ = self.inner.max_connections.compare_exchange(
            0,
            n.max(1),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        self.inner.max_connections.load(Ordering::SeqCst)
    }

    /// Bound the current cap to at most `n` (used by the downloader's
    /// failure-driven shrink). Never raises.
    pub(crate) fn shrink_by_one(&self) {
        let _ =
            self.inner
                .max_connections
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |cur| {
                    if cur > 1 { Some(cur - 1) } else { Some(1) }
                });
    }

    pub(crate) fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.inner.notify.notified()
    }
}

impl std::fmt::Debug for LiveControls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveControls")
            .field("max_connections", &self.max_connections())
            .finish()
    }
}

/// Per-download context: reporter + cancellation token + live controls.
///
/// Cheap to clone (`Arc` and a `CancellationToken` clone). One context per
/// `DownloadManager::download` call (attach via `DownloadRequest::ctx`).
#[derive(Clone)]
pub struct DownloadContext {
    pub reporter: Arc<dyn ProgressReporter>,
    pub cancel: CancellationToken,
    /// Optional URL the GUI knows this context by. Reporters that
    /// multiplex many downloads onto one channel use this to disambiguate.
    pub url: Option<Url>,
    /// Live knobs (currently: connection count). Clone and call
    /// `set_max_connections` on it mid-download to grow or shrink.
    pub live: LiveControls,
}

impl DownloadContext {
    pub fn new() -> Self {
        Self {
            reporter: Arc::new(NoopReporter),
            cancel: CancellationToken::new(),
            url: None,
            live: LiveControls::new(),
        }
    }

    pub fn with_live(mut self, live: LiveControls) -> Self {
        self.live = live;
        self
    }

    pub fn with_reporter(mut self, reporter: Arc<dyn ProgressReporter>) -> Self {
        self.reporter = reporter;
        self
    }

    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = cancel;
        self
    }

    pub fn with_url(mut self, url: Url) -> Self {
        self.url = Some(url);
        self
    }

    pub fn emit(&self, event: ProgressEvent) {
        self.reporter.on_event(event);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

impl Default for DownloadContext {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for DownloadContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadContext")
            .field("cancel", &self.cancel)
            .field("url", &self.url)
            .field("live", &self.live)
            .finish_non_exhaustive()
    }
}

/// Drop entries from the front of `window` whose timestamps are older
/// than `SPEED_WINDOW_LEN` relative to `now`. Always retains at least
/// the most recent entry so a rate can still be derived once a second
/// sample arrives.
pub(crate) fn trim_speed_window(
    window: &mut std::collections::VecDeque<(Instant, u64)>,
    now: Instant,
    window_len: Duration,
) {
    while window.len() > 1 {
        let Some(&(t, _)) = window.front() else {
            break;
        };
        if now.saturating_duration_since(t) > window_len {
            window.pop_front();
        } else {
            break;
        }
    }
}

/// Average rate (bytes/sec) across the samples currently in `window`.
/// Returns `None` when there is not enough span to compute a rate.
pub(crate) fn speed_window_rate(
    window: &std::collections::VecDeque<(Instant, u64)>,
) -> Option<f64> {
    if window.len() < 2 {
        return None;
    }
    let (t0, b0) = *window.front()?;
    let (t1, b1) = *window.back()?;
    let dt = t1.saturating_duration_since(t0).as_secs_f64();
    if dt <= 0.0 {
        return None;
    }
    Some(b1.saturating_sub(b0) as f64 / dt)
}

/// Internal aggregate progress tracker used by the downloader to drive
/// dynamic-split decisions without depending on tracing-indicatif.
///
/// Tracks bytes downloaded since `started_at`, plus an optional total
/// byte count. ETA is `(total - downloaded) / rate`, where `rate` is the
/// average over the elapsed window.
pub(crate) struct ProgressTracker {
    started_at: Instant,
    downloaded: std::sync::atomic::AtomicU64,
    total: std::sync::atomic::AtomicU64, // 0 means unknown
}

impl ProgressTracker {
    pub fn new(total: Option<u64>) -> Self {
        Self {
            started_at: Instant::now(),
            downloaded: std::sync::atomic::AtomicU64::new(0),
            total: std::sync::atomic::AtomicU64::new(total.unwrap_or(0)),
        }
    }

    pub fn advance(&self, delta: u64) -> u64 {
        let prev = self
            .downloaded
            .fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
        prev + delta
    }

    pub fn downloaded(&self) -> u64 {
        self.downloaded.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn total(&self) -> Option<u64> {
        let t = self.total.load(std::sync::atomic::Ordering::Relaxed);
        if t == 0 { None } else { Some(t) }
    }

    #[allow(dead_code)]
    pub fn set_total(&self, total: Option<u64>) {
        self.total
            .store(total.unwrap_or(0), std::sync::atomic::Ordering::Relaxed);
    }

    pub fn elapsed(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }

    /// Estimated time to completion. `Duration::ZERO` when unknown.
    pub fn eta(&self) -> std::time::Duration {
        let Some(total) = self.total() else {
            return std::time::Duration::ZERO;
        };
        let downloaded = self.downloaded();
        if downloaded == 0 || downloaded >= total {
            return std::time::Duration::ZERO;
        }
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed <= 0.0 {
            return std::time::Duration::ZERO;
        }
        let rate = downloaded as f64 / elapsed;
        if rate <= 0.0 {
            return std::time::Duration::ZERO;
        }
        let remaining = (total - downloaded) as f64;
        std::time::Duration::try_from_secs_f64(remaining / rate)
            .unwrap_or(std::time::Duration::ZERO)
    }
}
