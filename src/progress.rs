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
use std::time::{Duration, Instant};

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
pub enum Phase {
    /// Probing the server (HEAD request, redirect resolution, etc.).
    Evaluating,
    /// Resolving save / server conflicts before download begins.
    ResolvingConflicts,
    /// Actively downloading file parts.
    Downloading,
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
    /// Latest sampled bytes-per-second for a single part. Emitted on the
    /// same cadence as aggregate [`ProgressEvent::Speed`]. Raw window
    /// rate, no smoothing.
    PartSpeed { ulid: String, bytes_per_second: f64 },
    /// A part is being retried.
    PartRetrying { ulid: String, attempt: u32 },
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
/// Implementations must be cheap to call from hot paths: every received byte
/// triggers at least one `on_event(Progress { .. })` call.
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
        let worker = tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                inner.on_event(ev);
            }
        });
        Arc::new(Self {
            tx,
            _worker: worker,
        })
    }
}

impl ProgressReporter for AsyncReporter {
    fn on_event(&self, event: ProgressEvent) {
        // Lock-free: tokio's UnboundedSender uses an atomic intrusive
        // queue, no Mutex on the producer path.
        let _ = self.tx.send(event);
    }
}

/// Per-download context: reporter + cancellation token.
///
/// Cheap to clone (`Arc` and a `CancellationToken` clone). One context per
/// `DownloadManager::download_with` call.
#[derive(Clone)]
pub struct DownloadContext {
    pub reporter: Arc<dyn ProgressReporter>,
    pub cancel: CancellationToken,
    /// Optional URL the GUI knows this context by. Reporters that
    /// multiplex many downloads onto one channel use this to disambiguate.
    pub url: Option<Url>,
}

impl DownloadContext {
    pub fn new() -> Self {
        Self {
            reporter: Arc::new(NoopReporter),
            cancel: CancellationToken::new(),
            url: None,
        }
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
            .finish_non_exhaustive()
    }
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
