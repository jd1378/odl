//! Machine-readable (NDJSON / JSON) output for `--format json`.
//!
//! Download progress is streamed as newline-delimited JSON objects on
//! stdout — one object per line, each tagged with a `type` field and the
//! owning `url` so an agent multiplexing several concurrent downloads can
//! attribute every event. Each `println!` writes one whole line under the
//! stdout lock, so lines from concurrent download tasks never interleave.
//!
//! One-shot commands (`config --show`, `probe`, `status`/`list`) emit a
//! single JSON document instead; those are built directly in `main`.

use odl::progress::{Phase, ProgressEvent, ProgressReporter};
use serde_json::json;

pub fn phase_str(phase: Phase) -> &'static str {
    match phase {
        Phase::Evaluating => "evaluating",
        Phase::ResolvingConflicts => "resolving_conflicts",
        Phase::Downloading => "downloading",
        Phase::PostProcessing => "post_processing",
        Phase::Assembling => "assembling",
        Phase::Flushing => "flushing",
        Phase::Verifying => "verifying",
        // `Phase` is non-exhaustive; an unknown phase is still a phase, and a
        // consumer parsing this stream should not choke on it.
        _ => "unknown",
    }
}

/// Print one JSON value as a single NDJSON line on stdout.
pub fn emit_line(value: serde_json::Value) {
    println!("{value}");
}

/// `ProgressReporter` that emits NDJSON lifecycle events on stdout. One
/// instance per download; the `url` it was constructed with tags every
/// event. High-frequency, low-signal events (speed samples, per-part
/// progress) are intentionally dropped to keep the stream parseable —
/// aggregate `progress` events carry enough to derive throughput.
pub struct JsonReporter {
    url: String,
}

impl JsonReporter {
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

impl ProgressReporter for JsonReporter {
    fn on_event(&self, event: ProgressEvent) {
        let url = &self.url;
        match event {
            ProgressEvent::FilenameResolved(name) => {
                emit_line(json!({"type": "filename", "url": url, "filename": name}));
            }
            ProgressEvent::PhaseChanged(phase) => {
                emit_line(json!({"type": "phase", "url": url, "phase": phase_str(phase)}));
            }
            ProgressEvent::Progress { downloaded, total } => {
                emit_line(json!({
                    "type": "progress",
                    "url": url,
                    "downloaded": downloaded,
                    "total": total,
                }));
            }
            ProgressEvent::Message(message) => {
                if !message.is_empty() {
                    emit_line(json!({"type": "message", "url": url, "message": message}));
                }
            }
            ProgressEvent::Completed {
                path,
                already_complete,
            } => {
                emit_line(json!({
                    "type": "completed",
                    "url": url,
                    "path": path.to_string_lossy(),
                    "already_complete": already_complete,
                }));
            }
            ProgressEvent::Cancelled => {
                emit_line(json!({"type": "cancelled", "url": url}));
            }
            ProgressEvent::Failed { message } => {
                emit_line(json!({"type": "failed", "url": url, "message": message}));
            }
            // Rare, and exactly what a caller needs to render "resuming in
            // 30s" instead of an unexplained pause — so unlike the per-part
            // chatter below, this one is worth a line.
            ProgressEvent::RetryScheduled {
                ulid,
                attempt,
                max_attempts,
                delay,
                server_requested,
            } => {
                emit_line(json!({
                    "type": "retry_scheduled",
                    "url": url,
                    "part": ulid,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "delay_ms": delay.as_millis() as u64,
                    "server_requested": server_requested,
                }));
            }
            // Speed samples and per-part events are too noisy for the
            // line-oriented stream; aggregate progress is sufficient. The
            // wildcard also absorbs whatever a future engine reports: this
            // stream is a documented contract, and inventing a shape for an
            // event nobody has specified would be worse than staying silent.
            ProgressEvent::Speed { .. }
            | ProgressEvent::PartAdded { .. }
            | ProgressEvent::PartProgress { .. }
            | ProgressEvent::PartFinished { .. }
            | ProgressEvent::PartSpeed { .. }
            | ProgressEvent::PartRetrying { .. }
            | _ => {}
        }
    }
}
