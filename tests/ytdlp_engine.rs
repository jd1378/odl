//! End-to-end tests for the yt-dlp delegation engine, driven by a stand-in
//! script rather than the real tool.
//!
//! Pointing `ytdlp.binary_path` at a script makes the whole delegated path —
//! extraction, format pinning, progress parsing, the final move, resume, and
//! the conflict rules — testable without a network or an installed yt-dlp.

#![cfg(all(unix, feature = "ytdlp"))]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const VIDEO_BYTES: usize = 4096;

/// Info document the stand-in reports for any URL.
///
/// Two video tiers and one audio tier, so format selection has something to
/// decide and the default (`137+251`) is a compound id.
fn info_json() -> String {
    format!(
        r#"{{
  "id": "abc",
  "title": "Fixture Video",
  "extractor": "fixture",
  "ext": "mkv",
  "format_id": "137+251",
  "filesize_approx": {size},
  "subtitles": {{"en": [{{"ext": "vtt"}}, {{"ext": "srt"}}], "live_chat": [{{"ext": "json"}}]}},
  "automatic_captions": {{"de": [{{"ext": "vtt"}}]}},
  "formats": [
    {{"format_id": "18",  "ext": "mp4",  "vcodec": "avc1", "acodec": "mp4a", "height": 360,  "tbr": 600,  "filesize": 1024}},
    {{"format_id": "137", "ext": "mp4",  "vcodec": "avc1", "acodec": "none", "height": 1080, "tbr": 4000, "filesize": 3072}},
    {{"format_id": "251", "ext": "webm", "vcodec": "none", "acodec": "opus", "tbr": 128,     "filesize": 1024}}
  ]
}}"#,
        size = VIDEO_BYTES
    )
}

/// Write a stand-in `yt-dlp` and return its path.
///
/// `download_body` is the shell fragment run for a download invocation; it can
/// assume `$OUT_DIR`, `$STEM`, `$PRINT_FILE` and `$FORMAT` are set. Every
/// invocation appends its argv to `<dir>/calls.log` so tests can assert on
/// what odl actually asked for.
///
/// `$PRINT_FILE` is a bare name resolved against `$OUT_DIR`, matching how
/// yt-dlp resolves `--print-to-file` against its `home` path.
fn write_fake_ytdlp(dir: &Path, download_body: &str) -> PathBuf {
    let path = dir.join("yt-dlp");
    let script = format!(
        r#"#!/bin/sh
set -eu
echo "$@" >> "{log}"

case " $* " in
  *" --version "*) echo "2026.01.01"; exit 0 ;;
esac

# Parse only the flags the fake needs to behave correctly.
OUT_DIR=""
STEM=""
PRINT_FILE=""
FORMAT=""
DUMP_JSON=0
while [ $# -gt 0 ]; do
  case "$1" in
    -J) DUMP_JSON=1 ;;
    -f) FORMAT="$2"; shift ;;
    --paths) OUT_DIR="${{2#home:}}"; shift ;;
    -o) STEM="${{2%%.*}}"; shift ;;
    --skip-download) : ;;
    --print-to-file) PRINT_FILE="$3"; shift 2 ;;
  esac
  shift
done

if [ "$DUMP_JSON" = "1" ]; then
  EXTRACT_FAILS="$(dirname "$0")/extract_fails"
  if [ -f "$EXTRACT_FAILS" ]; then
    want=$(cat "$EXTRACT_FAILS")
    seen_file="$(dirname "$0")/extract_attempts"
    seen=0
    if [ -f "$seen_file" ]; then seen=$(cat "$seen_file"); fi
    seen=$((seen+1)); echo "$seen" > "$seen_file"
    if [ "$seen" -le "$want" ]; then
      echo "ERROR: unable to extract player response" >&2
      exit 1
    fi
  fi
  cat <<'JSON'
{info}
JSON
  exit 0
fi

case " $* " in
  *" --skip-download "*)
    LANG_ARG=$(echo "$@" | sed -n 's/.*--sub-langs \([^ ]*\).*/\1/p')
    printf 'WEBVTT\n\n00:00.000 --> 00:01.000\nhello\n' > "$OUT_DIR/$STEM.$LANG_ARG.srt"
    echo "$OUT_DIR/$STEM.$LANG_ARG.srt" > "$OUT_DIR/$PRINT_FILE"
    exit 0 ;;
esac

{body}
"#,
        log = dir.join("calls.log").display(),
        info = info_json(),
        body = download_body,
    );
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(
        &path,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o755),
    )
    .unwrap();
    path
}

/// A stand-in that fails its first `fail_times` download attempts, then
/// succeeds — the shape of a connection that drops and later recovers.
///
/// Attempts are counted through a file so the count survives each process.
fn flaky_download_body(fail_times: usize) -> String {
    format!(
        r#"
COUNT_FILE="$OUT_DIR/../attempts"
n=0
if [ -f "$COUNT_FILE" ]; then n=$(cat "$COUNT_FILE"); fi
n=$((n+1))
echo "$n" > "$COUNT_FILE"
if [ "$n" -le {fail_times} ]; then
  echo "ERROR: unable to download video data: connection reset" >&2
  exit 1
fi
{success}"#,
        success = successful_download_body()
    )
}

/// A stand-in that downloads successfully, reporting progress for both halves
/// of a merged format the way the real tool does — each format's byte count
/// restarts from zero.
fn successful_download_body() -> String {
    format!(
        r#"
echo '{{"k":"d","d":1024,"t":3072,"s":1000.0,"f":"137","st":"downloading"}}'
echo '{{"k":"d","d":3072,"t":3072,"s":2000.0,"f":"137","st":"finished"}}'
echo '{{"k":"d","d":512,"t":1024,"s":1500.0,"f":"251","st":"downloading"}}'
echo '{{"k":"d","d":1024,"t":1024,"s":1500.0,"f":"251","st":"finished"}}'
echo '{{"k":"p","pp":"Merger","st":"started"}}'
head -c {size} /dev/zero > "$OUT_DIR/$STEM.mkv"
echo "$OUT_DIR/$STEM.mkv" > "$OUT_DIR/$PRINT_FILE"
exit 0
"#,
        size = VIDEO_BYTES
    )
}

struct Fixture {
    _home: tempfile::TempDir,
    data_dir: PathBuf,
    save_dir: PathBuf,
    tool_dir: tempfile::TempDir,
    config: PathBuf,
}

impl Fixture {
    fn new(download_body: &str) -> Self {
        let home = tempfile::tempdir().unwrap();
        let tool_dir = tempfile::tempdir().unwrap();
        let data_dir = home.path().join("data");
        let save_dir = home.path().join("save");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&save_dir).unwrap();

        let binary = write_fake_ytdlp(tool_dir.path(), download_body);
        let config = home.path().join("config.toml");
        std::fs::write(
            &config,
            format!(
                "[ytdlp]\nenabled = true\nbinary_path = {:?}\nextra_hosts = [\"fixture.example\"]\n",
                binary.display().to_string()
            ),
        )
        .unwrap();

        Self {
            _home: home,
            data_dir,
            save_dir,
            tool_dir,
            config,
        }
    }

    fn run(&self, extra: &[&str]) -> std::process::Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_odl"));
        // No `-o`: for a single URL it names the output *file*, and these
        // tests want the name the extractor supplies. The save directory is
        // the working directory instead.
        cmd.current_dir(&self.save_dir)
            .arg("https://fixture.example/watch?v=abc")
            .arg("--download-dir")
            .arg(&self.data_dir)
            .arg("--config-file")
            .arg(&self.config)
            .arg("--choose-format")
            .arg("never")
            .arg("--format")
            .arg("json")
            .args(extra)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.output().expect("failed to spawn odl")
    }

    fn calls(&self) -> String {
        std::fs::read_to_string(self.tool_dir.path().join("calls.log")).unwrap_or_default()
    }

    /// The per-download directory odl created, whatever it named it.
    fn download_dir(&self) -> PathBuf {
        std::fs::read_dir(&self.data_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.is_dir())
            .expect("a download directory should exist")
    }
}

#[test]
fn delegated_download_produces_the_final_file() {
    let fx = Fixture::new(&successful_download_body());
    let out = fx.run(&[]);
    assert!(
        out.status.success(),
        "expected success, got {:?}\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );

    let final_file = fx.save_dir.join("Fixture Video.mkv");
    assert!(
        final_file.exists(),
        "final file missing; save dir holds {:?}",
        std::fs::read_dir(&fx.save_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        std::fs::metadata(&final_file).unwrap().len(),
        VIDEO_BYTES as u64
    );

    // The container comes from yt-dlp's own answer for the resolved format,
    // not from either input stream's extension.
    let calls = fx.calls();
    assert!(calls.contains("-f 137+251"), "calls were:\n{calls}");
}

#[test]
fn progress_events_report_the_sum_across_merged_formats() {
    let fx = Fixture::new(&successful_download_body());
    let out = fx.run(&[]);
    assert!(out.status.success());

    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut peak = 0u64;
    let mut saw_post_processing = false;
    for line in stdout.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if v["type"] == "progress"
            && let Some(d) = v["downloaded"].as_u64()
        {
            peak = peak.max(d);
        }
        if v["phase"] == "post_processing" {
            saw_post_processing = true;
        }
    }
    // 3072 from the video plus 1024 from the audio: a reader that followed
    // the per-format counter would have seen the total drop to 512 instead.
    assert_eq!(peak, 4096, "progress should aggregate both formats");
    assert!(saw_post_processing, "merging should be reported as a phase");
}

#[test]
fn status_reports_the_engine_and_a_completed_download() {
    let fx = Fixture::new(&successful_download_body());
    assert!(fx.run(&[]).status.success());

    // Top-level options precede the subcommand.
    let out = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg("--download-dir")
        .arg(&fx.data_dir)
        .arg("--config-file")
        .arg(&fx.config)
        .arg("status")
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");
    assert!(out.status.success());

    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let d = &v["downloads"][0];
    assert_eq!(d["engine"], "ytdlp");
    assert_eq!(d["finished"], true);
    assert_eq!(d["filename"], "Fixture Video.mkv");
}

#[test]
fn a_second_run_reuses_the_finished_file_instead_of_downloading_again() {
    let fx = Fixture::new(&successful_download_body());
    assert!(fx.run(&[]).status.success());
    let first = fx.calls().lines().count();

    let out = fx.run(&["--on-same-download-exists", "resume"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let second = fx.calls().lines().count();
    // A finished download must not re-run the downloader; only the version
    // check and extraction are allowed to repeat.
    let new_calls: Vec<String> = fx.calls().lines().skip(first).map(str::to_owned).collect();
    assert!(
        second > first,
        "the second run should have invoked the tool"
    );
    assert!(
        new_calls
            .iter()
            .all(|c| c.contains("--version") || c.contains("-J")),
        "second run should not download again; it ran:\n{new_calls:#?}"
    );
}

#[test]
fn an_interrupted_download_resumes_with_the_same_pinned_format() {
    // First run fails after leaving a partial file behind.
    let failing = r#"
echo '{"k":"d","d":1024,"t":3072,"s":900.0,"f":"137","st":"downloading"}'
head -c 1024 /dev/zero > "$OUT_DIR/$STEM.mkv.part"
echo "network died" >&2
exit 1
"#;
    let fx = Fixture::new(failing);
    let out = fx.run(&[]);
    assert!(!out.status.success(), "the first run is meant to fail");

    let dir = fx.download_dir();
    let partial: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".part"))
        .collect();
    assert_eq!(
        partial.len(),
        1,
        "a partial file should remain: {partial:?}"
    );

    // Swap in a stand-in that succeeds, then resume.
    write_fake_ytdlp(fx.tool_dir.path(), &successful_download_body());
    let out = fx.run(&["--on-same-download-exists", "resume"]);
    assert!(
        out.status.success(),
        "resume failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(fx.save_dir.join("Fixture Video.mkv").exists());

    // Every download invocation asked for the format pinned on the first run.
    // Continuing a partial under a different format is the silent-corruption
    // case this guards.
    for call in fx.calls().lines().filter(|c| c.contains("--paths")) {
        assert!(call.contains("-f 137+251"), "unpinned resume: {call}");
    }
}

#[test]
fn a_different_url_with_the_same_title_does_not_continue_the_other_download() {
    let failing = r#"
head -c 1024 /dev/zero > "$OUT_DIR/$STEM.mkv.part"
exit 1
"#;
    let fx = Fixture::new(failing);
    assert!(!fx.run(&[]).status.success());

    // A second URL that the extractor reports under the same title lands in
    // the same directory. Resuming it would splice two videos together.
    let out = Command::new(env!("CARGO_BIN_EXE_odl"))
        .current_dir(&fx.save_dir)
        .arg("https://fixture.example/watch?v=different")
        .arg("--download-dir")
        .arg(&fx.data_dir)
        .arg("--config-file")
        .arg(&fx.config)
        .arg("--choose-format")
        .arg("never")
        .arg("--on-same-download-exists")
        .arg("resume")
        .arg("--on-file-changed")
        .arg("abort")
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");

    assert_eq!(
        out.status.code(),
        Some(4),
        "a different source URL must surface as a conflict, got: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn forcing_the_engine_on_a_host_that_is_not_delegated_still_uses_it() {
    let fx = Fixture::new(&successful_download_body());
    let out = Command::new(env!("CARGO_BIN_EXE_odl"))
        .current_dir(&fx.save_dir)
        .arg("https://not-listed.example/video/1")
        .arg("--download-dir")
        .arg(&fx.data_dir)
        .arg("--config-file")
        .arg(&fx.config)
        .arg("--engine")
        .arg("ytdlp")
        .arg("--choose-format")
        .arg("never")
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");

    assert!(
        out.status.success(),
        "forced engine should bypass the host list: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(fx.save_dir.join("Fixture Video.mkv").exists());
}

#[test]
fn a_delegated_host_falls_back_to_http_when_the_tool_is_missing() {
    let home = tempfile::tempdir().unwrap();
    let data_dir = home.path().join("data");
    let save_dir = home.path().join("save");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&save_dir).unwrap();

    let config = home.path().join("config.toml");
    std::fs::write(
        &config,
        "[ytdlp]\nenabled = true\nbinary_path = \"/nonexistent/odl-test/yt-dlp\"\nextra_hosts = [\"fixture.example\"]\n",
    )
    .unwrap();

    // `auto` must not fail because the tool is absent; it falls through to the
    // HTTP engine, which then fails on its own terms (DNS), not with a
    // yt-dlp error.
    let out = Command::new(env!("CARGO_BIN_EXE_odl"))
        .current_dir(&save_dir)
        .arg("https://fixture.example/watch?v=abc")
        .arg("--download-dir")
        .arg(&data_dir)
        .arg("--config-file")
        .arg(&config)
        .arg("--max-retries")
        .arg("0")
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_ne!(
        out.status.code(),
        Some(7),
        "auto mode should not report a yt-dlp failure: {stderr}"
    );
    assert!(
        stderr.contains("network") || stderr.contains("\"kind\":\"network\""),
        "expected the HTTP engine to have taken over, got: {stderr}"
    );
}

/// Total size of the deterministic payload used by the resumable stand-in.
const RESUMABLE_BYTES: usize = 40_000;

/// A stand-in whose output depends on byte position, so a resume that
/// mis-splices — dropping, duplicating, or restarting a range — produces a
/// different file rather than a coincidentally equal one.
///
/// `stop_after` truncates the transfer and fails, simulating a dropped
/// connection; `None` runs to completion, honouring whatever `.part` a
/// previous attempt left behind.
fn resumable_download_body(stop_after: Option<usize>) -> String {
    let tail = match stop_after {
        Some(limit) => format!(
            r#"
yes "0123456789abcdef" | head -c {limit} | tail -c +$((have+1)) >> "$PART"
echo "connection reset" >&2
exit 1
"#
        ),
        None => format!(
            r#"
yes "0123456789abcdef" | head -c {total} | tail -c +$((have+1)) >> "$PART"
mv "$PART" "$OUT_DIR/$STEM.mkv"
echo "$OUT_DIR/$STEM.mkv" > "$OUT_DIR/$PRINT_FILE"
exit 0
"#,
            total = RESUMABLE_BYTES
        ),
    };
    format!(
        r#"
PART="$OUT_DIR/$STEM.mkv.part"
have=0
if [ -f "$PART" ]; then have=$(wc -c < "$PART" | tr -d ' '); fi
echo "{{\"k\":\"d\",\"d\":$have,\"t\":{total},\"s\":900.0,\"f\":\"137\",\"st\":\"downloading\"}}"
{tail}"#,
        total = RESUMABLE_BYTES
    )
}

#[test]
fn a_resumed_download_is_byte_identical_to_an_uninterrupted_one() {
    // Interrupted, then resumed.
    let interrupted = Fixture::new(&resumable_download_body(Some(RESUMABLE_BYTES / 3)));
    assert!(
        !interrupted.run(&[]).status.success(),
        "the first attempt is meant to fail partway"
    );
    write_fake_ytdlp(interrupted.tool_dir.path(), &resumable_download_body(None));
    let out = interrupted.run(&["--on-same-download-exists", "resume"]);
    assert!(
        out.status.success(),
        "resume failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The same download, never interrupted.
    let clean = Fixture::new(&resumable_download_body(None));
    assert!(clean.run(&[]).status.success());

    let resumed = std::fs::read(interrupted.save_dir.join("Fixture Video.mkv")).unwrap();
    let straight = std::fs::read(clean.save_dir.join("Fixture Video.mkv")).unwrap();
    assert_eq!(
        resumed.len(),
        RESUMABLE_BYTES,
        "resumed file has the wrong length"
    );
    assert_eq!(
        resumed, straight,
        "a resumed download must produce exactly the bytes an uninterrupted one would"
    );
}

#[test]
fn a_vanished_format_discards_the_partial_and_clears_the_pin() {
    // Leave a partial behind, pinned to the format chosen on the first run.
    let fx = Fixture::new(&resumable_download_body(Some(RESUMABLE_BYTES / 3)));
    assert!(!fx.run(&[]).status.success());
    let dir = fx.download_dir();
    let partials = |dir: &Path| -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".part"))
            .collect()
    };
    assert_eq!(partials(&dir).len(), 1, "expected a partial to resume from");

    // The site stops offering that format.
    write_fake_ytdlp(
        fx.tool_dir.path(),
        r#"
echo "ERROR: [fixture] Requested format is not available" >&2
exit 1
"#,
    );
    let out = fx.run(&["--on-same-download-exists", "resume"]);
    assert_eq!(
        out.status.code(),
        Some(7),
        "expected a yt-dlp error: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Continuing bytes of an encoding the site no longer serves is the
    // silent-corruption case; they must be gone rather than kept.
    assert!(
        partials(&dir).is_empty(),
        "the unusable partial should have been discarded: {:?}",
        partials(&dir)
    );

    // With the pin cleared, an ordinary run picks a format afresh.
    write_fake_ytdlp(fx.tool_dir.path(), &successful_download_body());
    let out = fx.run(&["--on-same-download-exists", "resume"]);
    assert!(
        out.status.success(),
        "recovery run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(fx.save_dir.join("Fixture Video.mkv").exists());
}

#[test]
fn interrupting_stops_the_helper_and_everything_it_spawned() {
    // The helper starts a grandchild that keeps writing, then waits — the
    // shape of yt-dlp spawning ffmpeg.
    let fx = Fixture::new(
        r#"
( while true; do echo tick >> "$OUT_DIR/grandchild.log"; sleep 0.05; done ) &
head -c 2048 /dev/zero > "$OUT_DIR/$STEM.mkv.part"
sleep 30
exit 0
"#,
    );

    let mut child = Command::new(env!("CARGO_BIN_EXE_odl"))
        .current_dir(&fx.save_dir)
        .arg("https://fixture.example/watch?v=abc")
        .arg("--download-dir")
        .arg(&fx.data_dir)
        .arg("--config-file")
        .arg(&fx.config)
        .arg("--choose-format")
        .arg("never")
        .arg("--format")
        .arg("json")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn odl");

    // Let the helper get going and the grandchild write something.
    std::thread::sleep(std::time::Duration::from_millis(1500));
    let dir = fx.download_dir();
    let marker = dir.join("grandchild.log");
    assert!(
        marker.exists() && std::fs::metadata(&marker).unwrap().len() > 0,
        "the grandchild should be running before the interrupt"
    );

    Command::new("kill")
        .arg("-INT")
        .arg(child.id().to_string())
        .status()
        .expect("failed to signal odl");

    let status = child.wait().expect("odl did not exit");
    assert_eq!(
        status.code(),
        Some(130),
        "an interrupt should be reported as a cancellation"
    );

    // The helper runs in its own process group, so the terminal's signal
    // never reaches it: if odl did not tear the group down, the grandchild
    // is still writing.
    let after = std::fs::metadata(&marker).unwrap().len();
    std::thread::sleep(std::time::Duration::from_millis(400));
    assert_eq!(
        after,
        std::fs::metadata(&marker).unwrap().len(),
        "a grandchild kept running after the interrupt"
    );

    // Cancelling must not throw away what was transferred.
    let partial: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".part"))
        .collect();
    assert_eq!(partial.len(), 1, "the partial should survive for a resume");
}

/// The status document is a documented contract for scripts and agents, so
/// the fields that describe a delegated download must be present and correct
/// rather than merely absent-and-defaulted.
#[test]
fn status_and_probe_documents_describe_the_delegated_engine() {
    let fx = Fixture::new(&successful_download_body());
    assert!(fx.run(&[]).status.success());

    let out = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg("--download-dir")
        .arg(&fx.data_dir)
        .arg("--config-file")
        .arg(&fx.config)
        .arg("status")
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let d = &v["downloads"][0];

    assert_eq!(d["engine"], "ytdlp");
    assert_eq!(d["finished"], true);
    assert_eq!(d["percent"], 100.0);
    // Measuring the finished file retires the extractor's estimate; leaving
    // this true would keep flagging an exact figure as approximate.
    assert_eq!(d["size_is_approx"], false);
    assert_eq!(d["size"], VIDEO_BYTES as u64);

    let out = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg("--config-file")
        .arg(&fx.config)
        .arg("probe")
        .arg("https://fixture.example/watch?v=abc")
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();

    assert_eq!(v["engine"], "ytdlp");
    assert_eq!(
        v["size_is_approx"], true,
        "the extractor reported an estimate"
    );
    // Keys stay present so parsing is uniform; null means "this engine cannot
    // know", which is why they are not simply omitted.
    assert!(v["etag"].is_null());
    assert!(v["last_modified"].is_null());
    assert_eq!(v["checksums"].as_array().unwrap().len(), 0);
}

#[test]
fn asking_again_lets_the_quality_change_by_starting_over() {
    // A download stopped partway, pinned to the format chosen the first time.
    let fx = Fixture::new(&resumable_download_body(Some(RESUMABLE_BYTES / 3)));
    assert!(!fx.run(&[]).status.success());
    let dir = fx.download_dir();
    let partial_size = |dir: &Path| -> Option<u64> {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().ends_with(".part"))
            .map(|e| e.metadata().unwrap().len())
    };
    assert_eq!(partial_size(&dir), Some((RESUMABLE_BYTES / 3) as u64));

    // An ordinary re-run keeps the pinned format and continues.
    write_fake_ytdlp(fx.tool_dir.path(), &successful_download_body());
    let out = fx.run(&["--on-same-download-exists", "resume"]);
    assert!(out.status.success());
    for call in fx.calls().lines().filter(|c| c.contains("--paths")) {
        assert!(
            call.contains("-f 137+251"),
            "an ordinary re-run must not re-decide the format: {call}"
        );
    }
}

#[test]
fn changing_quality_requires_discarding_what_was_downloaded() {
    let fx = Fixture::new(&resumable_download_body(Some(RESUMABLE_BYTES / 3)));
    assert!(!fx.run(&[]).status.success());

    // Asking for a different quality is a conflict, not a resume: the bytes on
    // disk are of the old encoding and nothing can splice the two.
    let out = fx.run(&[
        "--on-same-download-exists",
        "resume",
        "--on-file-changed",
        "abort",
        "--format-id",
        "18",
    ]);
    assert_eq!(
        out.status.code(),
        Some(4),
        "expected a conflict, got: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn a_transcript_can_be_downloaded_instead_of_the_media() {
    let fx = Fixture::new(&successful_download_body());
    let out = fx.run(&["--format-id", "subs:en"]);
    assert!(
        out.status.success(),
        "transcript download failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The file is the transcript, not the video.
    let saved = fx.save_dir.join("Fixture Video.srt");
    assert!(
        saved.exists(),
        "expected a subtitle file; save dir holds {:?}",
        std::fs::read_dir(&fx.save_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );

    let call = fx
        .calls()
        .lines()
        .find(|c| c.contains("--paths"))
        .expect("a download invocation")
        .to_owned();
    // Asking for a media format alongside would fetch the video too, which is
    // the opposite of what "transcript only" means.
    assert!(call.contains("--skip-download"), "{call}");
    assert!(call.contains("--write-subs"), "{call}");
    assert!(call.contains("--sub-langs en"), "{call}");
    assert!(
        !call.contains("-f "),
        "no media format may be requested: {call}"
    );
}

#[test]
fn a_transcript_choice_survives_into_the_stored_metadata() {
    let fx = Fixture::new(&successful_download_body());
    assert!(fx.run(&["--format-id", "autosubs:de"]).status.success());

    let out = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg("--download-dir")
        .arg(&fx.data_dir)
        .arg("--config-file")
        .arg(&fx.config)
        .arg("status")
        .arg("--format")
        .arg("json")
        .output()
        .expect("failed to spawn odl");
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    // The pinned id encodes the language and that it is machine-generated, so
    // a restart describes the download the same way.
    assert_eq!(v["downloads"][0]["quality"], "auto-subtitles (de)");
}

/// An automated caller must never be blocked by a question it cannot see.
#[test]
fn nothing_prompts_when_there_is_no_one_to_ask() {
    let fx = Fixture::new(&successful_download_body());

    // `--choose-format always` is the strongest request to prompt there is.
    // With JSON output and no terminal it must still run straight through.
    let out = Command::new(env!("CARGO_BIN_EXE_odl"))
        .current_dir(&fx.save_dir)
        .arg("https://fixture.example/watch?v=abc")
        .arg("--download-dir")
        .arg(&fx.data_dir)
        .arg("--config-file")
        .arg(&fx.config)
        .arg("--choose-format")
        .arg("always")
        .arg("--format")
        .arg("json")
        .stdin(Stdio::null())
        .output()
        .expect("failed to spawn odl");

    assert!(
        out.status.success(),
        "a non-interactive run must complete: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(fx.save_dir.join("Fixture Video.mkv").exists());
}

/// Declining is a decision; being unable to ask is not.
#[test]
fn an_unanswerable_install_offer_records_nothing() {
    let home = tempfile::tempdir().unwrap();
    let config = home.path().join("config.toml");
    std::fs::write(
        &config,
        "[ytdlp]\nenabled = true\nbinary_path = \"/nonexistent/odl-test/yt-dlp\"\n",
    )
    .unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_odl"))
        .arg("--config-file")
        .arg(&config)
        .arg("tools")
        .arg("install")
        .arg("yt-dlp")
        .stdin(Stdio::null())
        .output()
        .expect("failed to spawn odl");
    assert!(out.status.success(), "must not fail, and must not hang");

    // Recording a refusal nobody made would silently disable a future offer.
    let written = std::fs::read_to_string(&config).unwrap();
    assert!(
        !written.contains("offer_ytdlp_install = false"),
        "a decline was recorded without anyone being asked:\n{written}"
    );
}

#[test]
fn a_process_that_died_is_started_afresh_once() {
    // The tool exhausts its own retries before exiting, so what a failure
    // leaves worth trying is a new extraction — once, not once per configured
    // retry.
    let fx = Fixture::new(&flaky_download_body(1));
    let out = fx.run(&["--max-retries", "3", "--wait-between-retries", "10ms"]);
    assert!(
        out.status.success(),
        "a fresh process should recover the download: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(fx.save_dir.join("Fixture Video.mkv").exists());
    assert_eq!(attempts(&fx), 2, "one failure, then one fresh run");
}

#[test]
fn restarting_does_not_multiply_the_configured_retries() {
    // Stacking a respawn loop on the tool's own would turn a configured
    // "three tries" into sixteen. The transfer retries belong to yt-dlp; only
    // the re-extraction belongs here.
    let fx = Fixture::new(&flaky_download_body(99));
    let out = fx.run(&["--max-retries", "3", "--wait-between-retries", "10ms"]);
    assert_eq!(out.status.code(), Some(7), "the download should fail");
    assert_eq!(attempts(&fx), 2, "one attempt plus one restart");

    let call = fx
        .calls()
        .lines()
        .find(|c| c.contains("--paths"))
        .expect("a download invocation")
        .to_owned();
    // The transfer retries stay with the tool, where a retry reuses the URL
    // it already has instead of costing a fresh extraction.
    assert!(call.contains("--retries 3"), "{call}");
    assert!(call.contains("--fragment-retries 3"), "{call}");
}

#[test]
fn asking_for_no_retries_starts_nothing_afresh() {
    let fx = Fixture::new(&flaky_download_body(99));
    let out = fx.run(&["--max-retries", "0"]);
    assert_eq!(out.status.code(), Some(7));
    assert_eq!(attempts(&fx), 1, "no retries means exactly one attempt");
}

/// How many times the stand-in was invoked for a download.
fn attempts(fx: &Fixture) -> usize {
    std::fs::read_to_string(fx.data_dir.join("attempts"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

#[test]
fn a_settled_failure_is_not_retried() {
    // An unsupported URL will be unsupported next time too; spending the
    // user's backoff to learn that again helps nobody.
    let fx = Fixture::new(
        r#"
COUNT_FILE="$OUT_DIR/../attempts"
n=0
if [ -f "$COUNT_FILE" ]; then n=$(cat "$COUNT_FILE"); fi
echo "$((n+1))" > "$COUNT_FILE"
echo "ERROR: Unsupported URL: https://fixture.example/watch" >&2
exit 1
"#,
    );
    let out = fx.run(&["--max-retries", "5", "--wait-between-retries", "10ms"]);
    assert_eq!(out.status.code(), Some(7));
    assert_eq!(attempts(&fx), 1, "a settled failure must be attempted once");
}

#[test]
fn a_failed_extraction_is_retried_on_the_full_policy() {
    // Extraction is the phase odl owns: the tool is told not to retry it, so
    // the configured count has to be spent here or it is lost entirely.
    let fx = Fixture::new(&successful_download_body());
    std::fs::write(fx.tool_dir.path().join("extract_fails"), "2").unwrap();

    let out = fx.run(&["--max-retries", "3", "--wait-between-retries", "10ms"]);
    assert!(
        out.status.success(),
        "two extraction failures should be retried through: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let tries: usize = std::fs::read_to_string(fx.tool_dir.path().join("extract_attempts"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    // Two failures, then the run that succeeded, then the download's own
    // extraction — more than the single restart a transfer failure gets.
    assert!(
        tries >= 3,
        "expected the failures to be retried, saw {tries}"
    );

    // The tool must not have been quietly retrying underneath us.
    let call = fx
        .calls()
        .lines()
        .find(|c| c.contains(" -J "))
        .expect("an extraction invocation")
        .to_owned();
    assert!(call.contains("--extractor-retries 0"), "{call}");
}

#[test]
fn extraction_failures_stop_at_the_configured_limit() {
    let fx = Fixture::new(&successful_download_body());
    std::fs::write(fx.tool_dir.path().join("extract_fails"), "99").unwrap();

    let out = fx.run(&["--max-retries", "2", "--wait-between-retries", "10ms"]);
    assert_eq!(out.status.code(), Some(7), "it should give up, not loop");

    let tries: usize = std::fs::read_to_string(fx.tool_dir.path().join("extract_attempts"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(tries, 3, "one attempt plus the two configured retries");
}
