# Changelog

## 2.0.3

### A resumed download could fail with all its bytes present

A part whose bytes were already complete on disk finished without transferring
anything — and so never sent the first-chunk signal the scheduler waits for
before opening the next part in a ramp batch. The batch wait drained the task
set and returned with the rest of the queue unopened, and the run ended
reporting parts that "could not be downloaded" while every byte of them sat on
disk. Reachable after a crash between writing a part's bytes and recording it
as finished.

### A finished part now reports its size

`PartFinished` carries no byte count, so a UI rendering "downloaded / total"
had only the progress sampler's last word — up to one 125 ms tick stale, and
for a part already complete on disk, never spoken at all. Such a part showed
as complete and empty at the same time; an ordinary part could show 98% while
finishing. A final `PartProgress` with `downloaded == total` is now emitted
immediately before every `PartFinished`, so the two agree at the source.

A dynamic split also says so: shrinking a part's size used to leave the old
total standing until the next sample, and if the part finished or the download
paused inside that window it was never corrected.

## 2.0.2

### Stopping a waiting download

A download interrupted *while sitting in a retry backoff* reported the error
that preceded the stop instead of a cancellation: exit 1 with "All parts
failed", where a stop should exit 130. `wait_for_retry` returns `false` both
when the retry budget is spent and when the wait is interrupted, and every
caller read that as exhaustion.

It matters beyond the exit code — a download manager or CI wrapper that
restarts failed jobs would restart one the user had just paused, repeatedly.

Fixed at all three places that consult the retry policy: part transfers, the
initial probe, and yt-dlp respawns. The probe already had a cancellation check
sitting after its early return, where it could never run.

Reproduced against an always-503 server with a 20s backoff, interrupted 3s in:
1 run in 10 before, 0 in 20 after. The regression test repeats the interrupt
six times, since a single pass would miss it.

`wait_for_retry` keeps its `bool` return: an enum would let the compiler force
each caller to handle cancellation — three of them independently forgetting the
same check is the argument for it — but that is published API, so it waits for
3.0. The ambiguity is documented on the function meanwhile.

## 2.0.1

The first complete 2.0 release: 2.0.0's binaries were never published for
32-bit Linux or 32-bit ARM. A test asserted that every target odl ships for has
a yt-dlp build to install, which was never true — yt-dlp publishes no
standalone build for those platforms, and odl correctly declines to offer an
install there. The assertion failed the release on four targets. Library
behaviour is unchanged, so 2.0.0 from crates.io is unaffected.

### Media sites are delegated to yt-dlp

Links on a curated set of media hosts — YouTube, Vimeo, Twitch, Bilibili,
X/Twitter, SoundCloud and others — are handed to an externally installed
`yt-dlp`. Everything else downloads over odl's own multipart HTTP engine
exactly as before. `yt-dlp` is never bundled: it is found at runtime, and its
absence simply means the link is downloaded over HTTP.

- Quality selection, with the container each choice produces and, where the
  site publishes them, transcript tracks. `--choose-format`, `--format-id`.
- Resuming re-extracts from the stored page URL, because media URLs are signed
  and short-lived. The chosen format is pinned, so a partial file is never
  continued in a different encoding; changing quality discards and restarts.
- `odl tools status` / `odl tools install` fetch `yt-dlp` and `ffmpeg` on
  request, through odl's own downloader — resumable, retrying, and verified
  against the checksums published with them.
- `--engine auto|http|ytdlp` forces the choice.
- A download is identified by the extractor's id, not by the URL that was
  pasted, so `youtu.be/X`, `watch?v=X` and a timestamped link all resume the
  same partial file instead of reporting a conflict. The canonical page URL is
  what gets stored and re-extracted from.

### Checksums are usable on their own

`HashDigest::from_path` and `verify_file` make the hashing that verifies
downloads available to any caller, alongside `matches`, which compares by
value rather than by written form — the same hash in hex and in base64 is
equal under it and not under `==`. `Download::checksums` reads what a download will be checked against, and
`clear_checksums` discards it. To keep the values but stop odl acting on them,
set `verify_checksums = false` or pass `--no-verify-checksums`: the file's size
is still checked, but hashing its contents becomes the caller's business.

Hashing now reads 256 KiB at a time instead of 8 KiB: about four times faster
on large files, at unchanged memory.

### Dependencies

Updated across the tree, chiefly for security: the vendored OpenSSL moves to
3.6.3, which matters because `native-tls-vendored` links it *into* the shipped
binary, so users get odl's copy rather than their system's. Also picked up
HTTP/1 and HTTP/2 fixes on the streaming path, several `futures` soundness
fixes, and a clean `cargo audit`. Verified to still compile on the declared
1.88 minimum.

One of those fixes is user-visible: `reqwest` now strips `Authorization` and
`Cookie` headers when a redirect changes scheme. If you supply credentials via
`--header` or config and your URL redirects from `https` to `http`, those
headers are no longer forwarded, and a server that relied on them will answer
401. That is the safer behaviour — the previous one leaked credentials over
plaintext — but it can look like a regression.

`ulid` moved to 3.0. Part files are named by ULID and those names live in
persisted metadata, so the on-disk format is a compatibility contract — it is
unchanged across the versions, and a download interrupted by 1.x resumes
correctly under 3.0. A test now pins that shape so a future dependency change
cannot alter it quietly.

`base64` was deliberately held at 0.22: the upgrade fixes nothing odl is
affected by, and would compile two copies into the binary since `reqwest`
still requires 0.22.

### Filename safety

The sanitiser behind every download directory and output name had three holes,
all reachable from a title odl does not control:

- It panicked when a name longer than 255 bytes had to be cut at a byte that
  was mid-character — which any long non-Latin title is.
- Reserved Windows device names were only escaped without an extension, so
  `NUL` became `NUL_` but `NUL.mkv` went through unchanged.
- Names made only of dots or spaces sanitised to nothing, and an empty
  component resolves to its own parent directory.

### ASCII filenames, on request

`ascii_filenames = true` / `--ascii-filenames` transliterates a name to ASCII
before sanitising it: `Café Münster` saves as `Cafe Munster`, `Приветствие` as
`Privetstvie`, `中文标题` as `Zhong Wen Biao Ti`. Every script, not just
accented Latin, via `deunicode`.

Off by default, and deliberately so: it is lossy, and it renames the
per-download directory, so switching it makes a download already in progress
start over instead of resuming.

### Part responses are validated

A part's `GET` used to be streamed to disk without anyone looking at its
status. A server that stopped honouring `Range` and answered `200` with the
whole file had that body written at each part's offset — a file of the right
length, the wrong contents, and exit code 0. A `5xx` error page was written as
part data, and multi-part it looped forever, counting the error page as
progress and ignoring `--max-retries 0`.

Now a ranged request must be answered `206` from the offset it asked for.
Anything else is a conflict rather than data — except the case that has to
keep working: a single connection with nothing downloaded yet, where a `200`
returns exactly the bytes requested. That stays accepted whether the body
arrives with a `Content-Length` or chunked without one.

### Retries say what they are waiting for

A download that pauses to retry looked identical to
one that had hung: the only sign was a free-form `message` string nothing could
parse. The new `retry_scheduled` event carries `delay_ms`, `attempt`,
`max_attempts`, the part it belongs to, and whether the delay is the server's
own — so a UI can say "resuming in 30s" and mean it.

`Retry-After` is honoured on the statuses that carry it (`408`, `425`, `429`,
5xx), capped at five minutes since the header is server-supplied. The attempt
budget stays the caller's; the header only moves *when* the next attempt
happens, not whether there is one.

A refusal the server means permanently is no longer retried at all. A `404`, `410`,
`403`, `401` or `416` on a part used to spend the full retry budget with
backoff — seconds, to reach the answer the first response already gave — and
then exit 3, the class that tells a script or a GUI to try the whole thing
again. Those now fail the part immediately and exit 4, the conflict class, so
a dead link stops looking like a busy server. `408`, `425`, `429` and 5xx keep
their retries and their retryable class.

### Failures report their cause

A transfer that fails now says what failed. When every part in flight exhausts
its retries at once, the run ends with work still queued — correctly, since
the retry budget is the caller's stated tolerance and it is spent. But it used
to end as *success*, leaving the assembler to notice and report "part file
shorter than recorded size": an I/O error for what was plainly a failed
transfer. The cause is now carried out of the scheduler, so the same download
exits 3 with `HTTP 503 Service Unavailable`.

A download stopped by the caller reports as cancelled rather than as a
failure. With nothing left in flight the run loop's `select!` could take
either branch, so a stop had an even chance of surfacing as exit 1 instead of
130 — and of being restarted by a caller that auto-retries failures.

A failed download no longer leaves an output file behind. Assembly sizes the
destination up front, so a failure part-way through left a full-length file of
mostly zeros sitting where the download was meant to land, with nothing to
mark it as junk.

### Breaking changes

Metadata written by 1.x still loads — the engine discriminant defaults to
`http_multipart`, which is what those files mean.

- `Download::engine` and `DownloadStatus::engine` are `odl::engine::Engine`,
  not the generated proto type. `Engine` is `non_exhaustive`, so a future
  engine will not be another breaking change.
- `OdlError` gained variants and is `non_exhaustive`.
- `Phase` gained `PostProcessing`; `ProgressEvent`, `Quality`,
  `EvaluateRequest` and `DownloadRequest` are now `non_exhaustive`. Build the
  request types with `new` and the chainable setters.
- `DownloadStatus` gained `engine`, `size_is_approx` and `quality`.
- `quick_evaluate` returns an instruction marked `Engine::Unresolved`, which
  `download` refuses. It never could produce a correct media download; now it
  says so instead of quietly fetching the web page.
- `Download::from_response_info` takes `&DownloadOptions` in place of
  `max_connections`, `use_server_time`, `proxy` and `headers`. Those all came
  off the same options value at every call site, and a positional run of
  same-typed arguments is a swap waiting to happen.
- `fs_utils::cleanup_filename` takes a second argument selecting
  transliteration. Pass `false` for the previous behaviour.
- `YtdlpSpec` gained `video_id` and `ascii_filenames`.
- `retry_policies::wait_for_retry` takes the retrying part's id and an
  optional server-supplied delay. Pass `None, None` for the previous
  behaviour.
- `ProgressEvent` gained `RetryScheduled`. It is `non_exhaustive`, so a
  reporter with a wildcard arm needs no change.
- Minimum supported Rust version is declared: 1.88.
