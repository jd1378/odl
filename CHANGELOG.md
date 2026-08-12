# Changelog

## 3.1.0

### A direct-connection knob: `no_proxy`

`DownloadOptions::proxy` could name a proxy but never refuse one. With nothing
set, reqwest still picks up `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and the
platform's system proxy on its own, so an embedder that must reach a host
directly — a link-local address, a service inside the same network — had no way
to say so.

`DownloadOptions::no_proxy` (config key `no_proxy`, CLI `--no-proxy`) turns
every proxy off for the job: the configured one, the environment's, and the
system's. It wins over `proxy`, and the pair is collapsed when the options are
built, so `proxy()` reads back `None` rather than a value nothing honours. It
reaches the delegated `yt-dlp` engine too, as `--proxy ""` — that tool reads
the environment itself, so a direct connection has to be stated on its command
line rather than left unsaid.

## 3.0.0

### Optional accessors hand back a reference to the value, not to the `Option`

`Download::etag`, `Download::credentials` and `Download::headers` returned
`&Option<T>`, which every caller then had to unwrap through `.as_ref()` or a
deref before it composed with anything. They return `Option<&T>` now —
`Option<&str>` for the etag — matching the accessors elsewhere on the type and
on `DownloadOptions`.

### `Download` no longer carries a proxy it never used

`Download` and `YtdlpSpec` each held an `Option<reqwest::Proxy>`. Nothing read
either one: the proxy that reaches the wire is built from `DownloadOptions` at
request time, and a `reqwest::Proxy` is an opaque client object that cannot be
serialized, so the field was not persisted with the rest of the instruction
either. What it did do was put reqwest in odl's public API. Both fields, and
`Download::proxy`, are gone; `DownloadOptions::proxy` is where the setting
lives and always was.

### The metadata lock uses the standard library

File locking has been in `std` since Rust 1.89, so the `fs2` dependency —
unmaintained since 2018 — bought nothing. The calls underneath are the same
ones fs2 made: `flock` with `LOCK_EX | LOCK_NB` on Unix, `LockFileEx` with
`LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY` on Windows, released on
`unlock` or when the file closes. Contention is still reported as
`MetadataError::LockfileInUse`.

The minimum supported Rust version is now 1.89.

### reqwest is no longer part of odl's public API

Which HTTP client odl uses was visible from the outside: `self_update::plan`
and `ytdlp::install::plan` both took a `reqwest::Client`, `OdlError`
implemented `From<reqwest::Error>`, `DownloadOptions` converted into a
`reqwest::Proxy`, and `Url` and `HeaderMap` were named through reqwest's
re-exports. That made reqwest a *public* dependency: a consumer had to resolve
the same major version of it as odl did, and odl could not upgrade its client
without breaking them.

Both `plan` functions now take the `&DownloadOptions` they were only ever
using to build that client, so they configure proxy, certificate policy and
connect timeout exactly as before, with one fewer thing for the caller to
assemble. `Url` and `HeaderMap` are unchanged types, now imported from the
`url` and `http` crates that define them — both are declared dependencies, so
a consumer can match versions with odl directly. The reqwest error conversion
and the proxy conversion are internal.

Callers of `self_update::plan(&client, current)` pass
`&config.download()` instead of a client; callers of
`ytdlp::install::plan(&client, tool)` likewise. Anyone who relied on `?`
converting a `reqwest::Error` into an `OdlError` was reaching through odl to
its client, and now needs their own mapping.

### TLS is served by rustls, not a vendored OpenSSL

reqwest 0.13 makes rustls its default backend, and its `rustls` feature now
pulls in `rustls-platform-verifier` on its own — so verification against the
platform's own trust store, which was the reason to prefer native-tls in the
first place, comes for free.

What native-tls still cost was the vendored build. `native-tls-vendored`
compiled OpenSSL from source on every build, which meant a perl and C
toolchain on every build host, and it froze a copy of OpenSSL into the shipped
binary that only saw CVE fixes when someone remembered to bump `openssl-src`.
Both are gone; a Rust toolchain is again all that building odl needs.

rustls declines TLS 1.0/1.1 and the legacy cipher suites OpenSSL still
accepts, so a server old enough to need one of them now fails the handshake
rather than negotiating down to it. Trust anchors come from the platform store
rather than the vendored bundle, so OpenSSL's `SSL_CERT_FILE` and its
neighbours no longer have any bearing on which roots odl trusts.

## 2.3.1

### A resumed download whose parts are all present no longer fails

The ramped fill opens a batch of connections and waits for each to report a
first chunk. When every part in the batch finished before the task awaiting
those reports was polled, the scheduler read the empty task set as a reason to
stop and returned with the rest of the queue unopened — so a download ended
claiming parts were left over, with every byte of it already on disk. Parts
already complete on disk lose that race most often, since they finish without
transferring anything, which makes a resume the way to meet it. An empty task
set now ends the batch rather than the fill.

## 2.3.0

### Building odl no longer needs `protoc`

`prost-build` shells out to a `protoc` binary, so building odl meant having
one — and where there wasn't one (the `cross` containers), the
`vendored-protoc` feature pulled in `protobuf-src` and compiled protobuf's
C++ from source. A build-environment workaround does not belong in a crate's
public feature list, and every CI job carried a step to install a compiler
that only the build script used.

The schema is now compiled by `protox`, a protobuf compiler written in Rust,
in-process. `prost-build` still generates the code, from the descriptor set
protox hands it: the generated Rust is byte-identical to what protoc produced,
and the descriptors match protoc's own output field for field, so nothing
about the on-disk metadata format changes. `vendored-protoc` and the
`protobuf-src` dependency are gone, along with every protoc install step in
CI. Building odl now needs nothing but a Rust toolchain.

### `PartProgress` cadence is now part of the contract

The built-in downloader has always sampled every in-flight part on its 125 ms
tick whether or not bytes arrived, but the event was documented only as "a part
advanced". A consumer wanting to know which parts are on a connection right now
has nothing else to read — parts leave the wire to be re-scheduled without an
event of their own — so it was depending on the implementation rather than on
anything promised. `ProgressEvent::PartProgress` now states the guarantee: an
in-flight part is sampled at `SAMPLE_INTERVAL` regardless of byte arrival, so
the absence of samples means the part is not being transferred. `PartAdded`
says what it actually announces along with it — that a part exists, which is
not the same as being on a connection, and which can be said more than once for
the same ulid. A test holds a connection open saying nothing and asserts the
samples keep coming.

## 2.2.0

### A failed download is reported once

The download's own line already read `✕ name: why`, and the process then
printed `Error: why` again — through a channel the progress display does not
coordinate with, so it landed on top of a bar that was still drawing, and
before the line it was repeating. Text mode now leaves it to the `✕` line,
which is the one that names *which* download failed. An error that never
reached a download — a bad argument, an unreadable config — still gets the
banner, as does a run whose output is redirected, where the progress display
draws nothing at all. `--format json` is unchanged: the `failed` event and the
error object both carry it, and the object carries the exit code.

`AsyncReporter::drained` is new, and the CLI waits on it before reporting
anything of its own. Events are handed to a worker task, so a download's last
line could otherwise be drawn after the code that summarises it has run — or,
at exit, not at all.

### Verification reports progress

Hashing the finished file ran to completion silently, so the one stage where a
consumer could show nothing was also the one that can take several seconds on a
large file: the bar sat at 100% and hoped. Verification now reports on its own
row through the same `Part*` events assembly uses, under
`odl::progress::VERIFY_ULID`. `HashDigest::from_path_with_progress` and
`from_reader_with_progress` expose the same thing to library callers — a
per-block byte count, with no opinion about how it should be displayed.

### Assembly no longer rewinds the download's progress

Assembly reused the aggregate `Progress` event and counted its copy from zero,
so every consumer that did not special-case the phase showed the download
falling back toward 0% at the finish line. The transfer is complete by then.
The aggregate now belongs to the transfer alone: assembly and verification
report on their own rows, and the downloader emits one final `Progress` at the
full size, because the 8 Hz sampler's last tick almost always lands short of
the end and nothing follows it any more.

If you were compensating for this by ignoring `Progress` during
`Phase::Assembling`, that workaround is now unnecessary but harmless.

## 2.1.0

### `odl update`

odl can now replace itself with the latest GitHub release: `odl update --check`
reports whether one exists, `odl update` installs it after asking, `-y` skips
the question. `--format json` reports the same as a single object.

It only ever replaces an odl that the install script put in place. The scripts
now leave a receipt naming the directory they wrote to, and an install that
predates the receipt still qualifies if it sits where the scripts default to
(`~/.local/bin`, `%LOCALAPPDATA%\Programs\odl`) and the user can write it.
A copy from `cargo install`, Homebrew, Nix or a distribution package is refused
with the command that owns it named — writing over a package manager's file is
how a machine ends up with a manager reporting a version nothing on disk has.

The release workflow now publishes a `.sha256` beside every archive, and the
update verifies against it before overwriting anything; a release without one
is refused rather than installed with a warning. As with the yt-dlp installer,
that covers a corrupted transfer or a tampered mirror, not a compromised
upstream — the sums ship from the same place as the files. The download itself
goes through odl: resumable, retrying, and checksum-verified by the downloader
that already does this for every other file.

### A server that stops honouring `Range` is recoverable

A `200` answering a ranged request means the parts in flight are writing the
whole file at their own offsets, so the run was ended with a `NotResumable`
conflict and no way for the caller to say what to do about it. Recovering
meant reaching around odl: catch the error, wipe the work directory, run again
with one connection. That is now what odl does itself, through the same
`resolve_not_resumable` resolver the pre-download check uses — abort still
aborts (`--on-not-resumable abort`), and restart discards the parts and
re-fetches the file whole, once.

The observation is also recorded: a download that saw the server ignore
`Range` keeps `is_resumable: false` on disk, outranking the `accept-ranges`
the headers keep advertising, so a later resume does not split the file up and
ask for slices again. Such a download is no longer grown to more parts by a
larger `--max-connections` either.

### A download the server won't range is never split

`--dynamic-split` subdivided a part whenever a connection looked idle, without
asking whether the server serves ranges at all. On a non-resumable download
that turned one correct request into several the server answers from byte zero
— the corruption above, provoked by odl rather than by the server changing its
mind. Splitting is now gated on the download being resumable.

### New event: `ProgressEvent::PartsCleared`

A restart deletes parts that were already announced through `PartAdded` and
will never send `PartFinished`, because they did not finish. Consumers holding
per-part state (a row, a bar) get this event and drop all of it; new
`PartAdded` events follow. The enum is `#[non_exhaustive]`, so a consumer that
ignores it keeps compiling — it will just show the stale rows.

### A retrying part says so on its own row

The parent line already counts a backoff down, but with several parts in
flight it does not say which of them is waiting — the part's own row showed
`retry #2` and nothing more. It now reads `retry #2/5 in 20s`, with
`(Rate limited)` appended when the delay is the server's own `Retry-After`,
which no shorter backoff can improve. The countdown keeps the parent line to
itself, including for retries of whole-download steps such as the probe; it
carries the same `(Rate limited)` note, replacing the wordier
"(server asked us to wait)".

### Rampup is settable from the command line

`--rampup`, `--rampup-batch-size`, `--rampup-delay-min` and
`--rampup-delay-max` join `--dynamic-split`, on both the download command and
`odl config`. Turning off staggered connection opening — the first thing to
try against a server that refuses parallel opens — previously meant writing a
config file.

### `ASSEMBLY_ULID` is reachable

The ulid carried by the part events that report final-file assembly lived in a
private module, so a consumer had to hard-code `"_assemble"` to tell assembly
apart from a real part. It is now `odl::progress::ASSEMBLY_ULID`.

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
