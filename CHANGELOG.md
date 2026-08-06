# Changelog

## 2.0.0

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
Anything else is a conflict rather than data. The one exception is the case
that has to keep working: a single connection, nothing downloaded yet, where
`200` returns exactly the bytes requested.

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
- Minimum supported Rust version is declared: 1.88.
