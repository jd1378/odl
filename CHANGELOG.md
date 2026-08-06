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

`base64` and `ulid` were deliberately held at their current majors: neither
upgrade fixes anything odl is affected by, and upgrading `base64` would compile
two copies of it into the binary, since `reqwest` still requires 0.22.

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
- Minimum supported Rust version is declared: 1.88.
