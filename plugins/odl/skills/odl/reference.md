# odl machine-interface reference

Authoritative copy lives in `odl --help` (the `EXIT CODES` and
`JSON OUTPUT` sections). This file mirrors it for offline use. If they
disagree, trust `odl --help` for your installed version.

## Invocation

```
odl [OPTIONS] [INPUT] [COMMAND]
```

- `INPUT` — a URL, or a path to a file with one URL per line (blank lines
  and lines starting with `#` or `//` are ignored).
- `--format json` is global: works on the bare download form and on every
  subcommand. Always set it.

## Exit codes

| code | kind (in error JSON) | meaning |
|------|----------------------|---------|
| 0 | — | success |
| 1 | `other` | other / internal error |
| 2 | `cli`, `empty_input_file`, `url_decode`, `config`, `not_evaluated`, `invalid_request` | usage or invalid input |
| 3 | `network` | DNS, timeout, HTTP status, connection; also a site refusing us as too frequent (HTTP 429) — **retryable** |
| 4 | `conflict` | save/server conflict, checksum mismatch |
| 5 | `io` | I/O error |
| 6 | `metadata` | lockfile in use, decode failure |
| 7 | `ytdlp` | yt-dlp missing, too old, or failed; unsupported URL. **Not** retryable — fix the toolchain |
| 130 | `cancelled` | cancelled |

## NDJSON stream (downloads → stdout)

One JSON object per line. Every object has `"type"` and `"url"`.

| `type` | fields | notes |
|--------|--------|-------|
| `phase` | `phase` | one of `evaluating`, `resolving_conflicts`, `downloading`, `post_processing`, `assembling`, `flushing`, `verifying`; treat an unknown value as informational |
| `filename` | `filename` | resolved final filename |
| `progress` | `downloaded`, `total` | `total` is `null` when the size is unknown; up to ~8 events/sec. With the `ytdlp` engine these are data-driven, so a stalled transfer emits nothing at all — use your own clock to detect a stall |
| `message` | `message` | free-form status (e.g. retry countdown) |
| `retry_scheduled` | `part`, `attempt`, `max_attempts`, `delay_ms`, `server_requested` | the transfer is paused and will resume after `delay_ms`; use it to tell a pause from a hang. `part` is a ulid, or `null` for a whole-download step. `server_requested: true` means the delay is the server's `Retry-After` (honoured up to 5 minutes) rather than odl's backoff |
| `completed` | `path`, `already_complete` | terminal; `already_complete: true` ⇒ nothing was downloaded |
| `failed` | `message` | terminal; this URL failed |
| `cancelled` | — | terminal; cancelled |

Speed samples and per-part progress are intentionally **not** emitted in
JSON mode — derive throughput from successive `progress` events.
`retry_scheduled` is the exception: it is rare, and without it a retry wait
is indistinguishable from a stalled download.

In a batch, every URL produces exactly one terminal event
(`completed`/`failed`/`cancelled`). Match by `url`.

## One-shot documents (subcommands → stdout)

A single JSON object (not NDJSON).

### `probe`
```json
{
  "type": "probe",
  "url": "https://…",
  "filename": "file.zip",
  "size": 12345,                 // or null if unknown
  "size_is_approx": false,       // true ⇒ `size` is an estimate
  "engine": "http_multipart",    // or "ytdlp"
  "quality": "1080p60",          // null unless the engine chose a format
  "resumable": true,
  "etag": "\"abc\"",             // or null
  "last_modified": 1700000000,   // unix seconds, or null
  "last_modified_rfc3339": "2023-11-14T22:13:20+00:00",  // or null
  "requires_auth": false,
  "requires_basic_auth": false,
  "checksums": [ { "algorithm": "sha256", "digest": "…", "encoding": "hex" } ]
}
```

Keys are always present so parsing stays uniform. With `"engine": "ytdlp"` the
transfer is performed by yt-dlp, which never exposes the underlying HTTP
exchange: `etag`, `last_modified`, `last_modified_rfc3339` are `null` and
`checksums` is empty. That means "this engine cannot know", not "the server
sent nothing".

### `status` / `list`
```json
{
  "type": "status",
  "count": 2,
  "downloads": [
    {
      "filename": "file.zip",
      "url": "https://…",
      "save_dir": "/downloads",
      "final_file_path": "/downloads/file.zip",
      "final_file_exists": true,
      "download_dir": "/home/u/.local/share/odl/file.zip",
      "size": 12345,             // or null
      "size_is_approx": false,   // true ⇒ `size` is an estimate
      "engine": "http_multipart",// or "ytdlp"
      "quality": "1080p60",      // null unless the engine chose a format
      "downloaded": 12345,       // bytes still in the working dir; 0 once finished
      "percent": 100.0,          // null when size unknown and not finished
      "finished": true,
      "resumable": true,
      "parts_total": 4,          // always 1 for "ytdlp"; not a real part count
      "parts_finished": 4
    }
  ]
}
```
`list` returns the same JSON as `status`; they differ only in text mode.
Both accept an optional `FILTER` substring matched against url/filename.

`downloaded` is the literal byte count in the working directory, so a finished
download reports `0` — its parts were removed after assembly, or its file was
moved to `save_dir`. Use `finished`/`percent`, not `downloaded`, to judge
completion. For `"engine": "ytdlp"`, `parts_*` are a placeholder: that engine
has no part table.

### `config --show` / config write
```json
{ "type": "config", "path": "/…/config.toml", "config": { /* full config */ } }
{ "type": "config_saved", "path": "/…/config.toml" }
```

## Helper programs (`odl tools`)

Media links need `yt-dlp`; higher qualities additionally need `ffmpeg`. Neither
is bundled. **Nothing here ever prompts in `--format json` mode**, so an agent
cannot hang on a question: the install offer is skipped entirely unless stdin
is a terminal *and* output is text.

```bash
odl --format json tools status
```
```json
{
  "type": "tools",
  "config_path": "/…/config.toml",
  "tools_dir": "/…/odl/tools",
  "yt_dlp": "/usr/bin/yt-dlp",
  "ffmpeg": null,
  "can_install_yt_dlp": true,
  "can_install_ffmpeg": true
}
```
`yt_dlp` / `ffmpeg` are `null` when not installed. `can_install_ffmpeg` is
`false` on macOS, where odl has no build with a checksum it can verify.

Install without interaction by passing `-y`:

```bash
odl tools install yt-dlp -y     # exit 0 on success, 7 on failure
odl tools install -y            # both, yt-dlp first
```

Binaries come from the official yt-dlp releases and the ffmpeg builds the
yt-dlp project maintains, land in `tools_dir`, and are recorded in
`config_path`. The latest release is always used.

The asset is fetched by odl's own downloader, so an install resumes after an
interruption and is verified against the SHA-256 published with it — a
mismatch exits `4` (`conflict`) and installs nothing.

Without `-y` **and** without a terminal, `install` prints what it would do,
changes nothing, and exits 0 — re-check `tools status` rather than assuming it
installed. Declining an interactive offer is recorded
(`ytdlp.offer_*_install = false`) and never asked again; an explicit
`odl tools install` overrides that.

## Self-update (`odl update`)

`odl update --check --format json` reports whether a newer release exists and
whether odl is allowed to install it:

```json
{"type":"update","status":"available","current_version":"2.1.0","new_version":"2.1.1",
 "tag":"v2.1.1","asset":"odl-v2.1.1-x86_64-unknown-linux-gnu.tar.gz","size":4194304,
 "path":"/home/u/.local/bin/odl","can_install":true,"blocked_because":null}
```

`status` is `available`, `up_to_date` or `ineligible`. `odl update -y` performs
it and prints `{"type":"update","status":"updated",...}`. Without `--check`, an
install odl may not replace (cargo, Homebrew, Nix, system package) is an error
exit 2 with the reason. The downloaded archive is checked against the SHA-256
published beside it; a release without one is refused.

## Error object (→ stderr)

On any top-level failure, one JSON object on **stderr**:
```json
{ "type": "error", "kind": "network", "message": "…", "exit_code": 3 }
```
In a batch, individual `failed` events also stream to stdout; the stderr
`error` object is the single exit summary (first failure).

## Flags worth knowing (agent-relevant)

| flag | purpose |
|------|---------|
| `-o, --output FILE\|DIR` | file path (single URL) or output dir (list) |
| `-d, --download-dir DIR` | where parts + metadata are staged; **keep stable** to enable resume/skip |
| `--max-connections N` | parallel parts per file |
| `--max-concurrent-downloads N` | parallel files in a batch |
| `--speed-limit BYTES/S` | e.g. `100K`, `1.5MiB` (base 1024) |
| `--max-retries N`, `--n-fixed-retries N`, `--wait-between-retries DUR` | retry policy |
| `--timeout DUR` | connect timeout, e.g. `30s` |
| `--read-timeout DUR` | give up on a request that goes this long without receiving a byte, e.g. `30s`. Default `10s`; bounds silence, not transfer length, so a slow download is never cut off |
| `--header "K: V"` | repeatable; use for auth tokens |
| `--engine auto\|http\|ytdlp` | `auto` (default) delegates known media hosts to yt-dlp when installed; `ytdlp` fails if it is unavailable |
| `--choose-format auto\|always\|never` | quality prompt for delegated downloads. **Agents should pass `never`** — `auto` already declines to prompt without a terminal, but `never` states it |
| `--engine` / `--format-id` note | `odl tools status --format json` reports which helpers are installed and whether odl can fetch them; `odl tools install <tool> -y` installs non-interactively |
| `--format-id ID` | download this exact media format. `subs:<lang>` / `autosubs:<lang>` fetch that transcript instead of the media. Naming a different id than an in-progress download discards it and starts over — quality is pinned so a resume never mixes encodings |
| `--checksum ALGO:DIGEST` | verify the file against a known hash; repeatable. `ALGO`: `md5`/`sha1`/`sha256`/`sha384`/`sha512`. Digest hex by default, or `ALGO:base64:DIGEST`. Mismatch ⇒ exit 4 |
| `--http-user`, `--http-password` | HTTP basic auth |
| `--proxy URL` | `http(s)://` or `socks://` |
| `--no-proxy` | connect directly: ignores the configured proxy and `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY`. Conflicts with `--proxy` |
| `--on-final-file-exists abort\|replace-and-continue\|add-number-to-name-and-continue` | default `replace-and-continue` (overwrites) |
| `--on-same-download-exists abort\|resume\|add-number-to-name-and-continue` | default `resume` |
| `--on-file-changed abort\|restart`, `--on-not-resumable abort\|restart` | default `restart`. `--on-not-resumable` also decides what happens when a server stops honouring `Range` *during* a download: `restart` discards the parts and re-fetches whole on one connection, `abort` exits 4 |
| `--remote-list` | treat INPUT URL as a downloadable list of URLs |
| `--accept-invalid-certs` | TLS bypass — avoid unless you must |
| `--log-level off\|error\|warn\|info\|debug\|trace` | diagnostics to stderr; `RUST_LOG` overrides |
