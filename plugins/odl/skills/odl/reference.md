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
| 2 | `cli`, `empty_input_file`, `url_decode`, `config` | usage or invalid input |
| 3 | `network` | DNS, timeout, HTTP status, connection |
| 4 | `conflict` | save/server conflict, checksum mismatch |
| 5 | `io` | I/O error |
| 6 | `metadata` | lockfile in use, decode failure |
| 130 | `cancelled` | cancelled |

## NDJSON stream (downloads → stdout)

One JSON object per line. Every object has `"type"` and `"url"`.

| `type` | fields | notes |
|--------|--------|-------|
| `phase` | `phase` | one of `evaluating`, `resolving_conflicts`, `downloading`, `assembling`, `flushing`, `verifying` |
| `filename` | `filename` | resolved final filename |
| `progress` | `downloaded`, `total` | `total` is `null` when the server sent no Content-Length; ~8 events/sec |
| `message` | `message` | free-form status (e.g. retry countdown) |
| `completed` | `path`, `already_complete` | terminal; `already_complete: true` ⇒ nothing was downloaded |
| `failed` | `message` | terminal; this URL failed |
| `cancelled` | — | terminal; cancelled |

Speed samples and per-part events are intentionally **not** emitted in
JSON mode — derive throughput from successive `progress` events.

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
  "resumable": true,
  "etag": "\"abc\"",             // or null
  "last_modified": 1700000000,   // unix seconds, or null
  "last_modified_rfc3339": "2023-11-14T22:13:20+00:00",  // or null
  "requires_auth": false,
  "requires_basic_auth": false,
  "checksums": [ { "algorithm": "sha256", "digest": "…", "encoding": "hex" } ]
}
```

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
      "downloaded": 12345,       // bytes present across parts on disk
      "percent": 100.0,          // null when size unknown and not finished
      "finished": true,
      "resumable": true,
      "parts_total": 4,
      "parts_finished": 4
    }
  ]
}
```
`list` returns the same JSON as `status`; they differ only in text mode.
Both accept an optional `FILTER` substring matched against url/filename.

### `config --show` / config write
```json
{ "type": "config", "path": "/…/config.toml", "config": { /* full config */ } }
{ "type": "config_saved", "path": "/…/config.toml" }
```

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
| `--header "K: V"` | repeatable; use for auth tokens |
| `--http-user`, `--http-password` | HTTP basic auth |
| `--proxy URL` | `http(s)://` or `socks://` |
| `--on-final-file-exists abort\|replace-and-continue\|add-number-to-name-and-continue` | default `replace-and-continue` (overwrites) |
| `--on-same-download-exists abort\|resume\|add-number-to-name-and-continue` | default `resume` |
| `--on-file-changed abort\|restart`, `--on-not-resumable abort\|restart` | default `restart` |
| `--remote-list` | treat INPUT URL as a downloadable list of URLs |
| `--accept-invalid-certs` | TLS bypass — avoid unless you must |
| `--log-level off\|error\|warn\|info\|debug\|trace` | diagnostics to stderr; `RUST_LOG` overrides |
