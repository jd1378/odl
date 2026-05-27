---
name: odl
description: Download files reliably with the `odl` CLI — resumable, multi-connection, batch URL lists, with machine-readable NDJSON output and typed exit codes. Use when asked to download a file or URL, fetch/grab a list of URLs, resume an interrupted download, probe a URL's size/filename/resumability, or check download status. Trigger on "download", "fetch this URL", "grab this file", "download these links", "resume the download", "is this URL resumable".
---

# odl — agent download interface

`odl` is a resumable, multi-connection download manager. Drive it in
machine mode and never parse human output.

Requires `odl` ≥ 1.0 (the `--format json` contract). Verify with
`odl --version`; the full contract is in `odl --help`.

## Golden rule

**Always pass `--format json`.** Without it you get ANSI progress bars
that are unreadable and waste tokens. With it you get newline-delimited
JSON (NDJSON) on stdout and a JSON error object on stderr.

## Decide success two ways, together

1. **Exit code** is the authority for the whole invocation.
2. **Per-URL terminal events** (`completed` / `failed` / `cancelled`)
   tell you which file in a batch ended how.

Read stdout line by line, parse each line as one JSON object, branch on
its `"type"`, and attribute it to its `"url"`.

## Core commands

```bash
# Single download (server-provided filename), machine output
odl --format json "https://example.com/file.zip" -o /downloads/

# Batch from a file of URLs (one per line; '#'/'//' comments ignored)
odl --format json /path/to/urls.txt -o /downloads/

# Probe without downloading: size, filename, resumability, etag
odl --format json probe "https://example.com/file.zip"

# What is already downloaded / in progress?
odl --format json status            # detailed
odl --format json list              # brief; both accept an optional FILTER

# Show effective config
odl --format json config --show
```

## Exit codes (branch on these)

| code | meaning | retry? |
|------|---------|--------|
| 0 | success | — |
| 2 | usage / bad input | **no** — fix the command |
| 3 | network | **yes** — transient |
| 4 | conflict (file exists / changed / checksum) | no — change `--on-*` flags |
| 5 | I/O | no — check disk/permissions |
| 6 | metadata (lockfile held / corrupt) | maybe — another odl may be running |
| 130 | cancelled | — |
| 1 | other/internal | maybe |

## Load-bearing gotchas

- **Idempotent batches: control clobbering.** `--on-final-file-exists`
  defaults to `replace-and-continue` (overwrites). To avoid re-downloading
  or overwriting, pass `--on-final-file-exists abort` (fail if present) or
  `add-number-to-name-and-continue` (keep both).
- **Auto-skip of finished files** works only when `--download-dir` is the
  **same across runs** and the final file is still present — then odl
  emits `completed` with `"already_complete": true` and downloads nothing.
  Metadata lives under `--download-dir` (default: OS data dir), *not* next
  to the output. Change the dir and odl can't tell it already ran.
- **A HEAD request is still sent per URL even for finished files.** On
  rate-limited hosts, large mostly-done batches cost N HEADs.
- **`progress` may show `"downloaded":0,"total":null`** for chunked
  responses with no Content-Length. That is normal, not a stall — wait for
  `completed`.
- **Secrets**: pass auth via `--header "Authorization: Bearer $TOKEN"` or
  `--http-user/--http-password`, reading values from the environment.
  Never hardcode tokens into commands you save.

## More detail

- `reference.md` — full JSON event schema, every field, all relevant
  flags, probe/status/config document shapes, the error object.
- `examples.md` — copy-paste recipes: parse a download, idempotent batch,
  resume, probe-then-download, auth, a bash error-handling loop.
