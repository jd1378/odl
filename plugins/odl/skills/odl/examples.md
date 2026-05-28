# odl recipes

All examples assume `--format json`. Parse stdout line-by-line as JSON.

## 1. Single download, confirm result

```bash
odl --format json "https://example.com/file.zip" -o /downloads/
echo "exit=$?"
```
Watch for the terminal line:
```json
{"type":"completed","url":"https://example.com/file.zip","path":"/downloads/file.zip","already_complete":false}
```
Authority is the exit code (0 = ok). On `failed`/nonzero, read the stderr
`error` object's `kind` to decide retry (see reference.md).

## 2. Idempotent batch (don't clobber, fail loud on conflict)

```bash
# urls.txt: one URL per line
odl --format json urls.txt -o /downloads/ \
    --download-dir "$HOME/.cache/odl" \
    --on-final-file-exists abort
```
- `--download-dir` is fixed, so a re-run skips already-finished files
  (`"already_complete": true`) without re-downloading.
- `--on-final-file-exists abort` means a present-but-untracked file fails
  that URL (exit 4) instead of being overwritten.

To overwrite intentionally, drop the flag (default `replace-and-continue`).
To keep both copies, use `add-number-to-name-and-continue`.

## 3. Resume an interrupted download

Resume is automatic — re-run the **same command with the same
`--download-dir`**. odl reloads metadata and downloads only missing parts.

```bash
odl --format json "https://example.com/big.iso" -o /downloads/ \
    --download-dir "$HOME/.cache/odl"
```
Requires a resumable server (range support). If the server changed the
file, default `--on-file-changed restart` re-downloads; pass `abort` to
stop instead.

## 4. Probe before committing

```bash
odl --format json probe "https://example.com/file.zip"
```
Use `size`, `resumable`, and `requires_auth` to plan (disk space,
connection count, whether credentials are needed) before downloading.

## 5. Authenticated download (secrets from env)

```bash
odl --format json "https://api.example.com/artifact" -o /downloads/ \
    --header "Authorization: Bearer ${API_TOKEN}"

# or HTTP basic auth
odl --format json "https://example.com/file" -o /downloads/ \
    --http-user "$USER_NAME" --http-password "$USER_PASS"
```
Never inline the literal token; read it from the environment.

## 6. Throttle + tune concurrency

```bash
odl --format json urls.txt -o /downloads/ \
    --max-concurrent-downloads 2 --max-connections 4 --speed-limit 5MiB
```

## 7. Check what's tracked

```bash
odl --format json status                 # all tracked downloads
odl --format json status big.iso         # filter by url/filename substring
odl --format json list                   # brief
```

## 8. Verify a download against a known checksum

```bash
odl --format json "https://example.com/file.zip" -o /downloads/ \
    --checksum "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
```
The file is hashed after assembly (`phase: verifying`). A mismatch ends
that URL with `failed` and exits **4** (conflict). Digest is hex by
default; for base64 use `sha256:base64:<digest>`. `--checksum` is
repeatable and runs in addition to any checksum the server advertised.

## 9. Bash error-handling loop (retry transient failures)

```bash
url="https://example.com/file.zip"
for attempt in 1 2 3; do
  if odl --format json "$url" -o /downloads/ --download-dir "$HOME/.cache/odl"; then
    echo "ok"; break
  fi
  code=$?
  case $code in
    3) echo "network error, retrying ($attempt)"; sleep $((attempt*2));;   # transient
    2|4|5) echo "non-retryable (exit $code)"; break;;                       # fix input/conflict/io
    *) echo "exit $code"; break;;
  esac
done
```
Because `--download-dir` is stable, each retry resumes rather than
restarting.
