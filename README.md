# ODL

[![Crates.io](https://img.shields.io/crates/v/odl.svg)](https://crates.io/crates/odl)
[![Docs.rs](https://docs.rs/odl/badge.svg)](https://docs.rs/odl)

Flexible download library and CLI intended to be fast, reliable, and easy to use.

## Quick Start

### Install

**Linux / macOS** (installs to `~/.local/bin` by default):

```bash
curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/install.sh | sh
```

Custom directory:

```bash
curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/install.sh | sh -s -- --dir /usr/local/bin
```

**Windows** (PowerShell, installs to `%LOCALAPPDATA%\Programs\odl` and adds it to user PATH):

```powershell
irm https://raw.githubusercontent.com/jd1378/odl/main/tools/install.ps1 | iex
```

**From source** (any platform with Rust toolchain):

```bash
cargo install odl
```

### Uninstall

**Linux / macOS:**

```bash
curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/uninstall.sh | sh
# also drop user config
curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/uninstall.sh | sh -s -- --purge
```

**Windows** (PowerShell):

```powershell
irm https://raw.githubusercontent.com/jd1378/odl/main/tools/uninstall.ps1 | iex
```

### Use

```bash
odl https://example.com/file.zip
```

## Features

| Feature | Description |
| --- | --- |
| ⚡ Multi-part downloads | Configurable parallel connections for faster downloads |
| 🔄 Automatic resume support | Seamlessly continue interrupted downloads (if server supports range requests) |
| 📝 Conflict resolution | Handles file changes and existing files intelligently (configurable) |
| 🛡️ Crash resilient | Minimizes data loss during unexpected interruptions |
| 🌐 Custom HTTP headers & proxy support | Flexible networking options for advanced use cases |
| 🔁 Retry logic | Automatic retries with configurable backoff on failures |
| 🕒 Preserve modification times (optional) | Optionally keeps server file modification timestamps |
| 🏷️ Server-sent file names | Uses server-provided file names when available; otherwise falls back to the URL's last segment |
| 🎬 Media sites via yt-dlp | Delegates known media hosts to an installed `yt-dlp`, with quality selection and resumable downloads |

This project provides both a command-line program (`odl`) and a Rust library (`odl` crate). Use the CLI for quick downloads and scripting; use the library when you need programmatic control inside an application.

## CLI Usage

- **Download a single remote file (URL)**

```bash
# Download a single URL and use the server-provided filename
odl https://example.com/file.zip

# Specify output file path
odl https://example.com/file.zip -o /path/to/save/file.zip
```

- **Download from a remote list (URL pointing to a newline-separated list of URLs)**

```bash
# Treat the input as a remote list of URLs and save downloaded files into a directory
odl --remote-list https://example.com/list.txt -o /downloads
```

- **Download from a local file containing URLs**

```bash
# Input file contains one URL per line; output is a directory
odl /path/to/urls.txt -o /downloads
```

- **Temporary (one-off) configuration via CLI flags**

```bash
# Limit max connections for this single run
odl --max-connections 4 https://example.com/file.zip

# Temporary speed limit (per run). Accepts either a raw byte count or a human‑readable value with units; input is case‑insensitive.
# All different representations work the same: KiB, K, KB
odl --speed-limit 100K https://example.com/file.zip
```

- **Persistent configuration (save changes to config file)**

The CLI provides a `config` subcommand that updates the persistent configuration (default config path is `odl/config.toml` inside the user's appdata directory). Changes made with `odl config` are saved and used by subsequent runs.

```bash
# Show current configuration and its location
odl config --show

# Set persistent max connections
odl config --max-connections 8

# Use a specific config file and change a value there
odl config --config-file ~/.config/odl/config.toml --max-connections 6

# Then you can use it for a new download:
odl --config-file ~/.config/odl/config.toml https://example.com/file.zip
```

Note: Flags passed directly to `odl` (for example `--max-connections`, `--speed-limit`, `--user-agent`, etc.) apply only to that invocation and override persistent configuration for that run.

## Media sites (yt-dlp)

Links to major media hosts — YouTube, Vimeo, Twitch, Bilibili, X/Twitter,
SoundCloud and a handful of others — are handed to
[`yt-dlp`](https://github.com/yt-dlp/yt-dlp) when it is installed. Everything
else downloads over odl's own multipart HTTP engine exactly as before.

`yt-dlp` is **not bundled**. It is looked up on `PATH` at runtime, and if it is
missing the link is simply downloaded over HTTP instead. Installing it is
enough to enable this; nothing needs to be configured.

If you paste a media link and the helpers are missing, odl offers to fetch them
for you — asking about each separately, saying where they come from, and
pointing out that installing them yourself works just as well. You can also do
it explicitly:

```bash
odl tools status          # what is installed, and where
odl tools install         # offers yt-dlp, then ffmpeg
odl tools install ffmpeg -y
```

Downloads come from the official [yt-dlp releases](https://github.com/yt-dlp/yt-dlp/releases)
and the [ffmpeg builds the yt-dlp project maintains](https://github.com/yt-dlp/FFmpeg-Builds),
land in odl's data directory, and are recorded in your config. odl fetches them
with its own downloader — so an install survives a dropped connection and picks
up where it left off — and verifies each against the SHA-256 published
alongside it. An install that cannot be verified is refused rather than
completed with a warning. Note this protects
against a corrupted or tampered *transfer*, not against a compromised upstream
repository — the checksums come from the same release as the files.

Versions are never pinned: extractors break as sites change, so odl always
takes the current release. On macOS, ffmpeg is not offered automatically —
there is no build with a checksum odl can vouch for — so use
`brew install ffmpeg`.

```bash
# Delegated automatically: odl asks which quality you want, then downloads
odl "https://www.youtube.com/watch?v=…"

# Take the best available without asking (the default when not on a terminal)
odl --choose-format never "https://www.youtube.com/watch?v=…"

# Change your mind about quality: both discard what was downloaded and restart,
# because two encodings cannot be joined into one file
odl --choose-format always --on-file-changed restart "https://www.youtube.com/watch?v=…"
odl --format-id 137+251     --on-file-changed restart "https://www.youtube.com/watch?v=…"

# Force an engine instead of letting the host decide
odl --engine http  https://example.com/file.zip
odl --engine ytdlp "https://some-other-site.example/video/1"
```

The menu shows the container each choice produces (`mp4`, `mkv`, `m4a`, …) and,
when the site publishes them, transcript tracks — subtitles download as a
`.srt`/`.vtt` file instead of the media. Author-supplied tracks are listed
before machine-generated captions, which are capped at a few languages to keep
the menu readable; any other language is reachable with
`--format-id subs:<lang>` or `--format-id autosubs:<lang>`.

Install `ffmpeg` alongside it for the best results: without a muxer only
formats that come as a single file can be downloaded, which caps quality on
sites that serve video and audio separately. The quality menu still lists the
qualities you are missing, marked `— needs ffmpeg`, rather than quietly
stopping short and looking like all the site offers.

### What differs from a plain HTTP download

The transfer belongs to `yt-dlp`, so some things odl normally reports do not
exist:

- No multi-part downloading — `max_connections` only maps to fragment
  parallelism, and only for fragmented formats.
- No server checksums, ETag, or `last-modified`; `odl status` and `probe` omit
  those fields rather than showing them empty.
- Sizes are estimates for adaptive formats until the download finishes, shown
  with a leading `~`.

`max_retries` and `wait_between_retries` do apply, split by which layer can act
on the failure most cheaply:

- **Transfer errors** stay with yt-dlp (`--retries`, `--fragment-retries`, set
  from your configured number). A retry there re-uses the media URL it already
  holds — no second process, no extra call on the site's metadata API.
- **Extraction errors** are odl's. yt-dlp is told not to retry them, so each
  failure is counted against your policy, reported as progress, and can be
  interrupted — none of which is true of a retry hidden inside the process.
- **A finished-but-failed run** is restarted once. That costs a fresh
  extraction (a few seconds), and earns it only for what an internal retry
  cannot fix: a media URL that expired mid-download, or the tool dying
  outright. Scaling it with `max_retries` would multiply against the retries
  yt-dlp is already doing.

Settled failures — an unsupported URL, a format the site no longer offers, a
rate-limited refusal — are not retried at all, since the answer would not
change. `--max-retries 0` means exactly one attempt everywhere.

Resuming works: the page URL is stored rather than the (short-lived) media URL,
so a resume re-resolves it, and the chosen format is pinned so a partial file is
never continued in a different encoding. An ordinary re-run therefore keeps the
original quality; changing quality is an explicit act that starts the download
over.

Playlists are not supported yet — a playlist URL is refused with a clear
message rather than partially handled.

### Configuration

```toml
[ytdlp]
enabled = true              # master switch
binary_path = "/usr/bin/yt-dlp"   # default: found on PATH
ffmpeg_path = "/usr/bin/ffmpeg"   # default: found on PATH
format = "bv*+ba/b"         # default: chosen from ffmpeg availability
extra_hosts = ["some.video.site"] # delegate these too
excluded_hosts = ["reddit.com"]   # never delegate these
extra_args = ["--retries", "5"]   # appended to every yt-dlp invocation
cookies_from_browser = "firefox"  # off by default; reads your cookie store
offer_ytdlp_install = true        # set to false by declining the offer
offer_ffmpeg_install = true       # ditto; `odl tools install` ignores both
```

Declining an install offer is remembered — odl will not ask again for that
tool. Running `odl tools install <tool>` explicitly overrides the decline.

`extra_args` and `cookies_from_browser` are powerful enough to run arbitrary
commands or expose browser credentials, so they are settable from the config
file only — never from the command line.

## Machine interface (`--format json`)

For scripts and AI agents, pass `--format json` to get machine-readable
output instead of human progress bars. This is a documented, stable
contract — the full specification is printed by `odl --help`.

- **Downloads** stream newline-delimited JSON (NDJSON) on stdout, one
  object per line, each tagged with `type` and `url`: `phase`,
  `filename`, `progress`, `message`, `completed`, `failed`, `cancelled`.
- **`probe`, `status`/`list`, `config --show`** emit a single JSON
  document on stdout.
- **Errors** print one JSON object on stderr:
  `{"type":"error","kind":...,"message":...,"exit_code":N}`.

Exit codes: `0` success, `2` usage/bad input, `3` network, `4` conflict,
`5` I/O, `6` metadata, `7` yt-dlp, `130` cancelled, `1` other.

```bash
# Probe a URL without downloading (size, filename, resumability)
odl --format json probe https://example.com/file.zip

# List tracked downloads
odl --format json status
```

### Agent skill

This repo ships an [Agent Skill](plugins/odl/skills/odl/)
(the open `SKILL.md` standard) that teaches SKILL.md-compatible AI agents
to drive `odl` correctly.

One-liner (no checkout needed; downloads the skill from this repo). Run in
a terminal it **prompts** for the agent and scope; pass them as arguments
to skip the prompts. When non-interactive (CI, no terminal) it defaults to
**Claude Code, global**.

```bash
# Interactive: asks which agent (claude/codex/…) and global vs. project
curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/install-skill.sh | sh

# Non-interactive: name agent/scope after `--` to skip the prompts
curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/install-skill.sh | sh -s -- codex --project
curl -fsSL https://raw.githubusercontent.com/jd1378/odl/main/tools/install-skill.sh | sh -s -- cursor --dir ~/.cursor/skills
```

For other agents, scopes, `--dir`, or `agents-md` output, see
`tools/install-skill.sh --help` (or pass `--help` after `--` in the
one-liner).

Once installed, the agent activates the skill automatically when you ask
it to download, fetch, resume, or probe something — no extra command. It
then drives `odl` in `--format json` mode and reads the results.

**Claude Code plugin (marketplace):** this repo is also a plugin
marketplace, so Claude Code users can install and auto-update the skill
with:

```text
/plugin marketplace add jd1378/odl
/plugin install odl@jd1378
```

The canonical skill lives at `plugins/odl/skills/odl/`;
`.claude/skills/odl` is a symlink to it for in-repo dogfooding.

## Checksums

The hashing used to verify downloads is part of the public API, so a program
that already depends on odl need not pull in a second crate to check a file:

```rust
use odl::hash::{HashAlgorithm, HashDigest, HashEncoding};

// Hash a file, reading it in chunks rather than loading it.
let digest = HashDigest::from_path("big.iso", HashAlgorithm::SHA256, HashEncoding::Hex).await?;
println!("{}", digest.digest());

// Or check one against a value you were given, in whatever form it came.
let expected = HashDigest::parse_cli("sha256:b94d27b9…")?;   // also `sha256:base64:uU0nuZ…`
if !HashDigest::verify_file("big.iso", &expected).await? {
    eprintln!("that is not the file it claims to be");
}
```

`verify_file` hashes with the expectation's own algorithm, so you only need the
value you were handed. Compare digests with `matches` rather than `==`: the
same hash written as hex and as base64 is equal under the former and not the
latter, and servers and config files disagree about encoding often enough for
that to matter.

## Library Usage

```no_run
use odl::config::Config;
use odl::download_manager::{DownloadManager, DownloadRequest, EvaluateRequest};
use reqwest::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Create manager with default config
  let cfg = Config::default();
  let manager = DownloadManager::new(cfg);

  // Implement or reuse a SaveConflictResolver and ServerConflictResolver
  // (omitted for brevity). Then evaluate and download:
  // let instruction = manager
  //     .evaluate(EvaluateRequest::new(url, save_dir, &save_resolver))
  //     .await?;
  // let path = manager
  //     .download(DownloadRequest::new(instruction, &server_resolver))
  //     .await?;
  //
  // Per-job override (one download with different settings):
  // let opts = odl::config::DownloadOptionsBuilder::default()
  //     .max_connections(8)
  //     .speed_limit(Some(1_000_000))
  //     .build()?;
  // let instruction = manager
  //     .evaluate(EvaluateRequest::new(url, save_dir, &save_resolver).options(&opts))
  //     .await?;
  //
  // Fields are private; read via getters:
  // println!("download dir: {}", manager.config().download_dir().display());
  Ok(())
}
```

## Roadmap

- Open source multi-platform desktop application based on ODL

## Credits

Inspired by:

- [dlm](https://github.com/agourlay/dlm)
- [trauma](https://github.com/rgreinho/trauma)
- [AB Download Manager](https://github.com/amir1376/ab-download-manager)

## Contribution

Any contribution intentionally submitted for inclusion in the work by you, shall be
licensed as MIT as in the [LICENSE](./LICENSE) file, without any additional terms or conditions.
