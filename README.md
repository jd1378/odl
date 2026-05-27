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

| Feature                                           | Description                                                                                   |
|---------------------------------------------------|-----------------------------------------------------------------------------------------------|
| ⚡ Multi-part downloads                            | Configurable parallel connections for faster downloads                                        |
| 🔄 Automatic resume support                       | Seamlessly continue interrupted downloads (if server supports range requests)                  |
| 📝 Conflict resolution                            | Handles file changes and existing files intelligently (configurable)                                         |
| 🛡️ Crash resilient                               | Minimizes data loss during unexpected interruptions                                           |
| 🌐 Custom HTTP headers & proxy support            | Flexible networking options for advanced use cases                                            |
| 🔁 Retry logic                                   | Automatic retries with configurable backoff on failures                                       |
| 🕒 Preserve modification times (optional)         | Optionally keeps server file modification timestamps                                          |
| 🏷️ Server-sent file names         | Uses server-provided file names when available; otherwise falls back to the URL's last segment. |

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
`5` I/O, `6` metadata, `130` cancelled, `1` other.

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

From a checkout you can also run `tools/install-skill.sh` directly:

```bash
# Claude Code — if run inside a project it asks global vs. this project;
# otherwise installs globally (~/.claude/skills)
sh tools/install-skill.sh claude

# Force scope
sh tools/install-skill.sh claude --global     # ~/.claude/skills
sh tools/install-skill.sh codex  --project    # ./.codex/skills

# Any SKILL.md-compatible agent: install into an explicit directory
sh tools/install-skill.sh --dir /path/to/agent/skills

# Non-SKILL.md agents: flatten into AGENTS.md
sh tools/install-skill.sh agents-md ./AGENTS.md
```

Run `sh tools/install-skill.sh --help` for all agents (claude, codex,
copilot, gemini, cursor) and their known skill paths.

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
