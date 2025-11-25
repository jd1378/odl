# ODL

Flexible download library and CLI intended to be fast, reliable, and easy to use.

## Quick Start

```bash
cargo install odl

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

## Library Usage

```no_run
use odl::config::Config;
use odl::download_manager::DownloadManager;
use reqwest::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Create manager with default config
  let cfg = Config::default();
  let manager = DownloadManager::new(cfg);

  // Implement or reuse a SaveConflictResolver and ServerConflictResolver
  // (omitted for brevity). Then evaluate and download:
  // let instruction = manager.evaluate(url, save_dir, None, &resolver).await?;
  // let path = manager.download(instruction, &server_resolver).await?;
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
