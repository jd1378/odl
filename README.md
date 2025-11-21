# ODL

open source flexible download library and cli intended to be fast, reliable and easy to use.

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
| 🏷️ Server-sent file names         | Uses server-provided file names when available, otherwise falls back to the URL's last segment |

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
