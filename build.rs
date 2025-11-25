extern crate prost_build;

use std::{path::PathBuf, process::Command};

fn main() {
    // If the `PROTOC` env var is not set and the `protoc` command cannot
    // be executed/found, fall back to the bundled protoc provided by
    // `protobuf-src` and export its path via `PROTOC` as required by
    // `prost-build`.
    if std::env::var_os("PROTOC").is_none() {
        // Try `protoc` first. On Windows, `protoc` may need the explicit
        // `protoc.exe` suffix, so try that if the first attempt errors.
        let protoc_available = match Command::new("protoc").arg("--version").status() {
            Ok(s) => s.success(),
            Err(_) if cfg!(windows) => Command::new(PathBuf::from("protoc"))
                .arg("--version")
                .status()
                .map(|s| s.success())
                .unwrap_or(false),
            Err(_) => false,
        };

        if !protoc_available {
            // SAFETY:
            // - This build script runs in its own process and does not spawn
            //   threads before this point, so setting the process environment
            //   is safe on Unix.
            let vendored_protoc = protobuf_src::protoc();
            unsafe {
                std::env::set_var("PROTOC", vendored_protoc);
            }
        }
    }

    prost_build::compile_protos(&["src/proto/download_metadata.proto"], &["src/"]).unwrap();
}
