extern crate prost_build;

use std::{path::PathBuf, process::Command};

fn main() {
    // If the `PROTOC` env var is not set and the `protoc` command cannot
    // be executed/found, fall back to the bundled protoc provided by
    // `protobuf-src` and export its path via `PROTOC` as required by
    // `prost-build`.
    if std::env::var_os("PROTOC").is_none() {
        let protoc_available = Command::new(PathBuf::from("protoc"))
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        if !protoc_available {
            // SAFETY:
            // - This build script runs in its own process and does not spawn
            //   threads before this point, so setting the process environment
            //   is safe on Unix.
            unsafe {
                std::env::set_var("PROTOC", protobuf_src::protoc());
            }
        }
    }

    prost_build::compile_protos(&["src/proto/download_metadata.proto"], &["src/"]).unwrap();
}
