extern crate prost_build;

use std::process::Command;

use os_info::Type;

fn main() {
    // If the `PROTOC` env var is not set and the `protoc` command cannot
    // be executed/found, fall back to the bundled protoc provided by
    // `protobuf-src` and export its path via `PROTOC` as required by
    // `prost-build`.
    let info = os_info::get();

    if info.os_type() != Type::Windows && std::env::var_os("PROTOC").is_none() {
        let protoc_available = Command::new("protoc")
            .arg("--version")
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

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
