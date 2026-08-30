const PROTO: &str = "src/proto/download_metadata.proto";

fn main() {
    // The target triple names the release asset `odl update` must fetch.
    // `std::env::consts` cannot tell gnu from musl, and installing the wrong
    // one produces a binary that will not start.
    println!(
        "cargo:rustc-env=ODL_BUILD_TARGET={}",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string())
    );

    // `protox` parses the schema in Rust, so building odl needs no `protoc`
    // on the machine and no C++ toolchain to vendor one. `prost-build` is
    // still what generates the code — it is handed a descriptor set rather
    // than left to shell out for one.
    println!("cargo:rerun-if-changed={PROTO}");
    let descriptors = protox::compile([PROTO], ["src/"]).unwrap();
    prost_build::Config::new().compile_fds(descriptors).unwrap();

    // Embed application manifest on Windows targets to declare longPathAware,
    // UTF-8 activeCodePage, and supported OS compatibility GUIDs.
    #[cfg(target_os = "windows")]
    {
        println!("cargo:rerun-if-changed=resources/odl.manifest");
        let mut res = winres::WindowsResource::new();
        res.set_manifest_file("resources/odl.manifest");
        res.compile()
            .expect("failed to compile Windows application manifest");
    }
}
