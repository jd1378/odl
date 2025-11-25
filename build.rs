extern crate prost_build;

#[cfg(feature = "vendored-protoc")]
extern crate protobuf_src;

fn main() {
    #[cfg(feature = "vendored-protoc")]
    {
        let vendored_protoc = protobuf_src::protoc();
        // SAFETY:
        // - This build script runs in its own process and does not spawn
        //   threads before this point, so setting the process environment
        //   is safe on Unix.
        unsafe {
            std::env::set_var("PROTOC", vendored_protoc);
        }
    }

    prost_build::compile_protos(&["src/proto/download_metadata.proto"], &["src/"]).unwrap();
}
