extern crate prost_build;

fn main() {
    prost_build::compile_protos(&["src/proto/download_metadata.proto"], &["src/"]).unwrap();
}
