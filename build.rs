extern crate protoc_rust;

fn main() {
    protoc_rust::Codegen::new()
        .out_dir("src")
        .inputs(&["src/graphsync_pb.proto"])
        .include("src")
        .run()
        .expect("Running protoc failed.");
}
