extern crate protoc_rust;

fn main() {
    protoc_rust::Codegen::new()
        .out_dir("src")
        .inputs(&["src/graphsync_pb.proto"])
        .include("src")
        .customize(protoc_rust::Customize {
            generate_accessors: Some(false),
            lite_runtime: Some(true),
            ..Default::default()
        })
        .run()
        .expect("Running protoc failed.");
}
