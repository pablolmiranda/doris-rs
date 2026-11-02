fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    std::env::set_var("PROTOC", protoc);
    let protos = ["segment_v2", "olap_common", "types", "olap_file"];
    for p in protos {
        println!("cargo:rerun-if-changed=proto/{p}.proto");
    }
    let files: Vec<String> = protos.iter().map(|p| format!("proto/{p}.proto")).collect();
    prost_build::Config::new()
        .compile_protos(&files, &["proto/"])
        .expect("compile Doris protos");
}
