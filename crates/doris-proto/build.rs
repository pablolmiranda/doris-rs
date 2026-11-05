fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    std::env::set_var("PROTOC", protoc);
    let protos = [
        "segment_v2",
        "olap_common",
        "types",
        "olap_file",
        "internal_service",
        "data",
        "descriptors",
        "runtime_profile",
    ];
    for p in protos {
        println!("cargo:rerun-if-changed=proto/{p}.proto");
    }
    let files: Vec<String> = protos.iter().map(|p| format!("proto/{p}.proto")).collect();
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    let descriptors = out.join("descriptors.bin");
    prost_build::Config::new()
        .file_descriptor_set_path(&descriptors)
        .compile_protos(&files, &["proto/"])
        .expect("compile Doris protos");
    if std::env::var_os("CARGO_FEATURE_JSON").is_some() {
        let set = std::fs::read(&descriptors).expect("descriptor set");
        pbjson_build::Builder::new()
            .register_descriptors(&set)
            .expect("register descriptors")
            .preserve_proto_field_names()
            .ignore_unknown_fields()
            .build(&[".doris"])
            .expect("pbjson");
    }
}
