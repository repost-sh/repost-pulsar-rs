extern crate prost_build;

fn main() {
    #[cfg(feature = "protobuf-src")]
    std::env::set_var("PROTOC", protobuf_src::protoc());

    prost_build::Config::new()
        // They carry the credential: src/message.rs writes their Debug.
        .skip_debug([".pulsar.proto.CommandConnect", ".pulsar.proto.AuthData"])
        .compile_protos(&["./PulsarApi.proto"], &["./"])
        .unwrap();
}
