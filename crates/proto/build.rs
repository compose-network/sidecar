//! Build script for generating protobuf wire bindings when schema sources are present.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    // Resolve external proto root relative to this crate.
    let proto_root = manifest_dir
        .parent() // crates/
        .and_then(|p| p.parent()) // sidecar/
        .and_then(|p| p.parent()) // parent directory
        .map(|p| p.join("proto"))
        .expect("failed to resolve proto root");
    let schema_dir = proto_root.join("rollup/v2");
    let messages = schema_dir.join("messages.proto");

    if !proto_root.exists() {
        // Build with checked-in wire structs when external proto sources
        // are unavailable.
        eprintln!("cargo:warning=Proto root not found at {proto_root:?}, skipping generation");
        return Ok(());
    }
    if !messages.exists() {
        eprintln!("cargo:warning=Proto schema not found at {messages:?}, skipping generation");
        return Ok(());
    }

    println!(
        "cargo:rerun-if-changed={}",
        schema_dir.join("messages.proto").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        schema_dir.join("consensus.proto").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        schema_dir.join("sbcp.proto").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        schema_dir.join("transport.proto").display()
    );

    prost_build::Config::new()
        .out_dir(manifest_dir.join("src"))
        .compile_protos(&[messages], &[&proto_root])?;

    Ok(())
}
