fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use protox to parse proto files without requiring protoc binary.
    let file_descriptors = protox::compile(["proto/krabnet.proto"], ["proto/"])?;
    tonic_build::compile_fds(file_descriptors)?;
    Ok(())
}
