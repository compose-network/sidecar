# Build all crates
build:
    cargo build --workspace

# Build release binary
release:
    cargo build --release -p sidecar

# Run all tests
test:
    cargo test --workspace

# Run tests with verbose output
test-verbose:
    cargo test --workspace -- --nocapture

# Run clippy lints
lint:
    cargo clippy --workspace -- -D warnings

# Format code
fmt:
    cargo fmt --all

# Check formatting
fmt-check:
    cargo fmt --all -- --check

# Run all checks (lint + test + format)
check: fmt-check lint test

# Generate protobuf code
proto:
    cargo build -p compose-proto

# Run the sidecar binary
run *ARGS:
    cargo run -p sidecar -- {{ARGS}}

# Clean build artifacts
clean:
    cargo clean
