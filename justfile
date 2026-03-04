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
    cargo clippy --workspace --all-targets -- -D warnings

# Apply clippy suggestions automatically
lint-fix:
    cargo clippy --workspace --all-targets --fix --allow-staged -- -D warnings

# Format code
fmt:
    cargo fmt --all

# Check formatting
fmt-check:
    cargo fmt --all -- --check

# Check for unused dependencies (requires: cargo install cargo-machete)
machete:
    cargo machete

# Check licenses, advisories, and bans (requires: cargo install cargo-deny)
deny:
    cargo deny check

# Generate and open documentation
doc:
    cargo doc --workspace --no-deps --open

# Run the full CI suite locally
ci: fmt-check lint test

# Run the full CI suite including optional tools
ci-full: fmt-check lint test deny machete

# Generate protobuf code
proto:
    cargo build -p compose-proto

# Run the sidecar binary
run *ARGS:
    cargo run -p sidecar -- {{ARGS}}

# Install the pre-commit hooks (requires: pip install pre-commit)
install-hooks:
    pre-commit install

# Run pre-commit on all files
pre-commit:
    pre-commit run --all-files

# Install required dev tools
install-tools:
    cargo install cargo-deny --locked
    cargo install cargo-machete --locked

# Clean build artifacts
clean:
    cargo clean
