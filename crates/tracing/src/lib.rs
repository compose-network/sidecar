//! Shared tracing bootstrap for sidecar binaries and services.
//!
//! Provides a single `init` entrypoint that configures structured JSON logging
//! or pretty console output with `EnvFilter` support.

use tracing_subscriber::{fmt, EnvFilter};

/// Initialize the global tracing subscriber.
///
/// - `level`: filter string (e.g. `"info"`, `"debug"`, `"compose_coordinator=debug,info"`).
/// - `format`: either `"json"` or `"pretty"`.
pub fn init(level: &str, format: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    match format {
        "pretty" => {
            fmt::Subscriber::builder()
                .with_env_filter(filter)
                .with_target(true)
                .pretty()
                .init();
        }
        _ => {
            fmt::Subscriber::builder()
                .with_env_filter(filter)
                .with_target(true)
                .json()
                .init();
        }
    }
}

/// Re-export tracing macros for convenience.
pub use tracing::{debug, error, info, trace, warn};
