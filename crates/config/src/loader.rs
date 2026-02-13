//! Configuration loading from file and environment sources.

use crate::SidecarConfig;
use config::{Config, Environment, File};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("failed to load config: {0}")]
    Config(#[from] config::ConfigError),
}

/// Load configuration from a YAML file with environment variable overrides.
///
/// Environment variables use the `SIDECAR_` prefix with `__` as the separator
/// for nested keys (e.g. `SIDECAR_SERVER__LISTEN_ADDR`).
pub fn load(path: Option<&str>) -> Result<SidecarConfig, LoadError> {
    let mut builder = Config::builder();

    if let Some(path) = path {
        builder = builder.add_source(File::with_name(path).required(false));
    }

    builder = builder.add_source(
        Environment::with_prefix("SIDECAR")
            .separator("__")
            .try_parsing(true),
    );

    let config = builder.build()?;
    let cfg: SidecarConfig = config.try_deserialize()?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_missing_file_uses_defaults() {
        let cfg = load(Some("/nonexistent/config.yaml")).unwrap();
        assert_eq!(cfg.server.listen_addr, "0.0.0.0:8080");
    }
}
