//! Protobuf wire types and conversion helpers for sidecar networking.
//!
//! This crate defines the transport and coordination messages exchanged between
//! sidecars, builders, and publisher components.

/// Protocol wire message definitions.
pub mod rollup_v2;
pub use rollup_v2::*;

/// Conversion helpers between protobuf fields and sidecar primitives.
pub mod conversions;
