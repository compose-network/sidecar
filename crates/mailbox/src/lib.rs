//! Mailbox-focused helpers used by the sidecar simulation and delivery flows.
//!
//! This crate provides mailbox ABI encoding, dependency matching, queue
//! abstractions, and trace parsing.

pub mod abi;
pub mod builder;
pub mod error;
pub mod matching;
pub mod overrides;
pub mod parser;
pub mod put_inbox;
pub mod queue;
pub mod traits;
pub mod types;
