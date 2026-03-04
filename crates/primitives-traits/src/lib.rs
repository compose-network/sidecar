//! Shared traits and error types for sidecar coordination.
//!
//! This crate defines the integration boundaries between the coordinator
//! and its external dependencies (publisher, mailbox, put-inbox). By
//! sitting below the coordinator in the dependency graph, these traits
//! can be implemented by lower-level crates without circular dependencies.

pub mod error;
pub mod mailbox;
pub mod publisher;
pub mod put_inbox;

pub use error::CoordinatorError;
pub use mailbox::MailboxSender;
pub use publisher::PublisherClient;
pub use put_inbox::PutInboxBuilder;
