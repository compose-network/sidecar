//! Core sidecar coordinator implementation.
//!
//! It manages the XT lifecycle from start-instance intake through simulation,
//! voting, decision handling, and builder-facing delivery.

pub mod builder;
pub mod coordinator;
pub mod handlers;
pub mod model;
pub mod nonce_manager;
pub mod pipeline;
pub mod traits;

pub use compose_primitives_traits::{
    CoordinatorError, MailboxSender, PublisherClient, PutInboxBuilder,
};
