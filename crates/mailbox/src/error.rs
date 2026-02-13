//! Mailbox crate error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum MailboxError {
    #[error("queue error: {0}")]
    Queue(String),
    #[error("send error: {0}")]
    Send(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("abi encoding error: {0}")]
    Abi(String),
}
