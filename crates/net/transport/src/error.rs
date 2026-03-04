//! Transport-layer error types and conversions.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("connection closed")]
    ConnectionClosed,

    #[error("connection refused: {0}")]
    ConnectionRefused(String),

    #[error("timeout")]
    Timeout,

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("QUIC error: {0}")]
    Quic(String),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("message too large: {size} > {max}")]
    MessageTooLarge { size: usize, max: usize },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

impl From<quinn::ConnectionError> for TransportError {
    fn from(err: quinn::ConnectionError) -> Self {
        Self::Quic(err.to_string())
    }
}

impl From<quinn::WriteError> for TransportError {
    fn from(err: quinn::WriteError) -> Self {
        Self::Quic(err.to_string())
    }
}

impl From<quinn::ReadExactError> for TransportError {
    fn from(err: quinn::ReadExactError) -> Self {
        Self::Quic(err.to_string())
    }
}

impl From<prost::DecodeError> for TransportError {
    fn from(err: prost::DecodeError) -> Self {
        Self::Codec(err.to_string())
    }
}
