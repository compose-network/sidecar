//! Length-prefixed framing codec for transport payloads.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::TransportError;

/// Length-prefixed codec for the publisher wire format.
///
/// Format: `[4 bytes big-endian length][protobuf payload]`
#[derive(Debug, Clone)]
pub struct LengthPrefixCodec {
    max_message_size: usize,
}

impl LengthPrefixCodec {
    pub fn new(max_message_size: usize) -> Self {
        Self { max_message_size }
    }

    /// Encode a payload into a length-prefixed frame.
    pub fn encode(&self, payload: &[u8]) -> Result<Bytes, TransportError> {
        if payload.len() > self.max_message_size {
            return Err(TransportError::MessageTooLarge {
                size: payload.len(),
                max: self.max_message_size,
            });
        }
        let mut buf = BytesMut::with_capacity(4 + payload.len());
        buf.put_u32(payload.len() as u32);
        buf.put_slice(payload);
        Ok(buf.freeze())
    }

    /// Decode the length prefix from the first 4 bytes, returning the expected
    /// payload size.
    pub fn decode_length(&self, header: &[u8; 4]) -> Result<usize, TransportError> {
        let len = u32::from_be_bytes(*header) as usize;
        if len > self.max_message_size {
            return Err(TransportError::MessageTooLarge {
                size: len,
                max: self.max_message_size,
            });
        }
        Ok(len)
    }

    /// Decode a complete frame (length prefix + payload) from a buffer.
    /// Returns the payload and advances the buffer past the consumed frame.
    pub fn decode_frame(&self, buf: &mut BytesMut) -> Result<Option<Bytes>, TransportError> {
        if buf.len() < 4 {
            return Ok(None);
        }

        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if len > self.max_message_size {
            return Err(TransportError::MessageTooLarge {
                size: len,
                max: self.max_message_size,
            });
        }

        if buf.len() < 4 + len {
            return Ok(None);
        }

        buf.advance(4);
        let payload = buf.split_to(len).freeze();
        Ok(Some(payload))
    }
}

impl Default for LengthPrefixCodec {
    fn default() -> Self {
        Self::new(4 * 1024 * 1024) // 4 MiB
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let codec = LengthPrefixCodec::default();
        let payload = b"hello world";
        let frame = codec.encode(payload).unwrap();

        let mut buf = BytesMut::from(&frame[..]);
        let decoded = codec.decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(&decoded[..], payload);
    }

    #[test]
    fn rejects_oversized() {
        let codec = LengthPrefixCodec::new(10);
        let payload = vec![0u8; 11];
        assert!(codec.encode(&payload).is_err());
    }

    #[test]
    fn partial_frame_returns_none() {
        let codec = LengthPrefixCodec::default();
        let mut buf = BytesMut::from(&[0u8, 0, 0, 5, 1, 2][..]);
        assert!(codec.decode_frame(&mut buf).unwrap().is_none());
    }
}
