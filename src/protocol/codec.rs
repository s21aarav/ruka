//! Tokio codec for framing `Frame` values over a TCP stream.
//!
//! Uses length-delimited framing: the first 4 bytes of each message
//! specify the total length of the remaining frame body.

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::RukaError;
use crate::protocol::frame::Frame;

/// A codec that encodes/decodes `Frame` values on a byte stream.
///
/// Works with `tokio_util::codec::Framed` to provide a `Stream + Sink`
/// interface over a `TcpStream`.
#[derive(Debug, Default)]
pub struct RukaCodec;

impl RukaCodec {
    /// Create a new `RukaCodec`.
    pub fn new() -> Self {
        Self
    }
}

impl Decoder for RukaCodec {
    type Item = Frame;
    type Error = RukaError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Frame::decode(src)
    }
}

impl Encoder<Frame> for RukaCodec {
    type Error = RukaError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::{ApiKey, MAGIC_BYTE};
    use bytes::Bytes;

    #[test]
    fn codec_encode_then_decode() {
        let mut codec = RukaCodec::new();
        let frame = Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Produce,
            correlation_id: 100,
            topic: "codec-test".to_string(),
            partition: 7,
            payload: Bytes::from_static(b"codec payload"),
        };

        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, decoded);
    }

    #[test]
    fn codec_decode_incremental() {
        let mut codec = RukaCodec::new();
        let frame = Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Fetch,
            correlation_id: 200,
            topic: "incremental".to_string(),
            partition: 0,
            payload: Bytes::from_static(b"data"),
        };

        let mut full_buf = BytesMut::new();
        codec.encode(frame.clone(), &mut full_buf).unwrap();

        // Feed bytes one at a time
        let mut partial = BytesMut::new();
        for i in 0..full_buf.len() {
            partial.extend_from_slice(&full_buf[i..i + 1]);
            let result = codec.decode(&mut partial).unwrap();
            if i < full_buf.len() - 1 {
                assert!(result.is_none(), "should not decode until all bytes arrive");
            } else {
                assert_eq!(result.unwrap(), frame);
            }
        }
    }
}
