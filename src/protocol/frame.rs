//! Binary frame encoding and decoding.
//!
//! Wire format (all integers big-endian):
//! ```text
//! [TotalLength: 4B][MagicByte: 1B][ApiKey: 2B][CorrelationId: 4B]
//! [TopicLength: 2B][TopicName: VarB][Partition: 4B]
//! [PayloadLength: 4B][Payload: VarB]
//! ```
//!
//! `TotalLength` covers everything after itself (MagicByte through end of Payload).

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Result, RukaError};
use crate::protocol::types::{ApiKey, MAGIC_BYTE, MAX_FRAME_SIZE, MIN_HEADER_SIZE};

/// A fully parsed wire protocol frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Must be `MAGIC_BYTE` (0xCA).
    pub magic: u8,
    /// The API operation (Produce or Fetch).
    pub api_key: ApiKey,
    /// Client-assigned correlation ID echoed in responses.
    pub correlation_id: u32,
    /// Target topic name.
    pub topic: String,
    /// Target partition index (zero-based).
    pub partition: u32,
    /// Variable-length payload.
    /// - For Produce: raw message bytes to append.
    /// - For Fetch: 8-byte big-endian offset to read from.
    pub payload: Bytes,
}

impl Frame {
    /// Compute the total byte length of this frame when serialized
    /// (NOT including the 4-byte TotalLength prefix itself).
    fn body_len(&self) -> usize {
        // magic(1) + api_key(2) + correlation_id(4) + topic_len(2)
        // + topic_bytes + partition(4) + payload_len(4) + payload_bytes
        1 + 2 + 4 + 2 + self.topic.len() + 4 + 4 + self.payload.len()
    }

    /// Serialize this frame into the destination buffer, including the
    /// 4-byte TotalLength prefix.
    pub fn encode(&self, dst: &mut BytesMut) {
        let body_len = self.body_len();
        dst.reserve(4 + body_len);

        // TotalLength (4B) — length of everything after this field
        dst.put_u32(body_len as u32);
        // MagicByte (1B)
        dst.put_u8(self.magic);
        // ApiKey (2B)
        dst.put_u16(self.api_key.as_u16());
        // CorrelationId (4B)
        dst.put_u32(self.correlation_id);
        // TopicLength (2B)
        dst.put_u16(self.topic.len() as u16);
        // TopicName (variable)
        dst.put_slice(self.topic.as_bytes());
        // Partition (4B)
        dst.put_u32(self.partition);
        // PayloadLength (4B)
        dst.put_u32(self.payload.len() as u32);
        // Payload (variable)
        dst.put_slice(&self.payload);
    }

    /// Attempt to decode a single frame from the source buffer.
    ///
    /// Returns `Ok(None)` if not enough bytes are available yet
    /// (partial read). Consumes the frame bytes from `src` on success.
    ///
    /// # Errors
    ///
    /// - `InvalidMagicByte` if the magic byte doesn't match.
    /// - `UnknownApiKey` if the API key is unrecognized.
    /// - `FrameTooLarge` if total length exceeds `MAX_FRAME_SIZE`.
    /// - `InvalidTopicName` if the topic bytes aren't valid UTF-8.
    /// - `FrameTooShort` if the frame body is shorter than the minimum header.
    pub fn decode(src: &mut BytesMut) -> Result<Option<Frame>> {
        // Need at least 4 bytes for TotalLength
        if src.len() < 4 {
            return Ok(None);
        }

        // Peek at total length without consuming
        let total_len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        // Validate frame size
        if total_len > MAX_FRAME_SIZE {
            return Err(RukaError::FrameTooLarge {
                max: MAX_FRAME_SIZE,
                got: total_len,
            });
        }

        // Check minimum body size
        if total_len < MIN_HEADER_SIZE {
            return Err(RukaError::FrameTooShort {
                needed: MIN_HEADER_SIZE,
                have: total_len,
            });
        }

        // Wait for full frame
        if src.len() < 4 + total_len {
            // Reserve space so the caller knows how much we expect
            src.reserve(4 + total_len - src.len());
            return Ok(None);
        }

        // Consume the TotalLength prefix
        src.advance(4);

        // Split off exactly `total_len` bytes for this frame
        let mut frame_buf = src.split_to(total_len);

        // MagicByte (1B)
        let magic = frame_buf.get_u8();
        if magic != MAGIC_BYTE {
            return Err(RukaError::InvalidMagicByte(magic));
        }

        // ApiKey (2B)
        let api_key_raw = frame_buf.get_u16();
        let api_key = ApiKey::try_from(api_key_raw)?;

        // CorrelationId (4B)
        let correlation_id = frame_buf.get_u32();

        // Topic (2B len + String)
        let topic_len = frame_buf.get_u16() as usize;
        if topic_len == 0 && api_key != ApiKey::Error {
            return Err(RukaError::InvalidTopicName);
        }
        if topic_len > 255 {
            return Err(RukaError::InvalidTopicName);
        }

        // Validate we have enough bytes for topic + partition + payload_len
        if frame_buf.remaining() < topic_len + 4 + 4 {
            return Err(RukaError::FrameTooShort {
                needed: topic_len + 4 + 4,
                have: frame_buf.remaining(),
            });
        }

        // TopicName (variable)
        let topic_bytes = frame_buf.split_to(topic_len);
        let topic = std::str::from_utf8(&topic_bytes)
            .map_err(|_| RukaError::InvalidTopicName)?
            .to_string();

        // Partition (4B)
        let partition = frame_buf.get_u32();

        // PayloadLength (4B)
        let payload_len = frame_buf.get_u32() as usize;

        // Validate payload length
        if frame_buf.remaining() < payload_len {
            return Err(RukaError::FrameTooShort {
                needed: payload_len,
                have: frame_buf.remaining(),
            });
        }

        // Payload (variable)
        let payload = frame_buf.split_to(payload_len).freeze();

        Ok(Some(Frame {
            magic,
            api_key,
            correlation_id,
            topic,
            partition,
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_produce_frame() -> Frame {
        Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Produce,
            correlation_id: 42,
            topic: "test-topic".to_string(),
            partition: 0,
            payload: Bytes::from_static(b"hello world"),
        }
    }

    #[test]
    fn round_trip_encode_decode() {
        let frame = make_produce_frame();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, decoded);
        assert!(buf.is_empty(), "all bytes should be consumed");
    }

    #[test]
    fn decode_partial_returns_none() {
        let frame = make_produce_frame();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        // Truncate to simulate partial read
        let partial = buf.split_to(buf.len() / 2);
        let mut partial_buf = BytesMut::from(&partial[..]);
        let result = Frame::decode(&mut partial_buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn decode_bad_magic_byte() {
        let mut buf = BytesMut::new();
        let frame = make_produce_frame();
        frame.encode(&mut buf);

        // Corrupt magic byte (at index 4, after TotalLength)
        buf[4] = 0xFF;

        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Err(RukaError::InvalidMagicByte(0xFF))));
    }

    #[test]
    fn decode_unknown_api_key() {
        let mut buf = BytesMut::new();
        let frame = make_produce_frame();
        frame.encode(&mut buf);

        // Corrupt api key (at index 5-6, after TotalLength + MagicByte)
        buf[5] = 0x00;
        buf[6] = 0xFF;

        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Err(RukaError::UnknownApiKey(255))));
    }

    #[test]
    fn empty_payload() {
        let frame = Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Fetch,
            correlation_id: 0,
            topic: "t".to_string(),
            partition: 0,
            payload: Bytes::new(),
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, decoded);
    }

    #[test]
    fn multiple_frames_in_buffer() {
        let f1 = make_produce_frame();
        let f2 = Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Fetch,
            correlation_id: 99,
            topic: "other".to_string(),
            partition: 5,
            payload: Bytes::from_static(b"fetch me"),
        };

        let mut buf = BytesMut::new();
        f1.encode(&mut buf);
        f2.encode(&mut buf);

        let d1 = Frame::decode(&mut buf).unwrap().unwrap();
        let d2 = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(f1, d1);
        assert_eq!(f2, d2);
        assert!(buf.is_empty());
    }
}
