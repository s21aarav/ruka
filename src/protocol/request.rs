//! Typed request wrappers over raw frames.
//!
//! These provide semantic meaning to the raw binary payload
//! based on the `ApiKey`.

use bytes::{Buf, Bytes};

use crate::error::{Result, RukaError};
use crate::protocol::frame::Frame;
use crate::protocol::types::ApiKey;

/// A produce request: append a message to a topic partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    /// Client-assigned correlation ID.
    pub correlation_id: u32,
    /// Target topic name.
    pub topic: String,
    /// Target partition index.
    pub partition: u32,
    /// Raw message payload to append.
    pub payload: Bytes,
}

/// A produce batch request: append multiple messages to a topic partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceBatchRequest {
    /// Client-assigned correlation ID.
    pub correlation_id: u32,
    /// Target topic name.
    pub topic: String,
    /// Target partition index.
    pub partition: u32,
    /// Vector of raw message payloads to append.
    pub payloads: Vec<Bytes>,
}

/// A fetch request: read messages from a topic partition at a given offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    /// Client-assigned correlation ID.
    pub correlation_id: u32,
    /// Target topic name.
    pub topic: String,
    /// Target partition index.
    pub partition: u32,
    /// The logical offset to begin reading from.
    pub offset: u64,
    /// Maximum bytes to return.
    pub max_bytes: u32,
}

/// A parsed, typed request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Produce(ProduceRequest),
    ProduceBatch(ProduceBatchRequest),
    Fetch(FetchRequest),
}

impl Request {
    /// Convert a raw `Frame` into a typed `Request`.
    ///
    /// For `FETCH` frames, the payload must contain exactly 8 bytes
    /// representing the big-endian offset.
    pub fn from_frame(frame: Frame) -> Result<Self> {
        match frame.api_key {
            ApiKey::Error => Err(RukaError::InvalidFrame(
                "Client cannot send Error frames".to_string(),
            )),
            ApiKey::Produce => Ok(Request::Produce(ProduceRequest {
                correlation_id: frame.correlation_id,
                topic: frame.topic,
                partition: frame.partition,
                payload: frame.payload,
            })),
            ApiKey::ProduceBatch => {
                let mut payload = frame.payload;
                if payload.len() < 4 {
                    return Err(RukaError::FrameTooShort {
                        needed: 4,
                        have: payload.len(),
                    });
                }
                let batch_len = payload.get_u32() as usize;
                let mut payloads = Vec::with_capacity(batch_len);
                for _ in 0..batch_len {
                    if payload.len() < 4 {
                        return Err(RukaError::FrameTooShort {
                            needed: 4,
                            have: payload.len(),
                        });
                    }
                    let msg_len = payload.get_u32() as usize;
                    if payload.len() < msg_len {
                        return Err(RukaError::FrameTooShort {
                            needed: msg_len,
                            have: payload.len(),
                        });
                    }
                    payloads.push(payload.copy_to_bytes(msg_len));
                }
                if payload.has_remaining() {
                    return Err(RukaError::InvalidFrame(
                        "trailing bytes in batch payload".to_string(),
                    ));
                }
                Ok(Request::ProduceBatch(ProduceBatchRequest {
                    correlation_id: frame.correlation_id,
                    topic: frame.topic,
                    partition: frame.partition,
                    payloads,
                }))
            }
            ApiKey::Fetch => {
                if frame.payload.len() != 12 {
                    return Err(RukaError::FrameTooShort {
                        needed: 12,
                        have: frame.payload.len(),
                    });
                }
                let mut payload = frame.payload;
                let offset = payload.get_u64();
                let max_bytes = payload.get_u32();
                Ok(Request::Fetch(FetchRequest {
                    correlation_id: frame.correlation_id,
                    topic: frame.topic,
                    partition: frame.partition,
                    offset,
                    max_bytes,
                }))
            }
        }
    }
}

impl ProduceRequest {
    /// Convert back to a raw `Frame` for wire transmission.
    pub fn into_frame(self) -> Frame {
        Frame {
            magic: crate::protocol::types::MAGIC_BYTE,
            api_key: ApiKey::Produce,
            correlation_id: self.correlation_id,
            topic: self.topic,
            partition: self.partition,
            payload: self.payload,
        }
    }
}

impl ProduceBatchRequest {
    /// Convert back to a raw `Frame` for wire transmission.
    pub fn into_frame(self) -> Frame {
        let mut payload = bytes::BytesMut::new();
        bytes::BufMut::put_u32(&mut payload, self.payloads.len() as u32);
        for p in self.payloads {
            bytes::BufMut::put_u32(&mut payload, p.len() as u32);
            bytes::BufMut::put_slice(&mut payload, &p);
        }
        Frame {
            magic: crate::protocol::types::MAGIC_BYTE,
            api_key: ApiKey::ProduceBatch,
            correlation_id: self.correlation_id,
            topic: self.topic,
            partition: self.partition,
            payload: payload.freeze(),
        }
    }
}

impl FetchRequest {
    /// Convert back to a raw `Frame` for wire transmission.
    ///
    /// The offset is encoded as 8 big-endian bytes, followed by 4 bytes for max_bytes.
    pub fn into_frame(self) -> Frame {
        let mut payload = bytes::BytesMut::with_capacity(12);
        bytes::BufMut::put_u64(&mut payload, self.offset);
        bytes::BufMut::put_u32(&mut payload, self.max_bytes);
        Frame {
            magic: crate::protocol::types::MAGIC_BYTE,
            api_key: ApiKey::Fetch,
            correlation_id: self.correlation_id,
            topic: self.topic,
            partition: self.partition,
            payload: payload.freeze(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::MAGIC_BYTE;

    #[test]
    fn produce_from_frame() {
        let frame = Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Produce,
            correlation_id: 1,
            topic: "logs".to_string(),
            partition: 0,
            payload: Bytes::from_static(b"event data"),
        };
        let req = Request::from_frame(frame).unwrap();
        match req {
            Request::Produce(p) => {
                assert_eq!(p.topic, "logs");
                assert_eq!(p.payload, Bytes::from_static(b"event data"));
            }
            _ => panic!("expected ProduceRequest"),
        }
    }

    #[test]
    fn fetch_from_frame() {
        let mut offset_bytes = bytes::BytesMut::with_capacity(12);
        bytes::BufMut::put_u64(&mut offset_bytes, 12345);
        bytes::BufMut::put_u32(&mut offset_bytes, 1024);
        let frame = Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Fetch,
            correlation_id: 2,
            topic: "events".to_string(),
            partition: 3,
            payload: offset_bytes.freeze(),
        };
        let req = Request::from_frame(frame).unwrap();
        match req {
            Request::Fetch(f) => {
                assert_eq!(f.offset, 12345);
                assert_eq!(f.max_bytes, 1024);
                assert_eq!(f.topic, "events");
                assert_eq!(f.partition, 3);
            }
            _ => panic!("expected FetchRequest"),
        }
    }

    #[test]
    fn fetch_too_short_payload() {
        let frame = Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Fetch,
            correlation_id: 3,
            topic: "t".to_string(),
            partition: 0,
            payload: Bytes::from_static(b"shortness"), // only 9 bytes, need 12
        };
        let result = Request::from_frame(frame);
        assert!(result.is_err());
    }

    #[test]
    fn produce_round_trip_through_frame() {
        let req = ProduceRequest {
            correlation_id: 10,
            topic: "my-topic".to_string(),
            partition: 2,
            payload: Bytes::from_static(b"hello"),
        };
        let frame = req.clone().into_frame();
        let req2 = Request::from_frame(frame).unwrap();
        assert_eq!(Request::Produce(req), req2);
    }

    #[test]
    fn fetch_round_trip_through_frame() {
        let req = FetchRequest {
            correlation_id: 20,
            topic: "my-topic".to_string(),
            partition: 1,
            offset: 99999,
            max_bytes: 4096,
        };
        let frame = req.clone().into_frame();
        let req2 = Request::from_frame(frame).unwrap();
        assert_eq!(Request::Fetch(req), req2);
    }
}
