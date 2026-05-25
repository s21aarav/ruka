//! Response types for the wire protocol.
//!
//! Responses are simpler frames sent back to the client.
//! They share the same wire format as request frames.

use bytes::{BufMut, Bytes, BytesMut};

use crate::protocol::frame::Frame;
use crate::protocol::types::{ApiKey, ErrorCode, MAGIC_BYTE};

/// Response to a successful produce request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponse {
    /// Echoed correlation ID from the request.
    pub correlation_id: u32,
    /// The topic the message was written to.
    pub topic: String,
    /// The partition the message was written to.
    pub partition: u32,
    /// The committed offset assigned to the message.
    pub offset: u64,
}

/// Response to a successful produce batch request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceBatchResponse {
    /// Echoed correlation ID from the request.
    pub correlation_id: u32,
    /// The topic the batch was written to.
    pub topic: String,
    /// The partition the batch was written to.
    pub partition: u32,
    /// The base committed offset assigned to the first message in the batch.
    pub base_offset: u64,
}

/// Response to a fetch request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponse {
    /// Echoed correlation ID from the request.
    pub correlation_id: u32,
    /// The topic being read.
    pub topic: String,
    /// The partition being read.
    pub partition: u32,
    /// The fetched payload data.
    pub payload: Bytes,
}

/// An error response sent when a request cannot be fulfilled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResponse {
    /// Echoed correlation ID from the request.
    pub correlation_id: u32,
    /// Error code categorizing the failure.
    pub error_code: ErrorCode,
    /// Detailed error message.
    pub error_message: String,
}

/// A typed response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Produce(ProduceResponse),
    ProduceBatch(ProduceBatchResponse),
    Fetch(FetchResponse),
    Error(ErrorResponse),
}

impl ProduceResponse {
    /// Convert to a wire frame.
    ///
    /// The committed offset is encoded as 8 big-endian bytes in the payload.
    pub fn into_frame(self) -> Frame {
        let mut payload = BytesMut::with_capacity(8);
        payload.put_u64(self.offset);
        Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Produce,
            correlation_id: self.correlation_id,
            topic: self.topic,
            partition: self.partition,
            payload: payload.freeze(),
        }
    }
}

impl ProduceBatchResponse {
    /// Convert to a wire frame.
    ///
    /// The base committed offset is encoded as 8 big-endian bytes in the payload.
    pub fn into_frame(self) -> Frame {
        let mut payload = BytesMut::with_capacity(8);
        payload.put_u64(self.base_offset);
        Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::ProduceBatch,
            correlation_id: self.correlation_id,
            topic: self.topic,
            partition: self.partition,
            payload: payload.freeze(),
        }
    }
}

impl FetchResponse {
    /// Convert to a wire frame.
    pub fn into_frame(self) -> Frame {
        Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Fetch,
            correlation_id: self.correlation_id,
            topic: self.topic,
            partition: self.partition,
            payload: self.payload,
        }
    }
}

impl ErrorResponse {
    /// Convert to a wire frame.
    ///
    /// Uses ApiKey::Error with `[ErrorCode: 2B][Message: VarB]` payload.
    pub fn into_frame(self) -> Frame {
        let msg_bytes = self.error_message.as_bytes();
        let mut payload = BytesMut::with_capacity(2 + msg_bytes.len());
        payload.put_u16(self.error_code as u16);
        payload.put_slice(msg_bytes);

        Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Error,
            correlation_id: self.correlation_id,
            topic: String::new(),
            partition: 0,
            payload: payload.freeze(),
        }
    }

    pub fn from_error(
        correlation_id: u32,
        code: ErrorCode,
        e: impl std::string::ToString,
    ) -> Response {
        Response::Error(Self {
            correlation_id,
            error_code: code,
            error_message: e.to_string(),
        })
    }
}

impl Response {
    /// Convert any response variant into a wire frame.
    pub fn into_frame(self) -> Frame {
        match self {
            Response::Produce(r) => r.into_frame(),
            Response::ProduceBatch(r) => r.into_frame(),
            Response::Fetch(r) => r.into_frame(),
            Response::Error(r) => r.into_frame(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produce_response_round_trip() {
        let resp = ProduceResponse {
            correlation_id: 42,
            topic: "test".to_string(),
            partition: 0,
            offset: 1000,
        };
        let frame = resp.into_frame();
        assert_eq!(frame.correlation_id, 42);
        assert_eq!(frame.api_key, ApiKey::Produce);
        // Payload should be 8 bytes (big-endian offset)
        assert_eq!(frame.payload.len(), 8);
        let offset = u64::from_be_bytes(frame.payload[..8].try_into().unwrap());
        assert_eq!(offset, 1000);
    }

    #[test]
    fn fetch_response_preserves_payload() {
        let data = Bytes::from_static(b"some fetched data");
        let resp = FetchResponse {
            correlation_id: 7,
            topic: "logs".to_string(),
            partition: 1,
            payload: data.clone(),
        };
        let frame = resp.into_frame();
        assert_eq!(frame.payload, data);
        assert_eq!(frame.api_key, ApiKey::Fetch);
    }
}
