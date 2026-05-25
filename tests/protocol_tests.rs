//! Integration tests for the ruka wire protocol.
//!
//! Tests cover:
//! - Round-trip encode → decode for all frame types
//! - Boundary conditions (empty payload, max-length topic)
//! - Error cases (bad magic, unknown API key, truncated frames)
//! - Multiple frames in a single buffer
//! - Request/Response type conversions
//! - Codec integration with incremental reads

use bytes::{Buf, BufMut, Bytes, BytesMut};
use ruka::protocol::codec::RukaCodec;
use ruka::protocol::frame::Frame;
use ruka::protocol::request::{FetchRequest, ProduceRequest, Request};
use ruka::protocol::response::{ErrorResponse, FetchResponse, ProduceResponse};
use ruka::protocol::types::{ApiKey, ErrorCode, MAGIC_BYTE, MAX_FRAME_SIZE};
use ruka::storage::segment::Record;
use tokio_util::codec::{Decoder, Encoder};

// ─────────────────────────────────────────────────────────
// Frame round-trip tests
// ─────────────────────────────────────────────────────────

#[test]
fn frame_produce_round_trip() {
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 1,
        topic: "orders".to_string(),
        partition: 0,
        payload: Bytes::from_static(b"order-123-created"),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame, decoded);
    assert!(buf.is_empty());
}

#[test]
fn frame_fetch_round_trip() {
    let mut offset_payload = BytesMut::with_capacity(8);
    offset_payload.put_u64(42);

    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Fetch,
        correlation_id: 2,
        topic: "events".to_string(),
        partition: 3,
        payload: offset_payload.freeze(),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame, decoded);
}

// ─────────────────────────────────────────────────────────
// Boundary condition tests
// ─────────────────────────────────────────────────────────

#[test]
fn frame_empty_payload() {
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 0,
        topic: "t".to_string(),
        partition: 0,
        payload: Bytes::new(),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame, decoded);
    assert!(decoded.payload.is_empty());
}

#[test]
fn frame_single_char_topic() {
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 7,
        topic: "x".to_string(),
        partition: 0,
        payload: Bytes::from_static(b"y"),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame, decoded);
}

#[test]
fn frame_long_topic_name() {
    let long_topic = "a".repeat(1000);
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 8,
        topic: long_topic.clone(),
        partition: 99,
        payload: Bytes::from_static(b"data"),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf);
    assert!(decoded.is_err());
}

#[test]
fn frame_large_payload() {
    let large_payload = Bytes::from(vec![0xABu8; 65536]);
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 10,
        topic: "big-data".to_string(),
        partition: 0,
        payload: large_payload.clone(),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.payload, large_payload);
}

#[test]
fn frame_max_partition_id() {
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 11,
        topic: "test".to_string(),
        partition: u32::MAX,
        payload: Bytes::from_static(b"data"),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.partition, u32::MAX);
}

#[test]
fn frame_max_correlation_id() {
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Fetch,
        correlation_id: u32::MAX,
        topic: "test".to_string(),
        partition: 0,
        payload: Bytes::from(vec![0u8; 8]),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.correlation_id, u32::MAX);
}

// ─────────────────────────────────────────────────────────
// Error case tests
// ─────────────────────────────────────────────────────────

#[test]
fn decode_empty_buffer_returns_none() {
    let mut buf = BytesMut::new();
    let result = Frame::decode(&mut buf).unwrap();
    assert!(result.is_none());
}

#[test]
fn decode_partial_length_returns_none() {
    let mut buf = BytesMut::from(&[0x00, 0x00][..]);
    let result = Frame::decode(&mut buf).unwrap();
    assert!(result.is_none());
}

#[test]
fn decode_bad_magic_byte_errors() {
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 1,
        topic: "t".to_string(),
        partition: 0,
        payload: Bytes::from_static(b"x"),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    // Corrupt magic byte (position 4, after the 4-byte TotalLength)
    buf[4] = 0xFF;

    let result = Frame::decode(&mut buf);
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("magic byte"));
}

#[test]
fn decode_unknown_api_key_errors() {
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 1,
        topic: "t".to_string(),
        partition: 0,
        payload: Bytes::from_static(b"x"),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    // Corrupt API key (positions 5-6) to 0x00FF = 255
    buf[5] = 0x00;
    buf[6] = 0xFF;

    let result = Frame::decode(&mut buf);
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("API key"));
}

#[test]
fn decode_truncated_frame_returns_none() {
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 1,
        topic: "test-topic".to_string(),
        partition: 0,
        payload: Bytes::from_static(b"hello world"),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    // Keep only the length prefix + partial body
    let partial_len = buf.len() / 2;
    buf.truncate(partial_len);

    let result = Frame::decode(&mut buf).unwrap();
    assert!(result.is_none(), "truncated frame should return None");
}

#[test]
fn decode_random_bytes_no_panic() {
    // Feed random-ish bytes and ensure we get an error, not a panic
    let random_data: Vec<u8> = (0..200).map(|i| (i * 37 + 13) as u8).collect();

    // Construct a buffer with a valid length prefix pointing to the random data
    let mut buf = BytesMut::new();
    buf.put_u32(random_data.len() as u32);
    buf.extend_from_slice(&random_data);

    // This should return an error (bad magic byte), not panic
    let result = Frame::decode(&mut buf);
    assert!(result.is_err());
}

// ─────────────────────────────────────────────────────────
// Multiple frames in buffer
// ─────────────────────────────────────────────────────────

#[test]
fn multiple_frames_sequential_decode() {
    let frames: Vec<Frame> = (0..5)
        .map(|i| Frame {
            magic: MAGIC_BYTE,
            api_key: if i % 2 == 0 {
                ApiKey::Produce
            } else {
                ApiKey::Fetch
            },
            correlation_id: i,
            topic: format!("topic-{}", i),
            partition: i,
            payload: Bytes::from(format!("payload-{}", i)),
        })
        .collect();

    let mut buf = BytesMut::new();
    for frame in &frames {
        frame.encode(&mut buf);
    }

    for expected in &frames {
        let decoded = Frame::decode(&mut buf).unwrap().unwrap();
        assert_eq!(&decoded, expected);
    }
    assert!(buf.is_empty());
}

// ─────────────────────────────────────────────────────────
// Request type conversion tests
// ─────────────────────────────────────────────────────────

#[test]
fn produce_request_full_round_trip() {
    let req = ProduceRequest {
        correlation_id: 100,
        topic: "user-events".to_string(),
        partition: 2,
        payload: Bytes::from_static(b"user signed up"),
    };

    // Request → Frame → encode → decode → Frame → Request
    let frame = req.clone().into_frame();
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded_frame = Frame::decode(&mut buf).unwrap().unwrap();
    let decoded_req = Request::from_frame(decoded_frame).unwrap();

    assert_eq!(Request::Produce(req), decoded_req);
}

#[test]
fn fetch_request_full_round_trip() {
    let req = FetchRequest {
        correlation_id: 200,
        topic: "page-views".to_string(),
        partition: 0,
        offset: 999_999,
        max_bytes: 4096,
    };

    let frame = req.clone().into_frame();
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded_frame = Frame::decode(&mut buf).unwrap().unwrap();
    let decoded_req = Request::from_frame(decoded_frame).unwrap();

    assert_eq!(Request::Fetch(req), decoded_req);
}

// ─────────────────────────────────────────────────────────
// Response type tests
// ─────────────────────────────────────────────────────────

#[test]
fn produce_response_round_trip_through_wire() {
    let resp = ProduceResponse {
        correlation_id: 42,
        topic: "orders".to_string(),
        partition: 0,
        offset: 12345,
    };

    let frame = resp.into_frame();
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.correlation_id, 42);
    assert_eq!(decoded.topic, "orders");

    // Verify offset is encoded correctly in payload
    let offset = u64::from_be_bytes(decoded.payload[..8].try_into().unwrap());
    assert_eq!(offset, 12345);
}

#[test]
fn fetch_response_round_trip_through_wire() {
    let resp = FetchResponse {
        correlation_id: 77,
        topic: "logs".to_string(),
        partition: 1,
        payload: Bytes::from_static(b"fetched log entry data"),
    };

    let frame = resp.clone().into_frame();
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.correlation_id, 77);
    assert_eq!(
        decoded.payload,
        Bytes::from_static(b"fetched log entry data")
    );
}

// ─────────────────────────────────────────────────────────
// Codec integration tests
// ─────────────────────────────────────────────────────────

#[test]
fn codec_round_trip() {
    let mut codec = RukaCodec::new();
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 500,
        topic: "codec-integration".to_string(),
        partition: 4,
        payload: Bytes::from_static(b"codec test data"),
    };

    let mut buf = BytesMut::new();
    codec.encode(frame.clone(), &mut buf).unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame, decoded);
}

#[test]
fn codec_multiple_frames() {
    let mut codec = RukaCodec::new();

    let frames: Vec<Frame> = (0..10)
        .map(|i| Frame {
            magic: MAGIC_BYTE,
            api_key: ApiKey::Produce,
            correlation_id: i * 100,
            topic: format!("t{}", i),
            partition: i,
            payload: Bytes::from(vec![i as u8; (i as usize + 1) * 10]),
        })
        .collect();

    let mut buf = BytesMut::new();
    for frame in &frames {
        codec.encode(frame.clone(), &mut buf).unwrap();
    }

    for expected in &frames {
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(&decoded, expected);
    }
    assert!(buf.is_empty());
}

#[test]
fn codec_incremental_feed() {
    let mut codec = RukaCodec::new();
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Fetch,
        correlation_id: 999,
        topic: "incremental-test".to_string(),
        partition: 0,
        payload: Bytes::from_static(b"gradual"),
    };

    let mut full = BytesMut::new();
    codec.encode(frame.clone(), &mut full).unwrap();

    // Feed one byte at a time
    let mut partial = BytesMut::new();
    let total = full.len();
    for i in 0..total {
        partial.extend_from_slice(&full[i..i + 1]);
        let result = codec.decode(&mut partial).unwrap();
        if i < total - 1 {
            assert!(result.is_none());
        } else {
            assert_eq!(result.unwrap(), frame);
        }
    }
}

// ─────────────────────────────────────────────────────────
// Wire format correctness tests
// ─────────────────────────────────────────────────────────

#[test]
fn wire_format_byte_layout() {
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 0x01020304,
        topic: "AB".to_string(),
        partition: 0x05060708,
        payload: Bytes::from_static(b"\x09\x0A"),
    };
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    // Total body length:
    // magic(1) + api_key(2) + corr_id(4) + topic_len(2) + topic(2) + partition(4) + payload_len(4) + payload(2)
    // = 1 + 2 + 4 + 2 + 2 + 4 + 4 + 2 = 21
    let expected_body_len: u32 = 21;

    // TotalLength (4B, big-endian)
    assert_eq!(&buf[0..4], &expected_body_len.to_be_bytes());
    // MagicByte (1B)
    assert_eq!(buf[4], 0xCA);
    // ApiKey (2B, big-endian) = 0x0001 (Produce is 1)
    assert_eq!(&buf[5..7], &[0x00, 0x01]);
    // CorrelationId (4B, big-endian) = 0x01020304
    assert_eq!(&buf[7..11], &[0x01, 0x02, 0x03, 0x04]);
    // TopicLength (2B, big-endian) = 0x0002
    assert_eq!(&buf[11..13], &[0x00, 0x02]);
    // TopicName = "AB"
    assert_eq!(&buf[13..15], b"AB");
    // Partition (4B, big-endian) = 0x05060708
    assert_eq!(&buf[15..19], &[0x05, 0x06, 0x07, 0x08]);
    // PayloadLength (4B, big-endian) = 0x00000002
    assert_eq!(&buf[19..23], &[0x00, 0x00, 0x00, 0x02]);
    // Payload = [0x09, 0x0A]
    assert_eq!(&buf[23..25], &[0x09, 0x0A]);
    // Total buffer length = 4 + 21 = 25
    assert_eq!(buf.len(), 25);
}

#[test]
fn frame_size_too_large() {
    // Craft a buffer with a TotalLength that exceeds MAX_FRAME_SIZE
    let mut buf = BytesMut::new();
    buf.put_u32((MAX_FRAME_SIZE + 1) as u32);
    // Don't need the full body — the length check should fail first
    buf.extend_from_slice(&[0u8; 32]);

    let result = Frame::decode(&mut buf);
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("exceeds maximum size"));
}

// ─────────────────────────────────────────────────────────
// Error response & advanced frame tests
// ─────────────────────────────────────────────────────────

#[test]
fn error_response_round_trip() {
    use ruka::protocol::response::Response;

    let err_resp = ErrorResponse {
        correlation_id: 42,
        error_code: ErrorCode::OffsetNotFound,
        error_message: "offset 99 not found".to_string(),
    };

    let frame = Response::Error(err_resp).into_frame();
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.api_key, ApiKey::Error);
    assert_eq!(decoded.correlation_id, 42);
    assert_eq!(decoded.topic, "");

    // Payload starts with ErrorCode::OffsetNotFound as u16 (big-endian)
    let expected_code = ErrorCode::OffsetNotFound as u16;
    let actual_code = u16::from_be_bytes([decoded.payload[0], decoded.payload[1]]);
    assert_eq!(actual_code, expected_code);
}

#[test]
fn fetch_response_multi_record_decode() {
    // Build payload with 2 records
    let record0 = Record {
        offset: 0,
        timestamp_ms: 1000,
        key: None,
        value: Some(Bytes::from("msg-0")),
    };
    let record1 = Record {
        offset: 1,
        timestamp_ms: 2000,
        key: None,
        value: Some(Bytes::from("msg-1")),
    };

    let mut payload_buf = BytesMut::new();
    payload_buf.put_u32(2); // num_records
    record0.encode(&mut payload_buf);
    record1.encode(&mut payload_buf);

    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Fetch,
        correlation_id: 55,
        topic: "t".to_string(),
        partition: 0,
        payload: payload_buf.freeze(),
    };

    let mut buf = BytesMut::new();
    frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.api_key, ApiKey::Fetch);

    // Parse the payload back
    let mut payload = BytesMut::from(&decoded.payload[..]);
    let num_records = payload.get_u32();
    assert_eq!(num_records, 2);

    let dec0 = Record::decode(&mut payload).unwrap().unwrap();
    assert_eq!(dec0.offset, 0);
    assert_eq!(dec0.timestamp_ms, 1000);
    assert_eq!(dec0.key, None);
    assert_eq!(dec0.value, Some(Bytes::from("msg-0")));

    let dec1 = Record::decode(&mut payload).unwrap().unwrap();
    assert_eq!(dec1.offset, 1);
    assert_eq!(dec1.timestamp_ms, 2000);
    assert_eq!(dec1.key, None);
    assert_eq!(dec1.value, Some(Bytes::from("msg-1")));
}

#[test]
fn fetch_rejects_error_request_frame() {
    let frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Error,
        correlation_id: 1,
        topic: String::new(),
        partition: 0,
        payload: Bytes::new(),
    };

    let result = Request::from_frame(frame);
    assert!(
        result.is_err(),
        "Error frames should not be accepted as requests"
    );
}

#[test]
fn frame_allows_empty_topic_for_error_only() {
    // ApiKey::Error with empty topic should succeed
    let error_frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Error,
        correlation_id: 10,
        topic: "".to_string(),
        partition: 0,
        payload: Bytes::from_static(b"err"),
    };
    let mut buf = BytesMut::new();
    error_frame.encode(&mut buf);

    let decoded = Frame::decode(&mut buf);
    assert!(
        decoded.is_ok(),
        "Error frame with empty topic should decode successfully"
    );
    let decoded = decoded.unwrap().unwrap();
    assert_eq!(decoded.topic, "");

    // ApiKey::Produce with empty topic should fail with InvalidTopicName
    let produce_frame = Frame {
        magic: MAGIC_BYTE,
        api_key: ApiKey::Produce,
        correlation_id: 11,
        topic: "".to_string(),
        partition: 0,
        payload: Bytes::from_static(b"data"),
    };
    let mut buf = BytesMut::new();
    produce_frame.encode(&mut buf);

    let result = Frame::decode(&mut buf);
    assert!(
        result.is_err(),
        "Produce frame with empty topic should fail"
    );
}
