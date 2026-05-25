//! Binary wire protocol for client-broker communication.
//!
//! The protocol uses a custom length-delimited binary frame format:
//! ```text
//! [TotalLength: 4B][MagicByte: 1B][ApiKey: 2B][CorrelationId: 4B]
//! [TopicLength: 2B][TopicName: VarB][Partition: 4B]
//! [PayloadLength: 4B][Payload: VarB]
//! ```
//!
//! All multi-byte integers use big-endian (network) byte order.

pub mod codec;
pub mod frame;
pub mod request;
pub mod response;
pub mod types;
