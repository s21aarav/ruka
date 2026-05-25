use std::io;
use thiserror::Error;

/// Unified error type for the ruka broker.
#[derive(Debug, Error)]
pub enum RukaError {
    // Protocol errors
    #[error("invalid magic byte: expected 0xCA, got 0x{0:02X}")]
    InvalidMagicByte(u8),

    #[error("invalid frame: {0}")]
    InvalidFrame(String),

    #[error("unknown API key: {0}")]
    UnknownApiKey(u16),

    #[error("frame too short: need {needed} bytes, have {have}")]
    FrameTooShort { needed: usize, have: usize },

    #[error("Topic name is invalid (not UTF-8 or exceeds max length)")]
    InvalidTopicName,

    #[error("Message is too large to fit in a segment")]
    MessageTooLarge,

    #[error("frame exceeds maximum size of {max} bytes (got {got})")]
    FrameTooLarge { max: usize, got: usize },

    // Storage errors
    #[error("segment is full (current: {current} bytes, max: {max} bytes)")]
    SegmentFull { current: u64, max: u64 },

    #[error("offset {0} not found in any segment")]
    OffsetNotFound(u64),

    #[error("corrupted index entry at position {0}")]
    CorruptedIndex(u64),

    #[error("partition {topic}/{partition} does not exist")]
    PartitionNotFound { topic: String, partition: u32 },

    // I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    // Network errors
    #[error("connection reset by peer")]
    ConnectionReset,

    #[error("write failed: {0}")]
    WriteFailed(String),
}

/// Convenience Result alias.
pub type Result<T> = std::result::Result<T, RukaError>;
