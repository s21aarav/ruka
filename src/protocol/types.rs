//! Core protocol types, constants, and enums.

use crate::error::RukaError;

/// Magic byte that prefixes every valid ruka frame.
/// 0xCA = 'CA' for CAfka.
pub const MAGIC_BYTE: u8 = 0xCA;

/// Maximum allowed frame size (16 MB).
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Minimum frame header size (before variable-length fields):
/// MagicByte(1) + ApiKey(2) + CorrelationId(4) + TopicLength(2) + Partition(4) + PayloadLength(4) = 17 bytes
pub const MIN_HEADER_SIZE: usize = 17;

/// API operation keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ApiKey {
    /// Append messages to a topic partition.
    Produce = 1,
    /// Read messages from a topic partition at a given offset.
    Fetch = 2,
    /// Append a batch of messages to a topic partition.
    ProduceBatch = 3,
    Error = 4,
}

impl TryFrom<u16> for ApiKey {
    type Error = RukaError;

    fn try_from(value: u16) -> Result<Self, RukaError> {
        match value {
            1 => Ok(ApiKey::Produce),
            2 => Ok(ApiKey::Fetch),
            3 => Ok(ApiKey::ProduceBatch),
            4 => Ok(ApiKey::Error),
            _ => Err(RukaError::UnknownApiKey(value)),
        }
    }
}

/// Represents known error codes sent from the broker to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    None = 0,
    UnknownApiKey = 1,
    InvalidFrame = 2,
    OffsetNotFound = 3,
    PartitionNotFound = 4,
    StorageError = 5,
    MessageTooLarge = 6,
    ServerError = 7,
}

impl From<u16> for ErrorCode {
    fn from(value: u16) -> Self {
        match value {
            0 => ErrorCode::None,
            1 => ErrorCode::UnknownApiKey,
            2 => ErrorCode::InvalidFrame,
            3 => ErrorCode::OffsetNotFound,
            4 => ErrorCode::PartitionNotFound,
            5 => ErrorCode::StorageError,
            6 => ErrorCode::MessageTooLarge,
            _ => ErrorCode::ServerError,
        }
    }
}

impl From<&RukaError> for ErrorCode {
    fn from(e: &RukaError) -> Self {
        match e {
            RukaError::UnknownApiKey(_) => ErrorCode::UnknownApiKey,
            RukaError::InvalidFrame(_) => ErrorCode::InvalidFrame,
            RukaError::InvalidMagicByte(_) => ErrorCode::InvalidFrame,
            RukaError::FrameTooShort { .. } => ErrorCode::InvalidFrame,
            RukaError::FrameTooLarge { .. } => ErrorCode::MessageTooLarge,
            RukaError::MessageTooLarge => ErrorCode::MessageTooLarge,
            RukaError::InvalidTopicName => ErrorCode::InvalidFrame,
            RukaError::OffsetNotFound(_) => ErrorCode::OffsetNotFound,
            RukaError::PartitionNotFound { .. } => ErrorCode::PartitionNotFound,
            RukaError::SegmentFull { .. } => ErrorCode::StorageError,
            RukaError::CorruptedIndex(_) => ErrorCode::StorageError,
            RukaError::Io(_) => ErrorCode::StorageError,
            _ => ErrorCode::ServerError,
        }
    }
}

impl ApiKey {
    /// Get the raw u16 value.
    pub fn as_u16(self) -> u16 {
        self as u16
    }
}

impl std::fmt::Display for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiKey::Produce => write!(f, "PRODUCE"),
            ApiKey::Fetch => write!(f, "FETCH"),
            ApiKey::ProduceBatch => write!(f, "PRODUCE_BATCH"),
            ApiKey::Error => write!(f, "ERROR"),
        }
    }
}
