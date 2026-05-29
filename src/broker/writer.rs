//! Background writer task commands.

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::error::Result;
use crate::storage::segment::Record;

/// Commands sent from the network handlers to the per-partition background writer tasks.
pub enum BrokerCommand {
    Produce {
        key: Option<Bytes>,
        value: Option<Bytes>,
        reply: oneshot::Sender<Result<u64>>,
    },
    ProduceBatch {
        payloads: Vec<Bytes>,
        reply: oneshot::Sender<Result<u64>>,
    },
    Fetch {
        offset: u64,
        max_bytes: u32,
        reply: oneshot::Sender<Result<Vec<Record>>>,
    },
}
