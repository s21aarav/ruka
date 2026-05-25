//! Append-only segmented storage engine.
//!
//! The storage hierarchy is:
//! ```text
//! TopicRegistry
//! └── Topic ("orders")
//!     ├── Partition 0  ← Arc<RwLock<Partition>>
//!     │   ├── Segment { 00000000000000000000.log, .index }
//!     │   └── Segment { 00000000000000001024.log, .index }
//!     └── Partition 1  ← Arc<RwLock<Partition>>
//!         └── Segment { 00000000000000000000.log, .index }
//! ```
//!
//! Each partition is independently locked with an `RwLock`, ensuring that
//! operations on different partitions never block each other.

pub mod index;
pub mod partition;
pub mod segment;
pub mod topic;
