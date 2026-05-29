//! Topic registry and partition actor management.
//!
//! The registry maps topic/partition pairs to bounded MPSC channels.
//! Each partition is owned by a dedicated background task, so writes to
//! one partition do not block writes to another partition.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use tokio::sync::{mpsc, RwLock};

use crate::broker::writer::BrokerCommand;

use crate::error::{Result, RukaError};
use crate::storage::partition::Partition;

/// Registry of all active topics and their partitions.
#[allow(dead_code)]
pub struct TopicRegistry {
    base_dir: PathBuf,
    max_segment_bytes: u64,
    default_partitions: u32,
    sync_level: crate::config::SyncLevel,
    channel_capacity: usize,

    // Map: TopicName -> Map<PartitionId -> mpsc::Sender<BrokerCommand>>
    topics: RwLock<HashMap<String, HashMap<u32, mpsc::Sender<BrokerCommand>>>>,
}

impl TopicRegistry {
    /// Create a new topic registry managing data at `base_dir`.
    pub fn new(
        base_dir: impl AsRef<Path>,
        max_segment_bytes: u64,
        default_partitions: u32,
        sync_level: crate::config::SyncLevel,
        channel_capacity: usize,
    ) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            max_segment_bytes,
            default_partitions,
            sync_level,
            channel_capacity,
            topics: RwLock::new(HashMap::new()),
        }
    }

    /// Load existing topics and partitions from disk.
    pub async fn load(
        base_dir: impl AsRef<Path>,
        max_segment_bytes: u64,
        default_partitions: u32,
        sync_level: crate::config::SyncLevel,
        channel_capacity: usize,
    ) -> Result<Self> {
        let registry = Self::new(
            base_dir,
            max_segment_bytes,
            default_partitions,
            sync_level,
            channel_capacity,
        );

        let path = registry.base_dir.clone();
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
            return Ok(registry);
        }

        let mut topics_map = HashMap::new();

        for topic_entry in std::fs::read_dir(&path)? {
            let topic_entry = topic_entry?;
            let topic_path = topic_entry.path();

            if topic_path.is_dir() {
                let topic_name = topic_entry.file_name().to_string_lossy().to_string();
                let mut partition_map = HashMap::new();

                for part_entry in std::fs::read_dir(&topic_path)? {
                    let part_entry = part_entry?;
                    if part_entry.path().is_dir() {
                        let part_id_str = part_entry.file_name().to_string_lossy().to_string();
                        if let Ok(part_id) = part_id_str.parse::<u32>() {
                            let mut partition = Partition::open_or_create(
                                topic_name.clone(),
                                part_id,
                                &registry.base_dir,
                                registry.max_segment_bytes,
                                registry.sync_level,
                            )?;

                            let (tx, mut rx) =
                                mpsc::channel::<BrokerCommand>(registry.channel_capacity);

                            tokio::spawn(async move {
                                while let Some(cmd) = rx.recv().await {
                                    match cmd {
                                        BrokerCommand::Produce { key, value, reply } => {
                                            let res = partition.append(key, value);
                                            let _ = reply.send(res);
                                        }
                                        BrokerCommand::ProduceBatch { payloads, reply } => {
                                            let res = partition.append_batch(payloads);
                                            let _ = reply.send(res);
                                        }
                                        BrokerCommand::Fetch {
                                            offset,
                                            max_bytes,
                                            reply,
                                        } => {
                                            let res = partition.read_batch(offset, max_bytes);
                                            let _ = reply.send(res);
                                        }
                                    }
                                }
                            });

                            partition_map.insert(part_id, tx);
                        }
                    }
                }

                topics_map.insert(topic_name, partition_map);
            }
        }

        *registry.topics.write().await = topics_map;

        Ok(registry)
    }

    /// Get a reference to a specific partition, creating it and its topic if necessary.
    pub async fn get_or_create_partition(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Result<mpsc::Sender<BrokerCommand>> {
        // Fast path: try to get it with only a read lock
        {
            let map = self.topics.read().await;
            if let Some(partitions) = map.get(topic) {
                if let Some(tx) = partitions.get(&partition_id) {
                    return Ok(tx.clone());
                }
            }
        }

        // Slow path: acquire write lock
        let mut map = self.topics.write().await;

        // Re-check after acquiring write lock (double-checked locking)
        let partitions = map.entry(topic.to_string()).or_insert_with(HashMap::new);

        if let Some(tx) = partitions.get(&partition_id) {
            return Ok(tx.clone());
        }

        // Create the partition
        let mut partition = Partition::open_or_create(
            topic.to_string(),
            partition_id,
            &self.base_dir,
            self.max_segment_bytes,
            self.sync_level,
        )?;

        let (tx, mut rx) = mpsc::channel::<BrokerCommand>(self.channel_capacity);

        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    BrokerCommand::Produce { key, value, reply } => {
                        let res = partition.append(key, value);
                        let _ = reply.send(res);
                    }
                    BrokerCommand::ProduceBatch { payloads, reply } => {
                        let res = partition.append_batch(payloads);
                        let _ = reply.send(res);
                    }
                    BrokerCommand::Fetch {
                        offset,
                        max_bytes,
                        reply,
                    } => {
                        let res = partition.read_batch(offset, max_bytes);
                        let _ = reply.send(res);
                    }
                }
            }
        });

        partitions.insert(partition_id, tx.clone());

        Ok(tx)
    }

    /// Get a reference to a specific partition, returning an error if it doesn't exist.
    pub async fn get_partition(
        &self,
        topic: &str,
        partition_id: u32,
    ) -> Result<mpsc::Sender<BrokerCommand>> {
        let map = self.topics.read().await;
        if let Some(partitions) = map.get(topic) {
            if let Some(tx) = partitions.get(&partition_id) {
                return Ok(tx.clone());
            }
        }

        Err(RukaError::PartitionNotFound {
            topic: topic.to_string(),
            partition: partition_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    use crate::broker::writer::BrokerCommand;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn registry_get_or_create() {
        let dir = TempDir::new().unwrap();
        let registry = TopicRegistry::new(
            dir.path(),
            1024,
            1,
            crate::config::SyncLevel::LogAndIndex,
            1024,
        );

        let tx = registry.get_or_create_partition("topic1", 0).await.unwrap();

        // Use the partition via MPSC
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(BrokerCommand::Produce {
            key: None,
            value: Some(Bytes::from_static(b"hello")),
            reply: reply_tx,
        })
        .await
        .unwrap();

        let offset = reply_rx.await.unwrap().unwrap();
        assert_eq!(offset, 0);

        // Get it again, should be the same
        let tx2 = registry.get_partition("topic1", 0).await.unwrap();
        let (reply_tx2, reply_rx2) = oneshot::channel();
        tx2.send(BrokerCommand::Fetch {
            offset: 0,
            max_bytes: 4096,
            reply: reply_tx2,
        })
        .await
        .unwrap();

        let rec = reply_rx2
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(rec.value, Some(Bytes::from_static(b"hello")));
    }

    #[tokio::test]
    async fn registry_concurrent_access() {
        let dir = TempDir::new().unwrap();
        let registry = Arc::new(TopicRegistry::new(
            dir.path(),
            1024,
            1,
            crate::config::SyncLevel::LogAndIndex,
            1024,
        ));

        // Create two partitions
        registry.get_or_create_partition("t1", 0).await.unwrap();
        registry.get_or_create_partition("t1", 1).await.unwrap();

        let r1 = Arc::clone(&registry);
        let t1 = tokio::spawn(async move {
            let tx0 = r1.get_partition("t1", 0).await.unwrap();
            let (reply_tx, reply_rx) = oneshot::channel();
            tx0.send(BrokerCommand::Produce {
                key: None,
                value: Some(Bytes::from_static(b"p0_data")),
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        });

        let r2 = Arc::clone(&registry);
        let t2 = tokio::spawn(async move {
            let tx1 = r2.get_partition("t1", 1).await.unwrap();
            let (reply_tx, reply_rx) = oneshot::channel();
            tx1.send(BrokerCommand::Produce {
                key: None,
                value: Some(Bytes::from_static(b"p1_data")),
                reply: reply_tx,
            })
            .await
            .unwrap();
            reply_rx.await.unwrap().unwrap();
        });

        tokio::try_join!(t1, t2).unwrap();

        // Verify data
        let tx0 = registry.get_partition("t1", 0).await.unwrap();
        let (r_tx0, r_rx0) = oneshot::channel();
        tx0.send(BrokerCommand::Fetch {
            offset: 0,
            max_bytes: 4096,
            reply: r_tx0,
        })
        .await
        .unwrap();
        assert_eq!(
            r_rx0
                .await
                .unwrap()
                .unwrap()
                .into_iter()
                .next()
                .unwrap()
                .value
                .unwrap(),
            "p0_data"
        );

        let tx1 = registry.get_partition("t1", 1).await.unwrap();
        let (r_tx1, r_rx1) = oneshot::channel();
        tx1.send(BrokerCommand::Fetch {
            offset: 0,
            max_bytes: 4096,
            reply: r_tx1,
        })
        .await
        .unwrap();
        assert_eq!(
            r_rx1
                .await
                .unwrap()
                .unwrap()
                .into_iter()
                .next()
                .unwrap()
                .value
                .unwrap(),
            "p1_data"
        );
    }
}
