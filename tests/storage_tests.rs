use bytes::Bytes;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::oneshot;

use ruka::broker::writer::BrokerCommand;
use ruka::storage::topic::TopicRegistry;

#[tokio::test]
async fn integration_storage_end_to_end() {
    let dir = TempDir::new().unwrap();

    // We'll use a small max_segment_bytes (e.g., 200) to force rotation
    let registry = Arc::new(TopicRegistry::new(
        dir.path(),
        200,
        1,
        ruka::config::SyncLevel::LogAndIndex,
        1024,
    ));

    let tx = registry
        .get_or_create_partition("integration_topic", 0)
        .await
        .unwrap();

    // 1. Append enough data to cause multiple segment rotations
    let mut expected_data = Vec::new();
    for i in 0..10 {
        let payload = format!("Payload {}", i);
        let bytes = Bytes::from(payload.clone());

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(BrokerCommand::Produce {
            key: None,
            value: Some(bytes),
            reply: reply_tx,
        })
        .await
        .unwrap();

        let offset = reply_rx.await.unwrap().unwrap();

        assert_eq!(offset, i as u64);
        expected_data.push(payload);
    }

    // 2. Read back and verify sequentially
    for (i, expected) in expected_data.iter().enumerate() {
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(BrokerCommand::Fetch {
            offset: i as u64,
            max_bytes: 4096,
            reply: reply_tx,
        })
        .await
        .unwrap();

        let record = reply_rx.await.unwrap().unwrap().into_iter().next().unwrap();
        assert_eq!(record.offset, i as u64);
        let val = record.value.unwrap();
        assert_eq!(String::from_utf8_lossy(&val), *expected);
    }

    // 3. Close and Reload the registry to verify persistence
    let reloaded_registry = TopicRegistry::load(
        dir.path(),
        200,
        1,
        ruka::config::SyncLevel::LogAndIndex,
        1024,
    )
    .await
    .unwrap();
    let reloaded_tx = reloaded_registry
        .get_partition("integration_topic", 0)
        .await
        .unwrap();

    // Read everything back again
    for (i, expected) in expected_data.iter().enumerate() {
        let (reply_tx, reply_rx) = oneshot::channel();
        reloaded_tx
            .send(BrokerCommand::Fetch {
                offset: i as u64,
                max_bytes: 4096,
                reply: reply_tx,
            })
            .await
            .unwrap();

        let record = reply_rx.await.unwrap().unwrap().into_iter().next().unwrap();
        assert_eq!(record.offset, i as u64);
        let val = record.value.unwrap();
        assert_eq!(String::from_utf8_lossy(&val), *expected);
    }
}
