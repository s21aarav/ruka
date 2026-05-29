use bytes::{Buf, BytesMut};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use ruka::config::{BrokerConfig, SyncLevel};
use ruka::network::server::start_server;
use ruka::protocol::codec::RukaCodec;
use ruka::protocol::request::{FetchRequest, ProduceRequest};
use ruka::protocol::types::ApiKey;
use ruka::storage::segment::Record;
use ruka::storage::topic::TopicRegistry;

async fn fetch_one_record(port: u16, correlation_id: u32, topic: &str, offset: u64) -> Record {
    let stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("failed to connect to server");
    let mut framed = Framed::new(stream, RukaCodec::new());

    let fetch_req = FetchRequest {
        correlation_id,
        topic: topic.to_string(),
        partition: 0,
        offset,
        max_bytes: 4096,
    };
    framed
        .send(fetch_req.into_frame())
        .await
        .expect("failed to send fetch request");

    let fetch_frame = framed
        .next()
        .await
        .expect("no fetch response received")
        .expect("failed to decode fetch response frame");

    assert_eq!(
        fetch_frame.api_key,
        ApiKey::Fetch,
        "expected Fetch response, got {:?}",
        fetch_frame.api_key
    );
    assert_eq!(fetch_frame.correlation_id, correlation_id);

    let mut fetch_payload = BytesMut::from(&fetch_frame.payload[..]);
    assert!(
        fetch_payload.len() >= 4,
        "fetch payload too short for num_records"
    );
    let num_records = fetch_payload.get_u32();
    assert!(
        num_records >= 1,
        "expected at least 1 record, got {}",
        num_records
    );

    Record::decode(&mut fetch_payload)
        .expect("failed to decode record")
        .expect("no record decoded from fetch payload")
}

#[tokio::test]
async fn tcp_produce_fetch_and_restart() {
    // 1. Create a TempDir for the data directory
    let tmp_dir = TempDir::new().expect("failed to create temp dir");

    // 2. Pick a random available port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind to port 0");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    // 3. Create a BrokerConfig with that port and the temp dir
    let config = BrokerConfig {
        port,
        host: "127.0.0.1".to_string(),
        data_dir: tmp_dir.path().to_path_buf(),
        segment_max_bytes: 10 * 1024 * 1024,
        default_partitions: 1,
        sync_level: SyncLevel::LogAndIndex,
        channel_capacity: 1024,
    };

    // 4. Create a TopicRegistry and wrap it in Arc
    let registry = Arc::new(TopicRegistry::new(
        &config.data_dir,
        config.segment_max_bytes,
        config.default_partitions,
        config.sync_level,
        config.channel_capacity,
    ));

    // 5. Spawn start_server in a background tokio task
    let server_config = config.clone();
    let server_registry = Arc::clone(&registry);
    let server_handle = tokio::spawn(async move {
        let _ = start_server(server_config, server_registry).await;
    });

    // 6. Wait briefly for the server to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 7. Connect a client using TcpStream and wrap in Framed
    let stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("failed to connect to server");
    let mut framed = Framed::new(stream, RukaCodec::new());

    // 8. Send a ProduceRequest
    let produce_req = ProduceRequest {
        correlation_id: 1,
        topic: "test-topic".to_string(),
        partition: 0,
        payload: bytes::Bytes::from("Hello, TCP!"),
    };
    framed
        .send(produce_req.into_frame())
        .await
        .expect("failed to send produce request");

    // 9. Read the produce response and assert offset 0
    let response_frame = framed
        .next()
        .await
        .expect("no response received")
        .expect("failed to decode response frame");

    assert_eq!(
        response_frame.api_key,
        ApiKey::Produce,
        "expected Produce response, got {:?}",
        response_frame.api_key
    );
    assert_eq!(response_frame.correlation_id, 1);
    let mut payload = response_frame.payload;
    assert!(payload.len() >= 8, "produce response payload too short");
    let offset = payload.get_u64();
    assert_eq!(offset, 0, "expected first produce offset to be 0");

    // 10. Fetch the same topic/partition at offset 0 before restart.
    let record = fetch_one_record(port, 2, "test-topic", 0).await;

    // 11. Assert the record's value matches "Hello, TCP!"
    assert_eq!(
        record.value,
        Some(bytes::Bytes::from("Hello, TCP!")),
        "record value mismatch"
    );
    assert_eq!(record.offset, 0);

    // 12. Drop the client connection and stop the first server.
    drop(framed);
    server_handle.abort();
    let _ = server_handle.await;

    // 13. Reload the registry from the same data directory and restart the server.
    let reloaded_registry = Arc::new(
        TopicRegistry::load(
            &config.data_dir,
            config.segment_max_bytes,
            config.default_partitions,
            config.sync_level,
            config.channel_capacity,
        )
        .await
        .expect("failed to reload topic registry"),
    );

    let restarted_config = config.clone();
    let restarted_registry = Arc::clone(&reloaded_registry);
    let restarted_handle = tokio::spawn(async move {
        let _ = start_server(restarted_config, restarted_registry).await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 14. Fetch again after restart to verify persisted data is available.
    let restarted_record = fetch_one_record(port, 3, "test-topic", 0).await;
    assert_eq!(
        restarted_record.value,
        Some(bytes::Bytes::from("Hello, TCP!")),
        "record value should survive broker restart"
    );
    assert_eq!(restarted_record.offset, 0);

    restarted_handle.abort();
    let _ = restarted_handle.await;
}
