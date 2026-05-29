use bytes::{Buf, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use ruka::protocol::codec::RukaCodec;
use ruka::protocol::request::{FetchRequest, ProduceRequest};
use ruka::protocol::types::ApiKey;
use ruka::storage::segment::Record;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Connecting to Ruka broker at 127.0.0.1:9093...");

    // Connect to the broker
    let stream = TcpStream::connect("127.0.0.1:9093").await?;
    let mut client = Framed::new(stream, RukaCodec);

    let topic = "demo-topic".to_string();
    let partition = 0;
    let message_str = "Hello, Ruka! This is a high-performance test message.";

    // ==========================================
    // 1. PRODUCE A MESSAGE
    // ==========================================
    println!("\n📤 Sending PRODUCE request...");
    println!("   Topic: {}", topic);
    println!("   Payload: '{}'", message_str);

    let produce_req = ProduceRequest {
        correlation_id: 1,
        topic: topic.clone(),
        partition,
        payload: Bytes::from(message_str),
    };

    // Send it over the wire
    client.send(produce_req.into_frame()).await?;

    // Wait for the broker's response
    let mut committed_offset = 0;
    if let Some(Ok(frame)) = client.next().await {
        if frame.api_key == ApiKey::Produce {
            // The ProduceResponse payload contains the 8-byte big-endian offset
            let offset = u64::from_be_bytes(frame.payload[..8].try_into().unwrap());
            committed_offset = offset;
            println!("✅ Produce successful! Broker assigned Offset: {}", offset);
        }
    }

    // ==========================================
    // 2. FETCH THE MESSAGE BACK
    // ==========================================
    println!(
        "\n📥 Sending FETCH request for Offset {}...",
        committed_offset
    );

    let fetch_req = FetchRequest {
        correlation_id: 2,
        topic: topic.clone(),
        partition,
        offset: committed_offset,
        max_bytes: 4096,
    };

    // Send it over the wire
    client.send(fetch_req.into_frame()).await?;

    // Wait for the broker's response
    if let Some(Ok(frame)) = client.next().await {
        if frame.api_key == ApiKey::Fetch {
            let mut payload = frame.payload.clone();
            let num_records = payload.get_u32();

            if num_records > 0 {
                let mut record_buf = BytesMut::from(&payload[..]);
                if let Some(record) = Record::decode(&mut record_buf)? {
                    let received = record.value.unwrap_or_default();
                    let received_str = String::from_utf8_lossy(&received);
                    println!("✅ Fetch successful! Received payload:");
                    println!("   '{}'", received_str);

                    if received_str == message_str {
                        println!("\n🎉 SUCCESS: Data round-tripped perfectly through the broker!");
                    }
                }
            } else {
                println!("⚠️  Fetch returned 0 records.");
            }
        }
    }

    Ok(())
}
