use std::time::Instant;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use ruka::protocol::codec::RukaCodec;
use ruka::protocol::request::ProduceRequest;

const NUM_MESSAGES: u64 = 100_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting Ruka Benchmark");
    println!("   Connecting to 127.0.0.1:9093...");

    let stream = TcpStream::connect("127.0.0.1:9093").await?;
    let mut client = Framed::new(stream, RukaCodec);

    let topic = "benchmark-topic".to_string();
    let partition = 0;

    // Create a 256-byte payload to simulate a realistic small event (like JSON)
    let payload_data = vec![0u8; 256];
    let payload_bytes = Bytes::from(payload_data);

    println!(
        "   Sending {} sequential messages (256 bytes each)...",
        NUM_MESSAGES
    );

    let start = Instant::now();

    for i in 0..NUM_MESSAGES {
        let produce_req = ProduceRequest {
            correlation_id: i as u32,
            topic: topic.clone(),
            partition,
            payload: payload_bytes.clone(),
        };

        // Send request
        client.send(produce_req.into_frame()).await?;

        // Wait for acknowledgment
        if let Some(Err(e)) = client.next().await {
            eprintln!("Error receiving response: {}", e);
            break;
        }
    }

    let elapsed = start.elapsed();
    let seconds = elapsed.as_secs_f64();
    let msg_per_sec = (NUM_MESSAGES as f64) / seconds;
    let mb_per_sec = ((NUM_MESSAGES * 256) as f64) / 1_048_576.0 / seconds;

    println!("\n📊 Benchmark Results (Sequential Round-Trip)");
    println!("----------------------------------------------");
    println!("Total Messages : {}", NUM_MESSAGES);
    println!("Payload Size   : 256 bytes");
    println!("Total Time     : {:.3} seconds", seconds);
    println!("Throughput     : {:.0} msgs/sec", msg_per_sec);
    println!("Data Rate      : {:.2} MB/sec", mb_per_sec);
    println!(
        "Avg Latency    : {:.3} ms/msg",
        (seconds * 1000.0) / (NUM_MESSAGES as f64)
    );

    Ok(())
}
