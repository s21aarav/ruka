use clap::Parser;
use futures::{SinkExt, StreamExt};
use std::time::Instant;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use bytes::Bytes;
use ruka::protocol::codec::RukaCodec;
use ruka::protocol::request::ProduceBatchRequest;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "High-throughput pipelined batch benchmark client"
)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:9093")]
    broker: String,

    #[arg(short, long, default_value = "throughput_topic")]
    topic: String,

    #[arg(short, long, default_value = "0")]
    partition: u32,

    #[arg(short, long, default_value = "1000000")]
    messages: usize,

    #[arg(short, long, default_value = "1000")]
    batch_size: usize,

    #[arg(long, default_value = "10")]
    max_in_flight: usize,

    #[arg(short, long, default_value = "256")]
    payload_size: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Connecting to {}...", args.broker);
    let stream = TcpStream::connect(&args.broker).await?;
    stream.set_nodelay(true)?;

    let mut client = Framed::new(stream, RukaCodec);

    let payload = Bytes::from(vec![0xAA; args.payload_size]);
    let total_batches = args.messages / args.batch_size;

    println!(
        "Sending {} messages ({} batches of size {})...",
        args.messages, total_batches, args.batch_size
    );
    println!("Max in-flight batches: {}", args.max_in_flight);

    let start = Instant::now();
    let mut correlation_id = 0;

    let mut in_flight = 0;
    let mut replies_received = 0;
    let mut batches_sent = 0;

    while replies_received < total_batches {
        // Pipeline: send as many batches as allowed before waiting for replies
        while in_flight < args.max_in_flight && batches_sent < total_batches {
            correlation_id += 1;
            let mut payloads = Vec::with_capacity(args.batch_size);
            for _ in 0..args.batch_size {
                payloads.push(payload.clone());
            }

            let produce_req = ProduceBatchRequest {
                correlation_id,
                topic: args.topic.clone(),
                partition: args.partition,
                payloads,
            };

            client.send(produce_req.into_frame()).await?;
            batches_sent += 1;
            in_flight += 1;
        }

        // Wait for one reply to free up an in-flight slot
        let _resp_frame = client.next().await.unwrap()?;
        replies_received += 1;
        in_flight -= 1;
    }

    let elapsed = start.elapsed();
    let msgs_per_sec = (args.messages as f64 / elapsed.as_secs_f64()) as u64;
    let mb_per_sec =
        (args.messages * args.payload_size) as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0;

    println!("\n--- Pipelined Batch Benchmark Results ---");
    println!("Total Messages : {}", args.messages);
    println!("Batch Size     : {}", args.batch_size);
    println!("Payload Size   : {} bytes", args.payload_size);
    println!("Total Time     : {:.2}s", elapsed.as_secs_f64());
    println!("Throughput     : {} msgs/sec", msgs_per_sec);
    println!("Bandwidth      : {:.2} MB/s", mb_per_sec);

    Ok(())
}
