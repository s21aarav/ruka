#![allow(clippy::zombie_processes)]
use clap::Parser;
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use bytes::Bytes;
use ruka::protocol::codec::RukaCodec;
use ruka::protocol::request::{ProduceBatchRequest, ProduceRequest};

#[derive(Parser, Debug)]
#[command(
    name = "interview_bench",
    about = "Reproducible Ruka Benchmark Harness"
)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:9092")]
    broker: String,

    #[arg(long, default_value = "./data-bench")]
    data_dir: PathBuf,

    #[arg(long, default_value = "log-and-index")]
    sync_mode: String,
}

#[derive(Serialize)]
struct LatencyResult {
    workload: String,
    requests: usize,
    payload_size: usize,
    avg_us: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
}

#[derive(Serialize)]
struct ThroughputResult {
    workload: String,
    messages: usize,
    payload_size: usize,
    pipeline_depth: usize,
    total_time_sec: f64,
    msgs_per_sec: f64,
    mb_per_sec: f64,
}

#[derive(Serialize)]
struct BenchmarkReport {
    sync_mode: String,
    latency: Vec<LatencyResult>,
    throughput: Vec<ThroughputResult>,
}

fn start_broker(data_dir: &PathBuf, sync_mode: &str) -> std::io::Result<Child> {
    // 1. Cleanup
    if data_dir.exists() {
        let _ = fs::remove_dir_all(data_dir);
    }
    fs::create_dir_all(data_dir)?;

    // 2. Build and run
    println!("Building broker in release mode...");
    let status = Command::new("cargo")
        .args(["build", "--release", "--bin", "ruka"])
        .status()?;

    if !status.success() {
        return Err(std::io::Error::other("Failed to build broker"));
    }

    println!(
        "Starting broker with sync_mode = {} and data_dir = {:?}",
        sync_mode, data_dir
    );
    let child = Command::new("cargo")
        .args([
            "run",
            "--release",
            "--bin",
            "ruka",
            "--",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--sync-level",
            sync_mode,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    // Allow time to start up
    std::thread::sleep(Duration::from_secs(3));
    Ok(child)
}

async fn run_latency_bench(broker: &str, count: usize, size: usize) -> LatencyResult {
    println!(
        "\n[Latency] Running sequential latency test (size={}, count={})...",
        size, count
    );

    let stream = TcpStream::connect(broker).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let mut client = Framed::new(stream, RukaCodec);

    let payload = Bytes::from(vec![0xBB; size]);
    let mut latencies_us = Vec::with_capacity(count);

    // Warmup
    for i in 0..1000 {
        let req = ProduceRequest {
            correlation_id: i as u32,
            topic: "bench_latency".to_string(),
            partition: 0,
            payload: payload.clone(),
        };
        client.send(req.into_frame()).await.unwrap();
        let _ = client.next().await.unwrap().unwrap();
    }

    for i in 0..count {
        let req = ProduceRequest {
            correlation_id: (i + 1000) as u32,
            topic: "bench_latency".to_string(),
            partition: 0,
            payload: payload.clone(),
        };

        let start = Instant::now();
        client.send(req.into_frame()).await.unwrap();
        let _ = client.next().await.unwrap().unwrap();
        latencies_us.push(start.elapsed().as_micros() as f64);
    }

    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let avg_us = latencies_us.iter().sum::<f64>() / count as f64;
    let p50_us = latencies_us[(count as f64 * 0.50) as usize];
    let p95_us = latencies_us[(count as f64 * 0.95) as usize];
    let p99_us = latencies_us[(count as f64 * 0.99) as usize];

    println!("  Avg RTT: {:.2} us", avg_us);
    println!("  p50 RTT: {:.2} us", p50_us);
    println!("  p95 RTT: {:.2} us", p95_us);
    println!("  p99 RTT: {:.2} us", p99_us);

    LatencyResult {
        workload: "Sequential Latency".to_string(),
        requests: count,
        payload_size: size,
        avg_us,
        p50_us,
        p95_us,
        p99_us,
    }
}

async fn run_throughput_bench(
    broker: &str,
    msgs: usize,
    size: usize,
    batch: usize,
    pipeline: usize,
) -> ThroughputResult {
    println!(
        "\n[Throughput] Running pipelined throughput test (size={}, msgs={})...",
        size, msgs
    );

    let stream = TcpStream::connect(broker).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let mut client = Framed::new(stream, RukaCodec);

    let payload = Bytes::from(vec![0xAA; size]);
    let total_batches = msgs / batch;

    let start = Instant::now();
    let mut correlation_id = 0;
    let mut in_flight = 0;
    let mut replies_received = 0;
    let mut batches_sent = 0;

    while replies_received < total_batches {
        while in_flight < pipeline && batches_sent < total_batches {
            correlation_id += 1;
            let mut payloads = Vec::with_capacity(batch);
            for _ in 0..batch {
                payloads.push(payload.clone());
            }

            let req = ProduceBatchRequest {
                correlation_id,
                topic: "bench_throughput".to_string(),
                partition: 0,
                payloads,
            };

            client.send(req.into_frame()).await.unwrap();
            batches_sent += 1;
            in_flight += 1;
        }

        let _ = client.next().await.unwrap().unwrap();
        replies_received += 1;
        in_flight -= 1;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let msgs_per_sec = msgs as f64 / elapsed;
    let mb_per_sec = (msgs * size) as f64 / elapsed / 1024.0 / 1024.0;

    println!("  Total Time: {:.2}s", elapsed);
    println!("  Throughput: {:.0} msgs/sec", msgs_per_sec);
    println!("  Bandwidth : {:.2} MB/s", mb_per_sec);

    ThroughputResult {
        workload: "Pipelined Throughput".to_string(),
        messages: msgs,
        payload_size: size,
        pipeline_depth: pipeline * batch,
        total_time_sec: elapsed,
        msgs_per_sec,
        mb_per_sec,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Start Broker Process
    let mut broker_process =
        start_broker(&args.data_dir, &args.sync_mode).expect("Broker failed to start");

    let mut report = BenchmarkReport {
        sync_mode: args.sync_mode.clone(),
        latency: Vec::new(),
        throughput: Vec::new(),
    };

    println!("\n=== Starting Ruka Interview Benchmarks ===");
    println!("Sync Mode: {}", args.sync_mode);

    // Latency tests (100k requests)
    let sizes = [16, 64, 256, 1024];
    for size in sizes {
        let lat = run_latency_bench(&args.broker, 100_000, size).await;
        report.latency.push(lat);
    }

    // Throughput tests (1M requests, pipelined)
    let sizes = [16, 64, 256, 1024];
    for size in sizes {
        // pipeline depth: 10 batches in flight, batch size 1000 = 10,000 requests in flight
        let tp = run_throughput_bench(&args.broker, 1_000_000, size, 1000, 10).await;
        report.throughput.push(tp);
    }

    println!("\n=== Saving Results ===");
    let json = serde_json::to_string_pretty(&report)?;
    let mut f_json = fs::File::create("ruka_bench_results.json")?;
    f_json.write_all(json.as_bytes())?;
    println!("Saved raw JSON to ruka_bench_results.json");

    let mut f_csv = fs::File::create("ruka_bench_results.csv")?;
    writeln!(f_csv, "Workload,Size,Requests,Metrics")?;
    for l in &report.latency {
        writeln!(
            f_csv,
            "{},{},{},Avg: {:.2}us | p99: {:.2}us",
            l.workload, l.payload_size, l.requests, l.avg_us, l.p99_us
        )?;
    }
    for t in &report.throughput {
        writeln!(
            f_csv,
            "{},{},{},Throughput: {:.0} msgs/sec | Bandwidth: {:.2} MB/s",
            t.workload, t.payload_size, t.messages, t.msgs_per_sec, t.mb_per_sec
        )?;
    }
    println!("Saved CSV summary to ruka_bench_results.csv");

    // Cleanup broker process
    println!("\nShutting down broker...");
    let _ = broker_process.kill();
    let _ = broker_process.wait();

    Ok(())
}
