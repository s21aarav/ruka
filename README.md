# Ruka

**Kafka-inspired persistent message broker in Rust**

Ruka is a single-node persistent message broker and append-only log built entirely from scratch in Rust. It uses a custom binary wire protocol, a fully asynchronous Tokio architecture, and partition-isolated async request routing.

## Architecture

Ruka decouples the network thread pool from the disk I/O threads using Multi-Producer, Single-Consumer (MPSC) channels. Every partition has a dedicated Tokio background task for disk writes, ensuring that a slow disk flush on Partition A never blocks network requests destined for Partition B.

```text
Client -> TCP Codec -> Request Handler -> Topic Registry -> Partition Writer -> Segments + Index
```

## Protocol Format

Ruka eschews HTTP/JSON in favor of a raw binary protocol. All requests and responses are encapsulated in a `Frame`:

```text
[TotalLength: 4B][MagicByte: 1B][ApiKey: 2B][CorrelationId: 4B][TopicLen: 2B][TopicName: VarB][Partition: 4B][PayloadLen: 4B][Payload: VarB]
```

## Storage Layout

Data is stored as an append-only log with an in-memory offset index with O(log n) binary-search lookups. Segments rotate automatically based on size to prevent unbounded memory growth. 

```text
data/
  topic/
    0/
      00000000000000000000.log
      00000000000000000000.index
      0000000000000037450.log
      0000000000000037450.index
```

## Quick Demo

```text
$ cargo run --release --example client
🔌 Connecting to Ruka broker at 127.0.0.1:9093...

📤 Sending PRODUCE request...
   Topic: demo-topic
   Payload: 'Hello, Ruka! This is a high-performance test message.'
✅ Produce successful! Broker assigned Offset: 0

📥 Sending FETCH request for Offset 0...
✅ Fetch successful! Received payload:
   'Hello, Ruka! This is a high-performance test message.'

🎉 SUCCESS: Data round-tripped perfectly through the broker!
```

## Benchmarks

Single-node performance on an Apple M-series MacBook Air:

| Benchmark Type | Message Size | Batch Size | Throughput | Bandwidth |
| --- | --- | --- | --- | --- |
| Sequential Latency | 256 bytes | 1 | ~24,000 msgs/sec | ~5.5 MB/s |
| Pipelined Batching | 256 bytes | 1000 | ~77,000 msgs/sec | ~19.0 MB/s |

> Benchmarks were run with `--release`, `--sync-level none`, 256-byte payloads, on an Apple M-series MacBook Air using Rust 1.87 nightly. Results vary by machine, disk speed, and OS configuration.

## How to Run

1. **Start the Broker:**
   ```bash
   cargo run --release -- --port 9093 --data-dir ./ruka-data
   ```

2. **Run the Demo Client** (in a second terminal):
   ```bash
   cargo run --release --example client
   ```

3. **Run Sequential Benchmark:**
   ```bash
   cargo run --release --example benchmark
   ```

4. **Run Pipelined Batch Benchmark:**
   ```bash
   cargo run --release --example throughput_bench -- --messages 1000000 --batch-size 1000
   ```

## Graceful Shutdown

The server stops accepting new connections on Ctrl-C and exits the main loop. In-flight requests that have already been dispatched to partition actors will complete, but no fsync/flush of all partitions is performed on shutdown.

## Known Limitations

- **Replication/Consensus**: Ruka is currently a single-node engine without Raft or Paxos based replication.
- **Consumer Groups**: Fetching is entirely offset-based. Server-side consumer group state is not yet tracked.
- **Authentication**: No TLS or SASL is currently implemented.
- **Shutdown Flush**: Ctrl-C stops the accept loop but does not explicitly flush all partition data to disk.
