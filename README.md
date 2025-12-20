# VarveDB

> [!WARNING]
> **UNDER DEVELOPMENT**: This project is currently in early development and is **NOT** production ready. APIs and storage formats are subject to change.

VarveDB is a high-performance, embedded, append-only event store for Rust, powered by [LMDB](http://www.lmdb.tech/doc/) (via `heed`) and [rkyv](https://rkyv.org/).

It is designed for event sourcing, offering strongly-typed events, zero-copy deserialization, and high-throughput batch writes.

## Features

- **Zero-Copy Access**: Events are mapped directly from disk to memory using [rkyv](https://rkyv.org/), eliminating deserialization overhead for read operations.
- **High-Throughput Writes**: Achieve 1M+ events/sec with batch writes to amortize transaction overhead.
- **Embedded Architecture**: Runs in-process with your application, removing the latency and operational complexity of external database servers.
- **Strongly Typed**: Enforce schema correctness at compile time with Rust types.
- **Memory-Mapped Storage**: Leverages OS page cache for automatic memory management and high-speed access.
- **ACID Transactions**: Full crash safety and data integrity guarantees via LMDB.
- **Stream Organization**: Organize events into logical streams (e.g., "orders", "users") with efficient per-stream iteration.
- **Global Ordering**: All events receive a global sequence number for total ordering across streams.
- **Async Notifications** *(optional)*: Runtime-agnostic write notifications allow async readers to efficiently await new events without polling.

## Getting Started

Add `varvedb` to your `Cargo.toml`:

```toml
[dependencies]
varvedb = "0.4"
rkyv = { version = "0.8", features = ["bytecheck"] }
```

### Optional Features

VarveDB supports optional features that can be enabled in your `Cargo.toml`:

```toml
[dependencies]
varvedb = { version = "0.4", features = ["notify"] }
```

Available features:
- **`notify`**: Enables runtime-agnostic async notifications for write events. Allows readers to efficiently wait for new events without polling.

### Basic Usage

```rust
use rkyv::{Archive, Serialize, Deserialize};
use varvedb::{Varve, StreamId, StreamSequence};
use tempfile::tempdir;

// Define your event schema
#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
struct OrderPlaced {
    order_id: u64,
    product: String,
    quantity: u32,
    amount: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the database
    let dir = tempdir()?;
    let mut varve = Varve::new(dir.path())?;

    // Create a typed stream
    // Stream names organize related events (e.g., "orders", "users")
    // The buffer size (1024) should be larger than your largest event
    let mut stream = varve.stream::<OrderPlaced, 1024>("orders")?;

    // Append an event
    // Events are grouped by StreamId (e.g., a specific order, user, etc.)
    let event = OrderPlaced {
        order_id: 12345,
        product: "Laptop".to_string(),
        quantity: 1,
        amount: 99900, // cents
    };
    
    let (stream_seq, global_seq) = stream.append(StreamId(1), &event)?;
    println!("✓ Appended event at stream sequence {}, global sequence {}", 
             stream_seq.0, global_seq.0);

    // Read it back (zero-copy)
    let mut reader = stream.reader();
    
    if let Some(archived_event) = reader.get_archived(StreamId(1), stream_seq)? {
        // 'archived_event' is a reference directly into the memory-mapped file
        println!("✓ Read event: Order #{}, Product: {}", 
                 archived_event.order_id,
                 archived_event.product);
    }

    // Batch append for high throughput
    let more_events: Vec<OrderPlaced> = (0..100)
        .map(|i| OrderPlaced {
            order_id: 12346 + i,
            product: format!("Product-{}", i),
            quantity: 1,
            amount: 1000 * (i + 1),
        })
        .collect();
    
    let results = stream.append_batch(StreamId(2), &more_events)?;
    println!("✓ Batch appended {} events", results.len());

    // Iterate over a stream
    let iter = reader.iter_stream(StreamId(2), None)?;
    let events = iter.collect_bytes()?;
    println!("✓ Stream contains {} events", events.len());

    Ok(())
}
```

## Performance

Based on benchmarks with MacBook Pro M2 and NVMe SSD:

| Operation | Throughput | Latency |
|-----------|-----------|---------------|
| **Batch Write** (1M events) | 1.02M events/sec | **978 ns/event** |
| **Sequential Read** (1M events) | 2.28M events/sec | **438 ns/event** |
| **Stream Iterator** (8M events) | 2.92M events/sec | **342 ns/event** |

See the [Performance documentation](https://varvedb.org/docs/performance) for detailed benchmarks and optimization tips.

## Core Concepts

### Streams
Events are organized into **logical streams** by name (e.g., "orders", "users"):
- Each stream can contain multiple **StreamIds** (individual entities).
- Within a StreamId, events are ordered by **StreamSequence** (0, 1, 2...).

### Global Sequence
All events across all streams are assigned a **GlobalSequence** number, providing total ordering for replication or audit logs.

### Zero-Copy Reads
When you call `reader.get_archived()`, VarveDB returns a reference directly into the memory-mapped database file. No deserialization or allocation occurs, making reads extremely fast (<1µs).

### Batch Writes
Use `append_batch()` to write multiple events in a single transaction. This achieves **1M+ events/sec** by amortizing the fsync cost across many events.

## Architecture

VarveDB is built on three persistent components, all backed by LMDB:

1. **Global Events Database**: An append-only log storing all events ordered by GlobalSequence.
2. **Stream Index Database**: A secondary index optimizing lookups by `(StreamId, StreamSequence)`.
3. **Stream Metadata Database**: Tracks the current sequence number for each StreamId within a stream.

### Concurrency Model

- **Single-Writer**: Write operations require `&mut self`, enforcing a single-writer constraint at compile time.
- **Multi-Reader**: Read operations use shared references and are lock-free. Multiple readers can access the database simultaneously without blocking writes (thanks to LMDB's MVCC).

## API Overview

### Varve
The main entry point for database initialization:

```rust
let mut varve = Varve::new("./data")?;
let mut varve = Varve::with_config("./data", config)?;
let reader = varve.global_reader(); // Read across all streams
```

### Stream
Typed handles for appending and reading events:

```rust
let mut stream = varve.stream::<MyEvent, 1024>("stream_name")?;
let (seq, global_seq) = stream.append(StreamId(1), &event)?;
let results = stream.append_batch(StreamId(1), &events)?;
let reader = stream.reader();
```

### StreamReader
Cloneable, read-only views for efficient concurrent access:

```rust
let mut reader = stream.reader();
let archived = reader.get_archived(StreamId(1), seq)?;
let bytes = reader.get_bytes(StreamId(1), seq)?;
let iter = reader.iter_stream(StreamId(1), None)?;
```

### GlobalReader
Read events across all streams in global order:

```rust
let mut global_reader = varve.global_reader();
let event = global_reader.get(GlobalSequence(0))?;
let iter = global_reader.iter_from(GlobalSequence(0))?;
```

## Async Notifications (Optional)

When the `notify` feature is enabled, VarveDB provides runtime-agnostic write notifications that allow async readers to efficiently wait for new events:

```rust
use varvedb::{Varve, GlobalSequence};

// Enable with: varvedb = { version = "0.4", features = ["notify"] }

let mut varve = Varve::new("./data")?;
let watcher = varve.watcher();

// In an async context (works with any runtime: tokio, async-std, smol, etc.)
let mut cursor = GlobalSequence(0);
loop {
    // Try to read new events
    let reader = varve.global_reader();
    let iter = reader.iter_from(cursor)?;
    let events = iter.collect_all()?;
    
    if events.is_empty() {
        // No new events - efficiently wait for writes instead of polling
        cursor = watcher.wait_for_global_seq(cursor).await;
    } else {
        // Process events...
        for event in events {
            println!("Event at global seq {}: {:?}", event.global_seq.0, event);
            cursor = GlobalSequence(event.global_seq.0 + 1);
        }
    }
}
```

The notification system:
- **Runtime-agnostic**: Works with any async executor (Tokio, async-std, smol, etc.)
- **Zero polling overhead**: Readers sleep until writers notify them
- **In-process only**: Designed for embedded use cases where readers and writers share the same process
- **Watermark-based**: Notifications indicate committed writes, readers still query LMDB for actual data

All readers (`GlobalReader`, `StreamReader`) expose a `watcher()` method when the feature is enabled.

## Use Cases

- **Event Sourcing**: Store every state change in your application as an immutable sequence of events.
- **Audit Logging**: Tamper-proof logs for compliance and security audits.
- **Embedded Systems**: Efficient data storage for IoT devices where resources are constrained.
- **High-Frequency Data**: Capture market data or sensor readings with minimal latency.
- **Local-First Software**: Build offline-capable applications that sync when online, using the event log as the source of truth.
- **CQRS**: Separate your write model (event log) from read models (projections).

## Documentation

- **[Full Documentation](https://docs.rs/varvedb)**: Complete API reference
- **[Quick Start Guide](https://varvedb.org/docs/quick_start)**: Get up and running in 5 minutes
- **[Core Concepts](https://varvedb.org/docs/concepts)**: Understand architecture and data model
- **[Performance Guide](https://varvedb.org/docs/performance)**: Optimization strategies and benchmarks

## Requirements

- **Rust**: 1.81.0 or later
- **Platform**: Linux, macOS, Windows (LMDB support required)

## License

Mozilla Public License 2.0

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Project Status

VarveDB is under active development. The core engine is functional and benchmarked, but APIs may change before a 1.0 release.

**Current Status:**
- ✅ Core append-only log
- ✅ Zero-copy reads via rkyv
- ✅ Batch writes
- ✅ Stream organization
- ✅ Global iteration
- ✅ Comprehensive benchmark suite
- ✅ Async notifications (write watcher)

**Planned Features:**
- 🚧 Optimistic Concurrency Control (ExpectedVersion)
- 🚧 Authenticated encryption at rest
- 🚧 Snapshot exports for backups

## Links

- [GitHub Repository](https://github.com/Cardosaum/varvedb)
- [Documentation](https://docs.rs/varvedb)
- [Crates.io](https://crates.io/crates/varvedb)
