> [!WARNING]
> **UNDER DEVELOPMENT**: This project is currently in early development and is **NOT** production ready. APIs and storage formats are subject to change.

# Introduction

**VarveDB** is a high-performance, embedded, append-only event store for Rust. It provides a persistent, ACID-compliant event log optimized for high-throughput event sourcing applications.

Built on **LMDB** (Lightning Memory-Mapped Database) and **rkyv**, VarveDB is designed for scenarios where performance and data integrity are paramount, offering zero-copy access to data and strict concurrency controls.

## Key Features

*   **Zero-Copy Access**: Events are mapped directly from disk to memory using [rkyv](https://rkyv.org/), eliminating deserialization overhead for read operations.
*   **High-Throughput Writes**: Achieve 1M+ events/sec by batching writes to amortize transaction overhead.
*   **Embedded Architecture**: Runs in-process with your application, removing the latency and operational complexity of external database servers.
*   **Strongly Typed**: Enforce schema correctness at compile time with Rust types.
*   **Memory-Mapped Storage**: Leverages OS page cache for automatic memory management and high-speed access.
*   **ACID Transactions**: Full crash safety and data integrity guarantees via LMDB.

## Why VarveDB?

Traditional databases often struggle with the specific requirements of event sourcing—immutable logs, global ordering, and replayability. External event stores like Kafka or EventStoreDB introduce network latency and operational overhead that may be unnecessary for single-node or edge applications.

VarveDB bridges this gap by providing:

1.  **Speed**: By memory-mapping the database file, reading an event is as fast as reading from RAM (sub-microsecond latency).
2.  **Simplicity**: No external clusters to manage. Just include it as a crate.
3.  **Safety**: ACID transactions ensure that your event log is never corrupted, even in the event of power failure.
4.  **Performance**: Batch writes deliver 1M+ events/sec throughput on standard hardware.

## Use Cases

*   **Event Sourcing**: Store every state change in your application as an immutable sequence of events.
*   **Audit Logging**: Tamper-proof logs for compliance and security audits.
*   **Embedded Systems**: Efficient data storage for IoT devices where resources are constrained.
*   **High-Frequency Data**: Capture market data or sensor readings with minimal latency.
*   **Local-First Software**: Build offline-capable applications that sync when online, using the event log as the source of truth.
*   **CQRS**: Separate your write model (event log) from read models (projections).

## Core Concepts

*   **Varve**: The main entry point to the database, handling database initialization and stream creation.
*   **Stream**: A typed handle for appending and reading events of a specific type. Multiple streams can coexist (e.g., "orders", "users", "inventory").
*   **StreamId**: A u64 identifier for a specific entity within a stream (e.g., Order #1234).
*   **Event**: A user-defined struct containing domain data. Must be serializable via `rkyv`.
*   **Sequences**: Events are identified by both StreamSequence (within a stream) and GlobalSequence (across all streams).
*   **Reader**: A cloneable, read-only view for efficient concurrent access.

## Getting Started

```rust
use varvedb::{Varve, StreamId};

// Initialize database
let mut varve = Varve::new("./data")?;

// Create a typed stream
let mut stream = varve.stream::<MyEvent, 256>("events")?;

// Append events
stream.append(StreamId(1), &event)?;

// Read with zero-copy
let reader = stream.reader();
let archived = reader.get_archived(StreamId(1), seq)?;
```

Continue to [Quick Start](/docs/quick_start) for a complete, runnable example.

## Performance Highlights

Based on benchmarks with MacBook Pro M2 and NVMe SSD:

| Operation | Throughput | Latency |
|-----------|-----------|---------------|
| **Batch Write** (1M events) | 1.02M events/sec | **978 ns/event** |
| **Sequential Read** (1M events) | 2.28M events/sec | **438 ns/event** |
| **Stream Iterator** (8M events) | 2.92M events/sec | **342 ns/event** |

See [Performance](/docs/performance) for detailed benchmarks and optimization tips.

## Project Status

VarveDB is under active development. The core engine is functional and benchmarked, but APIs may change before a 1.0 release.

**Current Status:**
- ✅ Core append-only log
- ✅ Zero-copy reads via rkyv
- ✅ Batch writes
- ✅ Stream organization
- ✅ Global iteration
- ✅ Comprehensive benchmark suite

**Planned Features:**
- 🚧 Optimistic Concurrency Control (ExpectedVersion)
- 🚧 Authenticated encryption at rest
- 🚧 Snapshot exports for backups
- 🚧 Reactive subscriptions (watch channels)

We welcome feedback and contributions! See the [GitHub repository](https://github.com/Cardosaum/varvedb) for more information.
