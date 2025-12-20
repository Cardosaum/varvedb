---
title: "Core Concepts"
description: "Understand the architecture, data model, and design philosophy of VarveDB."
---

VarveDB is not a traditional relational database. It is an **embedded, append-only event store** designed for high-throughput event sourcing and immutable logging.

## Architecture

VarveDB is built on three persistent components, all backed by [LMDB](http://www.lmdb.tech/doc/) (via the `heed` crate):

### 1. Global Events Database
An append-only log storing all events in the system, ordered by **GlobalSequence** (u64).

- **Key**: `GlobalSequence` (monotonically increasing u64)
- **Value**: `GlobalEventRecord` containing:
  - Stream name (string)
  - StreamId (u64)
  - StreamSequence (u64)
  - Event payload (serialized bytes)

This is the primary storage for event data. All events are written here first.

### 2. Stream Index Database
A secondary index optimizing lookups by `(StreamId, StreamSequence)`.

- **Key**: `[StreamId: u64][StreamSequence: u64]` (16 bytes, big-endian)
- **Value**: `GlobalSequence` (pointer into the Global Events DB)

This allows efficient queries like "give me event #5 from Order #1234" without scanning the entire global log.

### 3. Stream Metadata Database
Tracks the current sequence number for each StreamId within a stream.

- **Key**: `StreamId` (u64)
- **Value**: Next sequence number (u64)

This enables automatic sequence number assignment during appends.

## The Data Model

### Stream Names
Events are organized into logical **stream namespaces** by name (e.g., "orders", "users", "inventory"):

```rust
let mut orders = varve.stream::<OrderEvent, 1024>("orders")?;
let mut users = varve.stream::<UserEvent, 512>("users")?;
```

Stream names provide:
- **Type isolation**: Each stream can have a different event type.
- **Logical grouping**: Related entities are co-located.
- **Independent databases**: LMDB creates separate index/metadata DBs per stream for efficient querying.

### Stream IDs
Within a stream, events are grouped by **StreamId** (u64):

```rust
// All events for Order #1234
stream.append(StreamId(1234), &event1)?;
stream.append(StreamId(1234), &event2)?;
stream.append(StreamId(1234), &event3)?;
```

StreamIds represent individual entities:
- For an "orders" stream: StreamId = order number.
- For a "users" stream: StreamId = user ID.
- For an "inventory" stream: StreamId = product SKU.

### Sequences

Each event is identified by two sequence numbers:

1. **StreamSequence**: Position within a specific `(stream_name, stream_id)` pair (0, 1, 2...).
2. **GlobalSequence**: Position in the global event log across all streams (monotonically increasing).

Example:

| Global Seq | Stream Name | Stream ID | Stream Seq | Event |
|------------|-------------|-----------|------------|-------|
| 0 | orders | 1234 | 0 | OrderPlaced |
| 1 | users | 42 | 0 | UserCreated |
| 2 | orders | 1234 | 1 | OrderShipped |
| 3 | orders | 5678 | 0 | OrderPlaced |

### Events

Events are arbitrary Rust structs serialized with [rkyv](https://rkyv.org/):

```rust
#[derive(Archive, Serialize, Deserialize)]
struct OrderPlaced {
    amount: u64,
    currency: String,
    items: Vec<LineItem>,
}
```

**Zero-Copy Deserialization**: When reading, `rkyv` provides direct references to the memory-mapped file without allocating or copying data.

## Consistency & Concurrency

### Strict Ordering
VarveDB guarantees **strict sequential ordering** within each `(stream_name, stream_id)` pair:

- Events appended to the same StreamId are numbered sequentially (0, 1, 2...).
- Concurrent appends to *different* StreamIds are independent and do not block each other.

### Single-Writer Model
VarveDB requires `&mut self` for write operations, enforcing a **single-writer** constraint at compile time:

```rust
let mut varve = Varve::new("./data")?;
let mut stream = varve.stream::<Event, 256>("orders")?;

// Only one thread can hold a mutable reference at a time
stream.append(StreamId(1), &event)?;
```

This eliminates concurrency bugs without runtime locks.

### Multi-Reader Model
Read operations use shared references and are lock-free:

```rust
let mut reader1 = stream.reader(); // Cloneable
let mut reader2 = reader1.clone();  // Independent readers

// Both can read concurrently
let data1 = reader1.get_archived(StreamId(1), seq)?;
let data2 = reader2.get_archived(StreamId(2), seq)?;
```

Multiple readers can access the database simultaneously without blocking writes (thanks to LMDB's MVCC).

### Waiting for new writes (optional)
If you’re building projections or tailing the log in an async context, enable the `notify` feature to get a runtime-agnostic `WriteWatcher` you can await instead of polling:

```toml
varvedb = { version = "0.4", features = ["notify"] }
```

See [Async Notifications](/docs/notifications) for patterns and semantics.

## Memory-Mapped I/O

VarveDB uses LMDB's memory-mapped architecture:

### How It Works
1. The entire database file is mapped into the process's virtual address space.
2. Reading an event returns a pointer directly into this mapped region.
3. The OS manages physical memory (page cache) automatically.

### Benefits
- **Zero-copy reads**: No deserialization or buffer allocation.
- **Automatic caching**: The OS keeps frequently accessed pages in RAM.
- **Crash safety**: Memory-mapped writes are durable after `fsync`.

### Trade-offs
- **Virtual memory usage**: Your process may show high virtual memory (the entire DB is mapped), but physical RAM usage is dynamic.
- **Cold start latency**: First access to a page may incur a disk read (page fault).

## Design Philosophy

### Immutability
Events are never modified or deleted. This provides:
- **Auditability**: Complete history is preserved.
- **Replayability**: Rebuild state by replaying events.
- **Simplicity**: No delete/update logic means fewer bugs.

### Embedded Architecture
VarveDB runs in-process (no separate server):
- **Lower latency**: No network round-trips.
- **Simpler deployment**: Just a library, not a service.
- **ACID guarantees**: Full transaction support via LMDB.

### Performance-First
Every design decision prioritizes throughput:
- **Zero-copy**: `rkyv` eliminates serialization overhead.
- **Batching**: Amortize fsync cost across many events.
- **Memory-mapping**: OS page cache is faster than application-level caching.

## Limitations & Future Work

### Current Limitations
- **No replication**: VarveDB is single-node only. For distributed systems, use external tools.
- **No built-in retention**: Events are never deleted automatically. You must manage disk space externally.
- **Single writer**: Only one process can write at a time (enforced by LMDB).

### Planned Features
- **Optimistic Concurrency Control**: Support for `ExpectedVersion` to prevent concurrent write conflicts.
- **Encryption**: Optional authenticated encryption for data at rest.
- **Snapshots**: Fast snapshot exports for backups and replication.
