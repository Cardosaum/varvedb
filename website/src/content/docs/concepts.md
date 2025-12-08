---
title: "Core Concepts"
description: "Understand the architecture, concurrency model, and data flow of VarveDB."
---

VarveDB is not a traditional relational database. It is an **embedded, append-only event store** designed for high-throughput event sourcing.

## Architecture

VarveDB is composed of three main persistent components, all backed by [LMDB](http://www.lmdb.tech/doc/) (via the `heed` crate):

1.  **Events Log**: An append-only log accumulating all events in the system. Keys are global sequence members (u64).
2.  **Stream Index**: A map optimizing lookups by Stream ID and Version.
    *   `Key`: `[StreamID (128-bit)][Version (32-bit)]`
    *   `Value`: `GlobalSequenceNumber (64-bit)`
3.  **Blob Store**: A strictly content-addressed store for payloads larger than 1KB. This keeps the hot path (the log) small and cache-friendly.

## The Data Model

### Streams
Events are grouped into logical **Streams**. A stream represents the history of a single entity (e.g., a specific User, an Order, or a Product).
*   **Stream ID**: `u128` (UUIDs fit perfectly).
*   **Version**: `u32` monotonic counter within the stream (1, 2, 3...).

### Events
Events are arbitrary Rust structs. We use [rkyv](https://rkyv.org/) to serialize them. This allows **Zero-Copy Deserialization**, meaning we can read an event from the OS page cache without allocating heap memory for it.

```rust
// Your event is just a struct!
#[derive(Archive, Serialize, Deserialize)]
struct OrderPlaced {
    amount: u32,
    currency: String,
}
```

## Consistency & Concurrency

VarveDB guarantees **Strict Ordering** within a stream. To handle concurrent writes, it uses **Optimistic Concurrency Control (OCC)** via the `ExpectedVersion` enum.

When you append an event, you must state what version you expect the stream to be at:

*   **`ExpectedVersion::Exact(n)`**: "I believe the last event was version `n`. This new one should be `n+1`."
    *   If the current version is not `n`, the write fails with `ConcurrencyConflict`.
    *   **Use Case**: Critical logic (e.g., checking bank balance before withdrawal).
*   **`ExpectedVersion::Auto`**: "I don't care about the order, just put it at the end."
    *   **Use Case**: Logging, IoT sensor data where order matters less than throughput.

## Zero-Copy & Encryption

### Zero-Copy (Default)
When encryption is **disabled**, reading an event returns a reference to the memory-mapped file. Accessing fields is as fast as accessing raw memory.

### Authenticated Encryption (Optional)
When enabled, each stream has its own AES-256-GCM key.
*   **AAD Binding**: The Stream ID and Sequence Number are bound to the ciphertext. You cannot copy an encrypted event from Stream A to Stream B, nor can you replay an old event; validation will fail.
*   **Trade-off**: Encryption disables Zero-Copy. Data must be decrypted into a temporary buffer.
