# Core Concepts

Understanding the fundamental building blocks of VarveDB.

## The Event Log

At its heart, VarveDB is an **append-only log** of events. This means:
*   **Immutability**: Once an event is written, it cannot be changed.
*   **Ordering**: Events are strictly ordered by a global sequence number.
*   **Durability**: Events are persisted to disk immediately.

## Streams

While the log is global, applications usually care about specific entities (e.g., a specific User or Order).
A **Stream** is a logical subsequence of the log events that share the same `StreamID`.

*   **StreamID**: A `u128` identifier.
*   **Version**: Each event in a stream has a monotonically increasing version number (1, 2, 3...).

This allows you to reconstruct the state of a single entity by replaying only its stream.

## Events

An **Event** is a domain-specific fact that happened in the past. In VarveDB, events are strongly typed Rust structs.

```rust
#[derive(Archive, Serialize, Deserialize)]
struct OrderPlaced {
    order_id: Uuid,
    amount: Decimal,
}
```

## Metadata

Every event carries **Metadata**. This is where you store context separate from the domain data:
*   Stream ID (Required)
*   Stream Version (Required)
*   Timestamp
*   Correlation ID
*   User ID

## The Engine

The **Engine** (accessible via the `Varve` struct) orchestrates everything:
1.  Accepts new events.
2.  Assigns global sequence numbers.
3.  Updates the Stream Index (mapping StreamID -> List of Event Log offsets).
4.  Notifies subscribers.
