---
title: "Quick Start: Zero to Production"
description: "Get VarveDB running in your project in under 5 minutes."
---

Get VarveDB running in your Rust project in under 5 minutes. This guide will walk you through setting up a database, defining your schema, and persisting your first event.

## 1. Installation

Add `varvedb` to your `Cargo.toml`. We also recommend `rkyv` for defining your event schema.

```toml
[dependencies]
varvedb = "0.4"
rkyv = { version = "0.8", features = ["bytecheck"] }
tempfile = "3" # Optional: for temporary test databases
```

## 2. Define Your Schema

VarveDB is schema-agnostic but relies on `rkyv` for zero-copy deserialization. Define your events as standard Rust structs.

```rust
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))]
struct OrderPlaced {
    pub order_id: u64,
    pub product: String,
    pub quantity: u32,
    pub amount: u64,
}
```

> [!NOTE]
> The `#[rkyv(derive(Debug))]` attribute enables debugging support for the archived (zero-copy) view of your event.

## 3. The "Hello World"

Here is a complete, runnable example that opens a database, appends an event, and reads it back using zero-copy access.

```rust
use rkyv::{Archive, Deserialize, Serialize};
use varvedb::{Varve, StreamId, StreamSequence};
use tempfile::tempdir;

// 1. Define your Event Schema
#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
struct OrderPlaced {
    order_id: u64,
    product: String,
    quantity: u32,
    amount: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 2. Initialize the Database
    // In production, use a persistent path like "./data/varvedb"
    let dir = tempdir()?;
    let mut varve = Varve::new(dir.path())?;

    // 3. Create a Typed Stream
    // Stream names organize related events (e.g., "orders", "users")
    // The buffer size (1024) should be larger than your largest event
    let mut stream = varve.stream::<OrderPlaced, 1024>("orders")?;

    // 4. Append an Event
    // Events are grouped by StreamId (e.g., a specific order, user, etc.)
    // Multiple events with the same StreamId form a logical stream
    let event = OrderPlaced {
        order_id: 12345,
        product: "Laptop".to_string(),
        quantity: 1,
        amount: 99900, // cents
    };
    
    let (stream_seq, global_seq) = stream.append(StreamId(1), &event)?;
    println!("✓ Appended event at stream sequence {}, global sequence {}", 
             stream_seq.0, global_seq.0);

    // 5. Read it Back (Zero-Copy)
    // Create a reader for efficient, cloneable access
    let mut reader = stream.reader();
    
    // Get the archived (zero-copy) view
    if let Some(archived_event) = reader.get_archived(StreamId(1), stream_seq)? {
        // 'archived_event' is a reference directly into the memory-mapped file
        // Access fields as if it were a normal struct reference
        println!("✓ Read event: Order #{}, Product: {}, Qty: {}", 
                 archived_event.order_id,
                 archived_event.product,
                 archived_event.quantity);
        
        assert_eq!(archived_event.order_id, 12345);
        assert_eq!(archived_event.product.as_str(), "Laptop");
    }

    // 6. Batch Append for High Throughput
    // Batching amortizes transaction overhead (700x faster!)
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

    // 7. Iterate Over a Stream
    // Read all events for a specific StreamId
    let iter = reader.iter_stream(StreamId(2), None)?;
    let events = iter.collect_bytes()?;
    println!("✓ Stream contains {} events", events.len());

    Ok(())
}
```

## Key Concepts

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

## Next Steps

Now that you have the basics running, explore how to build real-world applications:

*   [**Core Concepts**](/docs/concepts): Understand architecture, streams, and data organization.
*   [**Performance**](/docs/performance): Learn about throughput characteristics and optimization strategies.
