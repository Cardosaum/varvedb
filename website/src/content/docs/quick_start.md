---
title: "Quick Start: Zero to Production"
description: "Get VarveDB running in your project in under 5 minutes."
---

Get VarveDB running in your Rust project in under 5 minutes. This guide will walk you through setting up a database, defining your schema, and persisting your first event.

## 1. Installation

Add `varvedb` to your `Cargo.toml`. We also recommend `rkyv` for defining your event schema.

```toml
[dependencies]
varvedb = "0.2.1"
rkyv = { version = "0.8", features = ["bytecheck", "little_endian"] }
tempfile = "3" # Optional: mostly for tests/examples
```

## 2. define Your Schema

VarveDB is schema-agnostic but relies on `rkyv` for zero-copy deserialization. Define your events as standard Rust structs.

```rust
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))]
#[repr(C)]
struct InventoryEvent {
    pub item_id: u32,
    pub action: String, // e.g., "StockAdded"
    pub quantity: u32,
}
```

> [!NOTE]
> `#[repr(C)]` and `#[rkyv(derive(Debug))]` are recommended for ensuring consistent memory layout and debugging capabilities for the zero-copy view.

## 3. The "Hello World"

Here is a complete, runnable example that opens a database, appends an event, and reads it back.

```rust
use rkyv::{Archive, Deserialize, Serialize};
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};
use tempfile::tempdir;

// 1. Define your Event (Schema)
#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))]
#[repr(C)]
struct InventoryEvent {
    item_id: u32,
    action: String,
    quantity: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 2. Setup Configuration
    // In a real app, use a persistent path like "./data/db.mdb"
    let dir = tempdir()?; 
    let config = StorageConfig {
        path: dir.path().join("inventory.mdb"),
        ..Default::default()
    };

    // 3. Initialize the Engine
    // Storage handles the LMDB environment and background threads.
    let storage = Storage::open(config)?;
    let mut writer = Writer::new(storage.clone());

    // 4. Append an Event
    // We are creating a stream for Product #1 (stream_id=1)
    // We expect this to be the 1st event in the stream (version=1)
    let event = InventoryEvent {
        item_id: 1,
        action: "StockAdded".to_string(),
        quantity: 100,
    };
    
    // append(stream_id, expected_version, event)
    let seq_num = writer.append(1, 1, event)?;
    println!("Appended event with Global Sequence Number: {}", seq_num);

    // 5. Read it back
    // The Reader provides zero-copy access to the data.
    let reader = Reader::<InventoryEvent>::new(storage.clone());
    let txn = storage.env.read_txn()?;
    
    // Fetch by Global Sequence Number
    if let Some(view) = reader.get(&txn, seq_num)? {
        // 'view' is a zero-copy pointer to the data on disk.
        // It behaves exactly like a reference to your struct.
        println!("Read Event: {:?}", view);
        assert_eq!(view.quantity, 100);
    }

    Ok(())
}
```

## Next Steps

Now that you have the basics running, explore how to build real-world applications.

*   [**Core Concepts**](/docs/concepts): Understand architecture, streams, and optimistic concurrency.
*   [**User Guides**](/docs/guides): Learn how to handle migrations, concurrency, and backups.
