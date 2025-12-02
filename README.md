# VarveDB

VarveDB is a high-performance, embedded, append-only event store written in Rust. It leverages **LMDB** (Lightning Memory-Mapped Database) for storage and **rkyv** for zero-copy deserialization, making it incredibly fast and memory-efficient.

## Features

-   **Zero-Copy Reads**: Events are accessed directly from memory-mapped files without heap allocation using `rkyv`.
-   **ACID Transactions**: Fully ACID compliant writes and reads via LMDB.
-   **Reactive Bus**: Real-time event subscriptions using `tokio::watch`.
-   **Concurrency Control**: Optimistic concurrency control using stream versioning.
-   **Crypto-Shredding**: Built-in key management for per-stream encryption, enabling GDPR-compliant data deletion.
-   **Observability**: Integrated Prometheus metrics for monitoring throughput and latency.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
varvedb = { path = "." } # Or git URL
```

## Usage

### Basic Write and Read

```rust
use varvedb::storage::{Storage, StorageConfig};
use varvedb::engine::{Writer, Reader};
use rkyv::{Archive, Serialize, Deserialize};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
pub struct MyEvent {
    pub id: u32,
    pub data: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = StorageConfig {
        encryption_enabled: true,
        master_key: Some([0u8; 32]), // Provide a 32-byte master key
        ..Default::default()
    };
    let storage = Storage::open(config)?;
    
    let mut writer = Writer::<MyEvent>::new(storage.clone());
    let reader = Reader::<MyEvent>::new(storage.clone());

    // Append Event
    let event = MyEvent { id: 1, data: "Hello".to_string() };
    writer.append(1, 1, event)?;

    // Read Event
    let txn = storage.env.read_txn()?;
    if let Some(archived_event) = reader.get(&txn, 1)? {
        println!("Read event: {:?}", archived_event);
    }

    Ok(())
}
```

### Reactive Processing

```rust
use varvedb::engine::{Processor, EventHandler};

struct MyHandler;
impl EventHandler<MyEvent> for MyHandler {
    fn handle(&mut self, event: &ArchivedMyEvent) -> varvedb::error::Result<()> {
        println!("Processing: {:?}", event);
        Ok(())
    }
}

// ... inside async context
let rx = writer.subscribe();
let mut processor = Processor::new(reader, MyHandler, "consumer_group_1", rx);
processor.run().await?;
```

## Architecture

```mermaid
graph TD
    User[User Application]
    subgraph VarveDB
        Writer[Writer<E>]
        Reader[Reader<E>]
        Processor[Processor]
        
        subgraph Storage[LMDB Environment]
            Events[events_log (Seq -> Bytes)]
            Index[stream_index (StreamID+Ver -> Seq)]
            Cursors[consumer_cursors (Name -> Seq)]
            KeyStore[keystore (StreamID -> Key)]
        end
        
        Bus[Tokio Watch Bus]
    end

    User -->|Append| Writer
    User -->|Read| Reader
    User -->|Subscribe| Processor
    
    Writer -->|Write| Events
    Writer -->|Write| Index
    Writer -->|Notify| Bus
    
    Reader -->|Zero-Copy Read| Events
    
    Processor -->|Listen| Bus
    Processor -->|Load/Save| Cursors
    Processor -->|Handle| User
```

-   **Storage Engine**: LMDB (via `heed` crate).
-   **Serialization**: `rkyv` (guaranteed zero-copy).
-   **Async Runtime**: `tokio`.

## Security

### Encryption at Rest
VarveDB supports encryption at rest using AES-256-GCM.
To enable it, set `encryption_enabled: true` in `StorageConfig` and provide a 32-byte `master_key`.

The `master_key` is used to encrypt the per-stream keys stored in the database. This ensures that even if the database file is compromised, the data remains secure as long as the master key is protected.

### Stream ID Leakage
Note that while the event payload is encrypted, the **Stream ID** is currently stored in plaintext in the event header to allow for efficient indexing. This means an attacker with access to the raw database can see which streams are active and the volume of data they produce, but cannot read the content.

## License

MIT
