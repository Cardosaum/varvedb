# VarveDB

[![CI](https://github.com/Cardosaum/varvedb/actions/workflows/ci.yml/badge.svg)](https://github.com/Cardosaum/varvedb/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/varvedb.svg)](https://crates.io/crates/varvedb)
[![Docs.rs](https://docs.rs/varvedb/badge.svg)](https://docs.rs/varvedb)
[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

A high-performance, embedded, append-only event store for Rust.

VarveDB provides a persistent, ACID-compliant event log optimized for high-throughput event sourcing. It leverages **LMDB** for reliable storage and **rkyv** for zero-copy deserialization, ensuring minimal overhead.

## Features

*   **Zero-Copy Access**: Events are mapped directly from disk to memory.
*   **ACID Transactions**: Atomic, Consistent, Isolated, and Durable writes.
*   **Optimistic Concurrency**: Stream versioning prevents race conditions.
*   **Reactive Interface**: Real-time event subscriptions via `tokio::watch`.
*   **Authenticated Encryption**: Optional AES-256-GCM encryption with AAD binding.
*   **GDPR Compliance**: Crypto-shredding support via key deletion.

## Installation

```toml
[dependencies]
varvedb = { path = "." } # Or git URL
```

## Usage

### Basic Operation

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
        master_key: Some([0u8; 32]), // Secure 32-byte key
        ..Default::default()
    };
    let storage = Storage::open(config)?;
    
    let mut writer = Writer::<MyEvent>::new(storage.clone());
    let reader = Reader::<MyEvent>::new(storage.clone());

    // Append
    writer.append(1, 1, MyEvent { id: 1, data: "Hello".to_string() })?;

    // Read
    let txn = storage.env.read_txn()?;
    if let Some(event) = reader.get(&txn, 1)? {
        println!("Read event: {:?}", event);
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

## Security

### Encryption at Rest
VarveDB supports optional encryption at rest using **AES-256-GCM**.
*   **Key Wrapping**: Per-stream keys are encrypted with a provided `master_key`.
*   **AAD Binding**: Encryption is bound to `StreamID` + `Sequence` to prevent replay attacks.
*   **Stream ID Leakage**: Stream IDs are stored in plaintext for indexing efficiency.

## License

MPL-2.0
