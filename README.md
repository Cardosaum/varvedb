# VarveDB

VarveDB is a high-performance, embedded, append-only event store for Rust, powered by [LMDB](http://www.lmdb.tech/doc/) (via `heed`) and [rkyv](https://rkyv.org/).

It is designed for event sourcing, offering strongly-typed events, zero-copy deserialization, and optimistic concurrency control.

## Features

-   **Append-Only Log**: Immutable event history.
-   **Strongly Typed**: Events and metadata are defined by user structs.
-   **Zero-Copy**: Efficient access using `rkyv`.
-   **Optimistic Concurrency**: `ExpectedVersion` support for safer writes.
-   **Embedded**: Runs in-process, no external server required.

## Getting Started

Add `varvedb` to your `Cargo.toml`.

```toml
[dependencies]
varvedb = "0.2.1"
rkyv = "0.7" # Check version compatibility
serde = { version = "1.0", features = ["derive"] }
```

### Basic Usage

```rust
use varvedb::{Varve, Payload, ExpectedVersion};
use varvedb::traits::MetadataExt;
use rkyv::{Archive, Serialize, Deserialize};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(derive(Debug))]
#[archive(check_bytes)]
struct MyEvent {
    data: String,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(derive(Debug))]
#[archive(check_bytes)]
struct MyMetadata {
    stream_id: u128,
    version: u32,
}

impl MetadataExt for MyMetadata {
    fn stream_id(&self) -> u128 { self.stream_id }
    fn version(&self) -> u32 { self.version }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Varve::open("my_db.mdb")?;

    let event = MyEvent { data: "Hello".into() };
    let metadata = MyMetadata { stream_id: 1, version: 1 };
    
    db.append(Payload::new(event, metadata), ExpectedVersion::Auto)?;

    for result in db.iter()? {
        println!("Event: {:?}", result?);
    }

    Ok(())
}
```

## Architecture

-   **Varve**: The main facade for interacting with the database.
-   **Storage**: Manages the LMDB environment.
-   **Processor**: A framework for consuming events and tracking progress.

## License

Mozilla Public License 2.0
