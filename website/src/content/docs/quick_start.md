# Quick Start

Get up and running with VarveDB in minutes.

## Installation

Add `varvedb` and `rkyv` to your `Cargo.toml`. `rkyv` is required to define your event types.

```bash
cargo add varvedb
cargo add rkyv
```

Ensure your `Cargo.toml` looks similar to this:

```toml
[dependencies]
varvedb = "0.2"
rkyv = { version = "0.8", features = ["std"] } # Ensure features match your needs
```

## Hello World

This example demonstrates how to define a custom event and metadata, append it to the database, and read it back.

```rust
use varvedb::{Varve, Payload, ExpectedVersion};
use varvedb::traits::MetadataExt;
use rkyv::{Archive, Serialize, Deserialize};

// 1. Define your Event type.
// It must derive Archive, Serialize, and Deserialize from rkyv.
// We also enable validation checks with `check_bytes`.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))]
#[rkyv(check_bytes)] // Essential for safe zero-copy deserialization
struct UserSignedUp {
    username: String,
    email: String,
}

// 2. Define your Metadata type.
// It must implement MetadataExt to provide the StreamID and Version.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))]
#[rkyv(check_bytes)]
struct EventMetadata {
    stream_id: u128, // VarveDB uses u128 for Stream IDs
    version: u32,
    timestamp: u64,
}

impl MetadataExt for EventMetadata {
    fn stream_id(&self) -> u128 { self.stream_id }
    fn version(&self) -> u32 { self.version }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 3. Initialize the database
    // This creates a directory "my_varvedb" if it doesn't exist.
    let mut varve = Varve::open("my_varvedb")?;

    // 4. Create an event and its metadata
    let event = UserSignedUp {
        username: "alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    let stream_id = 1; 
    let metadata = EventMetadata {
        stream_id,
        version: 1, // First event in the stream
        timestamp: 1678886400,
    };

    // 5. Append the event
    // ExpectedVersion::Auto will automatically increment the version,
    // but here we are explicit for demonstration.
    let global_offset = varve.append(
        Payload::new(event, metadata), 
        ExpectedVersion::Exact(1)
    )?;

    println!("Appended event at global offset: {}", global_offset);

    // 6. Read it back
    // The reader provides a zero-copy view into the data.
    let reader = varve.reader();
    let txn = varve.storage().env.read_txn()?; // Start a read transaction

    if let Some(view) = reader.get(&txn, global_offset)? {
         println!("Read event: {:?}", view.event());
         // view.event() returns a reference to the archived data (UserSignedUp)
    }

    Ok(())
}
```

## Key Concept: Zero-Copy

Notice that when we read the event back, we didn't have to "deserialize" it in the traditional sense. `rkyv` allows us to point to the data on disk (mapped into memory) and read it as if it were a normal Rust struct. This is why `UserSignedUp` derives `Archive`.

## Next Steps

*   Check out [Concepts](./concepts.md) for a deep dive into Streams and Events.
*   See [Architecture](./architecture.md) to understand the underlying storage engine.

