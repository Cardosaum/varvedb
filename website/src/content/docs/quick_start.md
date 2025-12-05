# Quick Start

Get up and running with VarveDB in minutes.

## Installation

Add `varvedb` to your `Cargo.toml`:

```bash
cargo add varvedb
```

## Hello World

Here is a simple example of how to append an event and read it back.

```rust
use varvedb::{Varve, Event};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct UserSignedUp {
    username: String,
    email: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize the database
    let varve = Varve::open("my_db").await?;
    let mut writer = varve.writer().await?;
    let reader = varve.reader()?;

    // 2. Define an event
    let event = UserSignedUp {
        username: "alice".to_string(),
        email: "alice@example.com".to_string(),
    };

    // 3. Append the event
    let offset = writer.append("users", &event).await?;
    println!("Appended event at offset: {}", offset);

    // 4. Read it back
    let read_event: UserSignedUp = reader.get("users", offset)?;
    println!("Read event: {:?}", read_event);

    Ok(())
}
```

## Next Steps

*   Check out the [Architecture](./architecture.md) to understand how VarveDB works.
*   See the [Usage](./usage.md) guide for more advanced patterns.
