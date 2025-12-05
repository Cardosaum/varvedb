# Example: Bank Ledger

In this tutorial, we will build a simple **Bank Ledger** application. We will track account balances by storing `Deposit` and `Withdraw` events.

## 1. Setup

Create a new Rust project:

```bash
cargo new bank_ledger
cd bank_ledger
```

Add dependencies to `Cargo.toml`:

```toml
[dependencies]
varvedb = { path = "../varvedb" } # Adjust path if needed
rkyv = "0.8"
tokio = { version = "1", features = ["full", "macros"] }
serde = { version = "1", features = ["derive"] }
anyhow = "1.0"
```

## 2. Define Events

We'll define an enum to represent our transaction events.

```rust
use rkyv::{Archive, Serialize, Deserialize};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
pub enum BankEvent {
    AccountCreated { owner: String },
    Deposited { amount: u64 },
    Withdrawn { amount: u64 },
}
```

## 3. The Application Logic

We'll create a main function that initializes the database, writes some transactions, and then processes them to calculate the final balance.

```rust
use varvedb::storage::{Storage, StorageConfig};
use varvedb::engine::{Writer, Reader, Processor, EventHandler};
use varvedb::error::Result;
use std::collections::HashMap;

// ... BankEvent definition ...

struct BalanceProjector {
    balances: HashMap<String, u64>,
}

impl EventHandler<BankEvent> for BalanceProjector {
    fn handle(&mut self, event: &ArchivedBankEvent) -> Result<()> {
        // In a real app, we'd need the stream ID (account ID) here.
        // For simplicity, let's assume a single global account for this demo,
        // or we could embed the account ID in the event.
        match event {
            ArchivedBankEvent::AccountCreated { owner } => {
                println!("New account for: {}", owner);
            }
            ArchivedBankEvent::Deposited { amount } => {
                println!("Depositing: {}", amount);
            }
            ArchivedBankEvent::Withdrawn { amount } => {
                println!("Withdrawing: {}", amount);
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Setup Storage
    let config = StorageConfig::default();
    let storage = Storage::open(config)?;
    
    let mut writer = Writer::<BankEvent>::new(storage.clone());
    let reader = Reader::<BankEvent>::new(storage.clone());

    // 2. Simulate Transactions
    let account_id = "account-1";
    
    // Create Account
    writer.append(account_id, 0, BankEvent::AccountCreated { 
        owner: "Alice".to_string() 
    })?;

    // Deposit 100
    writer.append(account_id, 1, BankEvent::Deposited { amount: 100 })?;

    // Withdraw 50
    writer.append(account_id, 2, BankEvent::Withdrawn { amount: 50 })?;

    println!("Transactions committed.");

    // 3. Process Events
    let rx = writer.subscribe();
    let handler = BalanceProjector { balances: HashMap::new() };
    
    // Create a unique consumer ID
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    "balance-projector".hash(&mut hasher);
    let consumer_id = hasher.finish();

    let mut processor = Processor::new(reader, handler, consumer_id, rx);

    // Run the processor (in a real app, this would be a background task)
    // We'll run it in a separate task and wait a bit for it to catch up
    tokio::spawn(async move {
        if let Err(e) = processor.run().await {
            eprintln!("Processor error: {}", e);
        }
    });

    // Give the processor a moment to run
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    Ok(())
}
```

## 4. Running the Example

Run the application:

```bash
cargo run
```

You should see the output reflecting the processed events.

## Conclusion

You've built a simple event-sourced application with VarveDB! You learned how to:
1.  Define custom events.
2.  Append events to a stream.
3.  Process events to build a view (projection) of the system state.
