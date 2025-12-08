---
title: "Handling Concurrency"
description: "How to use Optimistic Concurrency Control to prevent race conditions."
---

In a distributed system, multiple processes often try to modify the same entity simultaneously. Without protection, this leads to **Race Conditions**.

## The Scenario: Overselling Inventory

Imagine an E-commerce system.
1.  **Product A** has 1 item left in stock.
2.  **User 1** adds it to cart vs **User 2** adds it to cart.
3.  Both check stock -> Both see "1 item".
4.  Both checkout -> **Both buy it.**
5.  Result: **-1 Stock** (Overselling).

## The Solution: ExpectedVersion

VarveDB solves this with **Optimistic Concurrency Control (OCC)**. You don't lock the table (pessimistic); instead, you "optimistically" assume you are the only one writing, but you enforce a check at the moment of writing.

### Step 1: Read the current state
First, you must read the stream to know its current version.

```rust
// Assume we have replayed the stream and know:
let current_version = 10;
let current_stock = 1;
```

### Step 2: Attempt to Write with `ExpectedVersion::Exact`

When you append the "StockReserved" event, you **must** specify that you expect the stream to be at version 10.

```rust
let event = InventoryEvent {
    item_id: 1,
    action: "StockReserved".to_string(),
    quantity: 1,
};

// We explicitly state: "This append is only valid if the stream is currently at version 10"
// The new event will be version 11.
let result = writer.append(1, ExpectedVersion::Exact(current_version), event);
```

### Step 3: Handle the Conflict

If User 2 beat User 1 by milliseconds, the stream version will already be 11. User 1's write (expecting 10) will fail.

```rust
match result {
    Ok(seq) => println!("Success! Stock reserved."),
    Err(varvedb::Error::ConcurrencyConflict { stream_id, version }) => {
        // The version changed under our feet!
        // 1. Re-read the stream to get the new state (maybe stock is now 0).
        // 2. Retry the operation or inform the user.
        println!("Conflict! Someone else modified the stream.");
    }
    Err(e) => eprintln!("System error: {}", e),
}
```

This pattern ensures that **event history is linear and consistent** without requiring heavy database locks.
