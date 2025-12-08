# Event Processing

The real power of an event store comes from consuming events to build read models, trigger external actions, or drive workflows. VarveDB provides a robust **Processor** framework for this.

## The Processor Pattern

A **Processor** in VarveDB is a background task that:
1.  Subscribes to the event log.
2.  Reads new events sequentially.
3.  Handles them (updates state, sends emails, etc.).
4.  **Checkpoints** its position.

If the application restarts, the Processor resumes exactly where it left off, ensuring **at-least-once** processing.

## Implementing a Handler

To create a processor, implement the `EventHandler` trait for your event type.

```rust
use varvedb::processor::EventHandler;

struct MyProjection {
    // Local state, e.g., a HashMap or a connection to a SQL DB
}

impl EventHandler<MyEvent> for MyProjection {
    fn handle(&mut self, event: &MyEvent::Archived) -> varvedb::Result<()> {
        match event {
            // Apply logic based on the event
            _ => println!("Handled event"),
        }
        Ok(())
    }
}
```

## Running the Processor

Use the `Processor` struct to run your handler.

```rust
use varvedb::processor::Processor;

// ... inside async main ...

let projection = MyProjection { ... };
let consumer_id = 100; // Unique ID for this processor to track its cursor

let mut processor = Processor::new(&varve, projection, consumer_id);

// Run in a background task
tokio::spawn(async move {
    processor.run().await.unwrap();
});
```

## Consumer Groups (Scaling)

Currently, VarveDB supports single-consumer processing per `consumer_id`. To scale processing (competing consumers), you would typically use an external message broker or partition your streams, but native support for competing consumers is on the roadmap.
