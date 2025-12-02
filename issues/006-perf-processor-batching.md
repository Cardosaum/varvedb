# Issue: Implement Processor Batching

## Context
The `Processor` in `src/engine.rs` commits the consumer cursor transaction after *every* single event.
```rust
wtxn.commit()?;
```
This is a major performance bottleneck as fsync/commit operations are expensive.

## Goal
Implement a batching strategy for cursor updates.

## Implementation Details
- Allow the `Processor` to process `N` events or wait `T` milliseconds before committing the cursor.
- Add configuration options for `batch_size` and `batch_timeout`.

## Acceptance Criteria
- [ ] `Processor` commits cursor updates in batches.
- [ ] Throughput is significantly improved (verify with benchmarks).
