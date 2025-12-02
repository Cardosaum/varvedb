# Issue: Document Zero-Copy vs Encryption Trade-off

## Context
When encryption is enabled, `Reader::get` must decrypt the event into a new `Vec<u8>`, which allocates memory. This negates the "zero-copy" feature promoted by `rkyv`.

## Goal
Clearly document this trade-off in `lib.rs` and `engine.rs`.

## Implementation Details
- Update `Reader::get` documentation to state that zero-copy is only possible when encryption is disabled.
- Update `lib.rs` feature list to reflect this nuance.

## Acceptance Criteria
- [ ] Documentation clearly explains that encryption implies allocation (no zero-copy).
