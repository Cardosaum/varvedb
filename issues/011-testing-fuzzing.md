# Issue: Add Fuzz Testing

## Context
The system relies on `rkyv` for zero-copy deserialization. If invalid data is written (e.g., via bit rot), reading it could cause crashes or UB.
Currently, there are no fuzz tests implemented.

## Goal
Add fuzz tests to ensure the system handles arbitrary input gracefully.

## Implementation Details
- Use `cargo-fuzz`.
- Fuzz the `Reader::get` method with random bytes in the storage.
- Fuzz the `Writer::append` method with random inputs.

## Acceptance Criteria
- [x] Fuzz targets defined.
- [x] Fuzzing runs without crashing (panics are acceptable if safe, but no UB).
