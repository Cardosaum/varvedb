# Issue: Hardening `EventView` Unsafe Usage

## Context
In `src/engine.rs:204`, the `EventView` dereferences the archived event using `unsafe`:

```rust
unsafe { rkyv::archived_root::<E>(bytes) }
```

This assumes that the bytes on disk are always valid `rkyv` archives. While the writer ensures this, disk corruption or malicious modification could lead to Undefined Behavior (UB) when reading.

## Goal
Replace the `unsafe` block with a validated check when the `validation` feature is enabled.

## Implementation Details
- Use `rkyv::check_archived_root` instead of `archived_root`.
- Propagate any validation errors up to the caller.
- Ensure that this check is efficient (it verifies the structure of the archive).

## Acceptance Criteria
- [ ] `EventView` no longer uses `unsafe` without validation (or uses a safe wrapper).
- [ ] Invalid data on disk returns a `Validation` error instead of causing UB.
