# Issue: Validate LMDB Open Parameters

## Context
In `src/storage/mod.rs:118`, the LMDB environment is opened using `unsafe`. This is standard for `heed`/LMDB, but the parameters passed (like `map_size`) should be validated against system limits to prevent runtime panics or unexpected behavior.

## Goal
Add validation logic before opening the LMDB environment.

## Implementation Details
- Check if `map_size` is reasonable for the target platform (e.g., 32-bit vs 64-bit).
- Ensure `max_dbs` is sufficient for the internal buckets plus any future extensions.

## Acceptance Criteria
- [ ] `Storage::open` returns a helpful error if `map_size` is invalid or too large for the system.
