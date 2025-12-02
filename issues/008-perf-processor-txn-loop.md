# Issue: Optimize Processor Transaction Loop

## Context
The `Processor` loop constantly opens and closes read transactions. While LMDB read transactions are cheap, doing this in a tight loop can still add overhead.

## Goal
Reuse read transactions where possible.

## Implementation Details
- Keep a read transaction open for a batch of reads.
- Reset the transaction instead of dropping and recreating it (if `heed` supports `txn.reset()` / `txn.renew()`).

## Acceptance Criteria
- [ ] Reduced overhead from transaction creation in `Processor`.
