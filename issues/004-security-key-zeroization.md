# Issue: Implement Key Zeroization

## Context
Encryption keys are currently held in memory as plain `[u8; 32]`. If the memory is swapped out or dumped, keys could be exposed.

## Goal
Use the `zeroize` crate to ensure keys are cleared from memory when they are dropped.

## Implementation Details
- Add `zeroize` dependency.
- Wrap keys in a type that implements `Zeroize` and `Drop`.
- Ensure `StorageConfig` handles the master key securely (e.g., using `Zeroizing<[u8; 32]>`).

## Acceptance Criteria
- [ ] Keys are zeroed out when dropped.
- [ ] `StorageConfig` uses a secure wrapper for the master key.
