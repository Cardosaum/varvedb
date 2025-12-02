# Issue: Harden Random Number Generation

## Context
`src/crypto.rs` currently uses `rand::thread_rng()`. While often secure, for a cryptographic library, it is best practice to explicitly use a Cryptographically Secure Pseudo-Random Number Generator (CSPRNG) like `OsRng`.

## Goal
Switch all random number generation to use `OsRng` or `ring::rand::SystemRandom`.

## Implementation Details
- Replace `rand::thread_rng()` with `rand::rngs::OsRng`.
- Ensure dependencies are updated if necessary.

## Acceptance Criteria
- [ ] All key and nonce generation uses `OsRng`.
- [ ] No usage of `thread_rng` in `src/crypto.rs`.
