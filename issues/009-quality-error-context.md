# Issue: Improve Error Handling Context

## Context
`src/error.rs` defines `Serialization(String)` and `Validation(String)`. While functional, they lack structured context.

## Goal
Enhance error types to provide more specific information.

## Implementation Details
- Use specific error variants for common failures (e.g., `StreamNotFound`, `VersionMismatch`).
- Include relevant data in the error (e.g., `stream_id` in `StreamNotFound`).

## Acceptance Criteria
- [ ] Errors provide structured data for programmatic handling.
- [ ] Error messages are descriptive and helpful for debugging.
