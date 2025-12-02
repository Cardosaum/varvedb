# Issue: Add Concurrency Tests

## Context
The system supports optimistic concurrency control, but there are no tests verifying behavior under high concurrent load.

## Goal
Add tests with multiple threads/tasks writing to the same stream simultaneously.

## Implementation Details
- Create a test case with N writers trying to append to the same stream.
- Verify that optimistic locking works (only one succeeds per version).
- Verify that no data is lost or corrupted.

## Acceptance Criteria
- [ ] Concurrency test suite added.
- [ ] Tests pass reliably.
