# Issue: Share Writer Notification Channel

## Context
`Writer::new` creates a new `tokio::watch::channel` every time. If multiple writers are created, they do not share the notification mechanism, meaning subscribers to one writer won't see events appended by another.

## Goal
Move the notification channel to a shared location (e.g., `Storage` or a new `SharedState` struct) so that all writers publish to the same channel.

## Implementation Details
- Move `tx` (sender) to `Storage` (wrapped in `Arc<Mutex<...>>` or similar if needed, though `watch::Sender` is not Clone, so maybe `Arc<watch::Sender>`).
- Actually `watch::Sender` handles internal synchronization, so `Arc<watch::Sender>` in `Storage` would work.
- Update `Writer` to use the shared sender.

## Acceptance Criteria
- [ ] All `Writer` instances trigger the same subscribers.
- [ ] `Reader` or `Processor` subscribes to the shared channel.
