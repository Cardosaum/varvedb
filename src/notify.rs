// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Write notification support for async readers.
//!
//! This module provides runtime-agnostic notification primitives that allow
//! async readers to efficiently wait for new writes without polling.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use event_listener::{Event, EventListener};

use crate::types::GlobalSequence;

/// Shared state for write notifications.
#[derive(Debug)]
pub(crate) struct WriteWatcherInner {
    /// The next global sequence that has been committed (watermark).
    /// This is only updated AFTER a successful commit.
    committed_next_global_seq: AtomicU64,
    /// Event for waking listeners.
    event: Event,
}

impl WriteWatcherInner {
    pub(crate) fn new(initial_next_seq: GlobalSequence) -> Self {
        Self {
            committed_next_global_seq: AtomicU64::new(initial_next_seq.0),
            event: Event::new(),
        }
    }

    /// Get the current committed watermark.
    pub(crate) fn committed_next_global_seq(&self) -> GlobalSequence {
        GlobalSequence(self.committed_next_global_seq.load(Ordering::Acquire))
    }

    /// Notify that new events have been committed.
    ///
    /// This should be called AFTER a successful commit, with the new watermark value.
    pub(crate) fn notify(&self, new_next_seq: GlobalSequence) {
        // Ensure watermark is monotonic even if notifications arrive out of order.
        self.committed_next_global_seq
            .fetch_max(new_next_seq.0, Ordering::AcqRel);
        self.event.notify(usize::MAX); // Wake all listeners
    }

    /// Create a listener for waiting on new commits.
    pub(crate) fn listen(&self) -> EventListener {
        self.event.listen()
    }
}

/// A cloneable handle for waiting on write notifications.
///
/// This allows async readers to efficiently wait for new events to be committed
/// without polling. The watcher is runtime-agnostic and works with any async executor.
///
/// # Example
///
/// ```ignore
/// use varvedb::{Varve, GlobalSequence};
///
/// let varve = Varve::new("./data")?;
/// let watcher = varve.watcher();
///
/// // In an async context:
/// let mut cursor = GlobalSequence(0);
/// loop {
///     // Try to read events...
///     // If no new events, wait for a write notification
///     cursor = watcher.wait_for_global_seq(cursor).await;
/// }
/// ```
#[derive(Debug, Clone)]
pub struct WriteWatcher {
    inner: Arc<WriteWatcherInner>,
}

impl WriteWatcher {
    /// Create a new write watcher with the given initial watermark.
    pub(crate) fn new(initial_next_seq: GlobalSequence) -> Self {
        Self {
            inner: Arc::new(WriteWatcherInner::new(initial_next_seq)),
        }
    }

    /// Get the current committed next global sequence (watermark).
    ///
    /// This represents the next global sequence that will be assigned.
    /// If this value is `N`, then events with global sequences `0..N` have been committed.
    pub fn committed_next_global_seq(&self) -> GlobalSequence {
        self.inner.committed_next_global_seq()
    }

    /// Wait until the committed watermark is greater than `from`.
    ///
    /// This async function will return when new events have been committed such that
    /// `committed_next_global_seq() > from`.
    ///
    /// Returns the new committed watermark value.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Wait for any events after sequence 100
    /// let new_watermark = watcher.wait_for_global_seq(GlobalSequence(100)).await;
    /// // Now we know events with sequences >= 100 may be available
    /// ```
    pub async fn wait_for_global_seq(&self, from: GlobalSequence) -> GlobalSequence {
        loop {
            // First, create a listener BEFORE checking the watermark (critical ordering)
            let listener = self.inner.listen();

            // Now check the watermark
            let current = self.inner.committed_next_global_seq();
            if current > from {
                return current;
            }

            // Wait for notification
            listener.await;
        }
    }

    /// Internal: notify that new events have been committed.
    pub(crate) fn notify(&self, new_next_seq: GlobalSequence) {
        self.inner.notify(new_next_seq);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;
    use std::thread;
    use std::time::Duration;

    #[rstest]
    #[case(GlobalSequence(0))]
    #[case(GlobalSequence(5))]
    #[case(GlobalSequence(42))]
    fn committed_next_global_seq_starts_at_initial(#[case] initial: GlobalSequence) {
        let watcher = WriteWatcher::new(initial);
        assert_eq!(watcher.committed_next_global_seq(), initial);
    }

    #[rstest]
    fn clone_shares_state() {
        let watcher = WriteWatcher::new(GlobalSequence(0));
        let watcher2 = watcher.clone();

        watcher.notify(GlobalSequence(3));
        assert_eq!(watcher2.committed_next_global_seq(), GlobalSequence(3));
    }

    #[rstest]
    fn notify_is_monotonic() {
        let watcher = WriteWatcher::new(GlobalSequence(5));

        watcher.notify(GlobalSequence(10));
        assert_eq!(watcher.committed_next_global_seq(), GlobalSequence(10));

        // Should not go backwards.
        watcher.notify(GlobalSequence(7));
        assert_eq!(watcher.committed_next_global_seq(), GlobalSequence(10));
    }

    #[rstest]
    fn wait_returns_immediately_when_already_ahead() {
        let watcher = WriteWatcher::new(GlobalSequence(10));

        let got = pollster::block_on(watcher.wait_for_global_seq(GlobalSequence(5)));
        assert_eq!(got, GlobalSequence(10));
    }

    #[rstest]
    fn wait_ignores_spurious_notifications_when_watermark_does_not_advance() {
        let watcher = WriteWatcher::new(GlobalSequence(0));
        let notifier = watcher.clone();

        thread::spawn(move || {
            // Notify without advancing.
            thread::sleep(Duration::from_millis(10));
            notifier.notify(GlobalSequence(0));

            // Now advance.
            thread::sleep(Duration::from_millis(10));
            notifier.notify(GlobalSequence(1));
        });

        let got = pollster::block_on(watcher.wait_for_global_seq(GlobalSequence(0)));
        assert_eq!(got, GlobalSequence(1));
    }

    #[rstest]
    fn wait_wakes_multiple_waiters() {
        let watcher = WriteWatcher::new(GlobalSequence(0));

        let w1 = watcher.clone();
        let w2 = watcher.clone();

        let t1 =
            thread::spawn(move || pollster::block_on(w1.wait_for_global_seq(GlobalSequence(0))));
        let t2 =
            thread::spawn(move || pollster::block_on(w2.wait_for_global_seq(GlobalSequence(0))));

        // Give threads a chance to start waiting (avoid flakiness by not requiring it).
        thread::sleep(Duration::from_millis(10));
        watcher.notify(GlobalSequence(1));

        assert_eq!(t1.join().unwrap(), GlobalSequence(1));
        assert_eq!(t2.join().unwrap(), GlobalSequence(1));
    }
}
