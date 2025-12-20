// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Optional snapshot subsystem (separate LMDB environment).
//!
//! Snapshots are **opt-in** and live in a **separate LMDB environment** to reduce
//! page-cache interference with the primary event log.
//!
//! VarveDB does not compute snapshots. User code owns state computation and
//! decides when to save; VarveDB provides storage, lookup, and retention helpers.

mod codecs;
mod keys;
mod reader;
mod store;
mod writer;

pub use reader::SnapshotReader;
pub use store::{SnapshotConfig, SnapshotStore};
pub use writer::{GlobalSnapshotWriter, SnapshotWriter, StreamSnapshotWriter};

use crate::types::{GlobalSequence, StreamId, StreamSequence};

/// Borrowed stream snapshot scope (avoids allocating a `String`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotStreamScope<'a> {
    pub stream_name: &'a str,
    pub stream_id: StreamId,
}

impl<'a> SnapshotStreamScope<'a> {
    pub fn new(stream_name: &'a str, stream_id: StreamId) -> Self {
        Self {
            stream_name,
            stream_id,
        }
    }
}

/// Borrowed global/projection snapshot scope (avoids allocating a `String`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotGlobalScope<'a> {
    pub projection_name: &'a str,
}

impl<'a> SnapshotGlobalScope<'a> {
    pub fn new(projection_name: &'a str) -> Self {
        Self { projection_name }
    }
}

/// Identifies what a snapshot is “for”.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SnapshotScope {
    /// A per-aggregate snapshot anchored to a stream cursor.
    Stream {
        stream_name: String,
        stream_id: StreamId,
    },
    /// A global/projection snapshot anchored to a global cursor.
    Global { projection_name: String },
}

impl SnapshotScope {
    /// Create a stream scope (allocates the stream name).
    pub fn stream(stream_name: impl Into<String>, stream_id: StreamId) -> Self {
        Self::Stream {
            stream_name: stream_name.into(),
            stream_id,
        }
    }

    /// Create a global scope (allocates the projection name).
    pub fn global(projection_name: impl Into<String>) -> Self {
        Self::Global {
            projection_name: projection_name.into(),
        }
    }
}

/// Cursor associated with a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SnapshotCursor {
    Stream(StreamSequence),
    Global(GlobalSequence),
}

impl SnapshotCursor {
    pub fn as_u64(self) -> u64 {
        match self {
            SnapshotCursor::Stream(s) => s.0,
            SnapshotCursor::Global(g) => g.0,
        }
    }
}

/// Advisory output for snapshot scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotAdvice {
    pub should_snapshot: bool,
    pub events_since_last_snapshot: u64,
}

/// “Every N events applied, consider snapshotting” policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPolicy {
    pub every_n_events: std::num::NonZeroU64,
}

impl SnapshotPolicy {
    pub fn every_n_events(every_n_events: std::num::NonZeroU64) -> Self {
        Self { every_n_events }
    }
}

/// Retention policy for old snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotRetention {
    /// Keep only the most recent `n` snapshots for a scope.
    KeepLast(std::num::NonZeroU64),
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use rkyv::{Archive, Deserialize, Serialize};

    use crate::snapshot::{SnapshotPolicy, SnapshotRetention};
    use crate::types::{GlobalSequence, StreamId, StreamSequence};
    use crate::Varve;

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    #[rkyv(attr(derive(Debug, PartialEq)))]
    struct TestSnapshot {
        count: u64,
        name: String,
    }

    #[test]
    fn snapshot_save_load_latest_and_at_or_before_stream() {
        let dir = tempfile::tempdir().expect("tempdir");

        let varve = Varve::new(dir.path()).expect("varve");
        let store = varve.snapshots().expect("snapshots");

        let mut writer = store.writer();
        let mut reader = store.reader();

        // No snapshots yet.
        assert!(reader
            .load_latest_stream_archived::<TestSnapshot>("orders", StreamId(1))
            .unwrap()
            .is_none());

        // Save at seq 10.
        writer
            .save_stream(
                "orders",
                StreamId(1),
                StreamSequence(10),
                &TestSnapshot {
                    count: 1,
                    name: "a".to_string(),
                },
            )
            .unwrap();

        // Latest should be 10.
        let (cursor, archived) = reader
            .load_latest_stream_archived::<TestSnapshot>("orders", StreamId(1))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(10));
        assert_eq!(archived.count, 1);
        assert_eq!(archived.name.as_str(), "a");

        // Save at seq 20 (newer).
        writer
            .save_stream(
                "orders",
                StreamId(1),
                StreamSequence(20),
                &TestSnapshot {
                    count: 2,
                    name: "b".to_string(),
                },
            )
            .unwrap();

        // Latest should be 20.
        let (cursor, archived) = reader
            .load_latest_stream_archived::<TestSnapshot>("orders", StreamId(1))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(20));
        assert_eq!(archived.count, 2);
        assert_eq!(archived.name.as_str(), "b");

        // At-or-before 15 should return the seq 10 snapshot.
        let (cursor, archived) = reader
            .load_stream_at_or_before_archived::<TestSnapshot>(
                "orders",
                StreamId(1),
                StreamSequence(15),
            )
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(10));
        assert_eq!(archived.count, 1);

        // At-or-before 20 should return seq 20.
        let (cursor, archived) = reader
            .load_stream_at_or_before_archived::<TestSnapshot>(
                "orders",
                StreamId(1),
                StreamSequence(20),
            )
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(20));
        assert_eq!(archived.count, 2);

        // Saving an older snapshot should not move latest cursor.
        writer
            .save_stream(
                "orders",
                StreamId(1),
                StreamSequence(15),
                &TestSnapshot {
                    count: 999,
                    name: "older".to_string(),
                },
            )
            .unwrap();
        let (cursor, archived) = reader
            .load_latest_stream_archived::<TestSnapshot>("orders", StreamId(1))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(20));
        assert_eq!(archived.count, 2);
    }

    #[test]
    fn snapshot_due_policy_uses_applied_cursor() {
        let dir = tempfile::tempdir().expect("tempdir");

        let varve = Varve::new(dir.path()).expect("varve");
        let store = varve.snapshots().expect("snapshots");

        let policy = SnapshotPolicy::every_n_events(NonZeroU64::new(3).unwrap());

        // No snapshot: applied=0 => events_since = 1
        let advice = store
            .due_stream("orders", StreamId(1), StreamSequence(0), policy)
            .unwrap();
        assert_eq!(advice.events_since_last_snapshot, 1);
        assert!(!advice.should_snapshot);

        // No snapshot: applied=2 => events_since = 3 => should snapshot
        let advice = store
            .due_stream("orders", StreamId(1), StreamSequence(2), policy)
            .unwrap();
        assert_eq!(advice.events_since_last_snapshot, 3);
        assert!(advice.should_snapshot);

        // Save snapshot at cursor 2, then applied=2 => events_since 0
        let mut writer = store.writer();
        writer
            .save_stream(
                "orders",
                StreamId(1),
                StreamSequence(2),
                &TestSnapshot {
                    count: 1,
                    name: "x".to_string(),
                },
            )
            .unwrap();

        let advice = store
            .due_stream("orders", StreamId(1), StreamSequence(2), policy)
            .unwrap();
        assert_eq!(advice.events_since_last_snapshot, 0);
        assert!(!advice.should_snapshot);

        // applied=5 => events_since 3 => should snapshot
        let advice = store
            .due_stream("orders", StreamId(1), StreamSequence(5), policy)
            .unwrap();
        assert_eq!(advice.events_since_last_snapshot, 3);
        assert!(advice.should_snapshot);
    }

    #[test]
    fn snapshot_prune_keep_last_updates_latest() {
        let dir = tempfile::tempdir().expect("tempdir");

        let varve = Varve::new(dir.path()).expect("varve");
        let store = varve.snapshots().expect("snapshots");

        let mut writer = store.writer();
        let mut reader = store.reader();

        for seq in [1u64, 2, 3, 4] {
            writer
                .save_global(
                    "proj",
                    GlobalSequence(seq),
                    &TestSnapshot {
                        count: seq,
                        name: format!("s{seq}"),
                    },
                )
                .unwrap();
        }

        // Latest should be 4.
        let (cursor, archived) = reader
            .load_latest_global_archived::<TestSnapshot>("proj")
            .unwrap()
            .unwrap();
        assert_eq!(cursor, GlobalSequence(4));
        assert_eq!(archived.count, 4);

        // Keep last 2.
        let deleted = writer
            .prune_global(
                "proj",
                SnapshotRetention::KeepLast(NonZeroU64::new(2).unwrap()),
            )
            .unwrap();
        assert_eq!(deleted, 2);

        // Latest should remain 4.
        let (cursor, archived) = reader
            .load_latest_global_archived::<TestSnapshot>("proj")
            .unwrap()
            .unwrap();
        assert_eq!(cursor, GlobalSequence(4));
        assert_eq!(archived.count, 4);

        // At-or-before 3 should now return 3 (since 1 and 2 were pruned).
        let (cursor, archived) = reader
            .load_global_at_or_before_archived::<TestSnapshot>("proj", GlobalSequence(3))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, GlobalSequence(3));
        assert_eq!(archived.count, 3);
    }

    #[test]
    fn snapshot_writer_save_if_due_and_prune_stream_is_one_call() {
        let dir = tempfile::tempdir().expect("tempdir");

        let varve = Varve::new(dir.path()).expect("varve");
        let store = varve.snapshots().expect("snapshots");

        let policy = SnapshotPolicy::every_n_events(NonZeroU64::new(2).unwrap());
        let retention = SnapshotRetention::KeepLast(NonZeroU64::new(1).unwrap());

        let mut writer =
            store
                .writer()
                .for_stream_scope(crate::snapshot::SnapshotStreamScope::new(
                    "orders",
                    StreamId(1),
                ));
        let mut reader = store.reader();

        // applied=0 => events_since = 1 => not due => no write.
        let advice = writer
            .save_if_due_and_prune(
                StreamSequence(0),
                policy,
                retention,
                &TestSnapshot {
                    count: 0,
                    name: "should_not_write".to_string(),
                },
            )
            .unwrap();
        assert!(!advice.should_snapshot);
        assert!(reader
            .load_latest_stream_archived::<TestSnapshot>("orders", StreamId(1))
            .unwrap()
            .is_none());

        // applied=1 => events_since = 2 => due => writes at cursor 1.
        let advice = writer
            .save_if_due_and_prune(
                StreamSequence(1),
                policy,
                retention,
                &TestSnapshot {
                    count: 1,
                    name: "s1".to_string(),
                },
            )
            .unwrap();
        assert!(advice.should_snapshot);

        let (cursor, archived) = reader
            .load_latest_stream_archived::<TestSnapshot>("orders", StreamId(1))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(1));
        assert_eq!(archived.count, 1);

        // applied=3 => due again; keep-last=1 should prune the older snapshot.
        let advice = writer
            .save_if_due_and_prune(
                StreamSequence(3),
                policy,
                retention,
                &TestSnapshot {
                    count: 3,
                    name: "s3".to_string(),
                },
            )
            .unwrap();
        assert!(advice.should_snapshot);

        let (cursor, archived) = reader
            .load_latest_stream_archived::<TestSnapshot>("orders", StreamId(1))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(3));
        assert_eq!(archived.count, 3);

        // The earlier snapshot at 1 should have been pruned; thus nothing at-or-before 2.
        assert!(reader
            .load_stream_at_or_before_archived::<TestSnapshot>(
                "orders",
                StreamId(1),
                StreamSequence(2)
            )
            .unwrap()
            .is_none());
    }
}
