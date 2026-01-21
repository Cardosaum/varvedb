// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Core VarveDB database handle.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(feature = "snapshot")]
use std::path::PathBuf;

use heed::{Env, EnvOpenOptions};

use crate::config::{PathCreation, VarveConfig};
use crate::constants;
use crate::error::Result;
use crate::global::GlobalReader;
use crate::stream::{Stream, StreamCore};
use crate::types::{GlobalEventsDb, GlobalSequence, StreamIndexDb, StreamMetaDb};

#[cfg(feature = "notify")]
use crate::notify::WriteWatcher;

/// Per-stream database handles.
struct StreamDbs {
    index_db: StreamIndexDb,
    meta_db: StreamMetaDb,
}

/// A single-open event store handle with stream-based organization.
///
/// - **Writes** require `&mut self` (single-writer by construction; no locks).
/// - Use stream handles for typed access to specific streams.
/// - Use [`Varve::global_reader`] for reading events across all streams.
pub struct Varve {
    env: Env,
    global_db: GlobalEventsDb,
    /// Shared global sequence counter (atomic for lock-free access across streams)
    next_global_seq: Arc<AtomicU64>,
    /// Per-stream database handles (lazy creation)
    stream_dbs: HashMap<String, StreamDbs>,
    /// Base path for this database (used by optional subsystems, e.g. snapshots).
    #[cfg(feature = "snapshot")]
    base_path: PathBuf,
    /// Write notification watcher (optional, only when notify feature is enabled)
    #[cfg(feature = "notify")]
    watcher: WriteWatcher,
}

impl Varve {
    /// Create a new VarveDB instance at the specified path with default configuration.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_config(path, VarveConfig::default())
    }

    /// Create a new VarveDB instance at the specified path with custom configuration.
    pub fn with_config(path: impl AsRef<Path>, config: VarveConfig) -> Result<Self> {
        let path = path.as_ref();

        // Handle directory creation based on config
        match config.path_creation {
            PathCreation::None => {}
            PathCreation::Parents => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            PathCreation::All => {
                std::fs::create_dir_all(path)?;
            }
        }

        #[cfg(feature = "snapshot")]
        let base_path = path.to_path_buf();

        let env = unsafe {
            EnvOpenOptions::new()
                .read_txn_with_tls()
                .max_dbs(config.max_dbs)
                .map_size(config.map_size)
                .open(path)?
        };

        // Create or open the global events database
        let global_db: GlobalEventsDb = {
            let mut wtxn = env.write_txn()?;
            let db = env.create_database(&mut wtxn, Some(constants::GLOBAL_EVENTS_DB_NAME))?;
            wtxn.commit()?;
            db
        };

        // Get the next global sequence from the last entry
        let next_global_seq = {
            let rtxn = env.read_txn()?;
            match global_db.last(&rtxn)? {
                Some((last_key, _)) => last_key.saturating_add(1),
                None => 0,
            }
        };

        let next_global_seq_arc = Arc::new(AtomicU64::new(next_global_seq));

        #[cfg(feature = "notify")]
        let watcher = WriteWatcher::new(GlobalSequence(next_global_seq));

        Ok(Self {
            env,
            global_db,
            next_global_seq: next_global_seq_arc,
            stream_dbs: HashMap::new(),
            #[cfg(feature = "snapshot")]
            base_path,
            #[cfg(feature = "notify")]
            watcher,
        })
    }

    /// Get the current next global sequence.
    pub fn next_global_seq(&self) -> GlobalSequence {
        GlobalSequence(self.next_global_seq.load(Ordering::Relaxed))
    }

    /// Create or get a typed stream handle.
    ///
    /// The stream name is used to create separate LMDB databases for efficient
    /// stream-based iteration.
    ///
    /// # Type Parameters
    /// - `T`: The event payload type (must implement rkyv serialization)
    /// - `N`: The serialization buffer size (must be large enough for your events)
    pub fn stream<T, const N: usize>(&mut self, name: &str) -> Result<Stream<T, N>> {
        // Get or create the stream databases
        let (index_db, meta_db) = self.get_or_create_stream_dbs(name)?;

        let stream_core = Arc::new(StreamCore {
            env: self.env.clone(),
            stream_name: name.to_string(),
            index_db,
            meta_db,
            global_db: self.global_db,
            next_global_seq: Arc::clone(&self.next_global_seq),
            #[cfg(feature = "notify")]
            watcher: self.watcher.clone(),
        });

        Ok(Stream::new(stream_core))
    }

    /// Get or create stream databases (index + meta).
    fn get_or_create_stream_dbs(&mut self, name: &str) -> Result<(StreamIndexDb, StreamMetaDb)> {
        if let Some(dbs) = self.stream_dbs.get(name) {
            return Ok((dbs.index_db, dbs.meta_db));
        }

        let index_db_name = format!(
            "{}{}{}",
            constants::STREAM_DB_PREFIX,
            name,
            constants::STREAM_INDEX_SUFFIX
        );
        let meta_db_name = format!(
            "{}{}{}",
            constants::STREAM_DB_PREFIX,
            name,
            constants::STREAM_META_SUFFIX
        );

        let index_db: StreamIndexDb = {
            let mut wtxn = self.env.write_txn()?;
            let db = self.env.create_database(&mut wtxn, Some(&index_db_name))?;
            wtxn.commit()?;
            db
        };

        let meta_db: StreamMetaDb = {
            let mut wtxn = self.env.write_txn()?;
            let db = self.env.create_database(&mut wtxn, Some(&meta_db_name))?;
            wtxn.commit()?;
            db
        };

        self.stream_dbs
            .insert(name.to_string(), StreamDbs { index_db, meta_db });

        Ok((index_db, meta_db))
    }

    /// Create a reader for global event iteration.
    pub fn global_reader(&self) -> GlobalReader {
        GlobalReader {
            env: self.env.clone(),
            global_db: self.global_db,
            scratch: rkyv::util::AlignedVec::new(),
            #[cfg(feature = "notify")]
            watcher: self.watcher.clone(),
        }
    }

    /// Open the snapshot subsystem for this database (opt-in).
    ///
    /// This opens a separate LMDB environment under `<db_path>/snapshots`.
    #[cfg(feature = "snapshot")]
    pub fn snapshots(&self) -> Result<crate::snapshot::SnapshotStore> {
        crate::snapshot::SnapshotStore::open_default_under(&self.base_path)
    }

    /// Get a write watcher for async notification of new writes.
    ///
    /// This allows async readers to efficiently wait for new events without polling.
    #[cfg(feature = "notify")]
    pub fn watcher(&self) -> WriteWatcher {
        self.watcher.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PathCreation;
    use crate::types::{StreamId, StreamSequence};
    use rkyv::{Archive, Deserialize, Serialize};
    use rstest::{fixture, rstest};
    use rstest_reuse::{apply, template};
    use std::path::PathBuf;
    use tempfile::{tempdir, TempDir};

    // ============================================
    // Event type definitions
    // ============================================

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    #[rkyv(attr(derive(Debug)))]
    pub struct SimpleEvent {
        pub id: u64,
        pub timestamp: u64,
        pub value: i32,
    }

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    #[rkyv(attr(derive(Debug)))]
    pub struct OrderEvent {
        pub order_id: String,
        pub customer_id: String,
        pub amount: u64,
    }

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    #[rkyv(attr(derive(Debug)))]
    pub struct UserEvent {
        pub user_id: String,
        pub email: String,
        pub action: String,
    }

    // ============================================
    // Tests
    // ============================================

    #[test]
    fn test_create_stream_and_append_simple_event() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        let mut stream = varve
            .stream::<SimpleEvent, 1024>("test_stream")
            .expect("Failed to create stream");

        let event = SimpleEvent {
            id: 1,
            timestamp: 1702400000,
            value: 42,
        };

        let stream_id = StreamId(100);
        let (stream_seq, global_seq) = stream
            .append(stream_id, &event)
            .expect("Failed to append event");

        assert_eq!(stream_seq.0, 0);
        assert_eq!(global_seq.0, 0);

        // Read back the event
        let bytes = stream
            .get_bytes(stream_id, stream_seq)
            .expect("Failed to get bytes")
            .expect("Event not found");

        let archived =
            rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(archived.id, 1);
        assert_eq!(archived.timestamp, 1702400000);
        assert_eq!(archived.value, 42);
    }

    #[test]
    fn test_append_multiple_events_to_same_stream() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        let mut stream = varve
            .stream::<SimpleEvent, 1024>("events")
            .expect("Failed to create stream");

        let stream_id = StreamId(1);

        for i in 0..10u64 {
            let event = SimpleEvent {
                id: i,
                timestamp: 1702400000 + i,
                value: (i * 10) as i32,
            };
            let (stream_seq, global_seq) = stream
                .append(stream_id, &event)
                .expect("Failed to append event");
            assert_eq!(stream_seq.0, i);
            assert_eq!(global_seq.0, i);
        }

        // Verify all events
        for i in 0..10u64 {
            let bytes = stream
                .get_bytes(stream_id, StreamSequence(i))
                .expect("Failed to get bytes")
                .expect("Event not found");
            let archived =
                rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(&bytes).unwrap();
            assert_eq!(archived.id, i);
        }
    }

    #[test]
    fn test_multiple_stream_ids_in_same_stream() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        let mut stream = varve
            .stream::<SimpleEvent, 1024>("events")
            .expect("Failed to create stream");

        let stream_id_1 = StreamId(1);
        let stream_id_2 = StreamId(2);

        // Append to stream_id_1
        let (seq1_0, _) = stream
            .append(
                stream_id_1,
                &SimpleEvent {
                    id: 100,
                    timestamp: 1,
                    value: 1,
                },
            )
            .unwrap();
        assert_eq!(seq1_0.0, 0);

        // Append to stream_id_2
        let (seq2_0, _) = stream
            .append(
                stream_id_2,
                &SimpleEvent {
                    id: 200,
                    timestamp: 2,
                    value: 2,
                },
            )
            .unwrap();
        assert_eq!(seq2_0.0, 0); // Each stream_id has its own sequence

        // Append more to stream_id_1
        let (seq1_1, _) = stream
            .append(
                stream_id_1,
                &SimpleEvent {
                    id: 101,
                    timestamp: 3,
                    value: 3,
                },
            )
            .unwrap();
        assert_eq!(seq1_1.0, 1);

        // Verify
        let bytes = stream
            .get_bytes(stream_id_1, StreamSequence(0))
            .unwrap()
            .unwrap();
        let archived =
            rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(archived.id, 100);

        let bytes = stream
            .get_bytes(stream_id_2, StreamSequence(0))
            .unwrap()
            .unwrap();
        let archived =
            rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(archived.id, 200);
    }

    #[test]
    fn test_multiple_stream_names_share_global_sequence() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        // Create two different stream names
        let mut orders = varve
            .stream::<OrderEvent, 4096>("orders")
            .expect("Failed to create orders stream");

        let mut users = varve
            .stream::<UserEvent, 4096>("users")
            .expect("Failed to create users stream");

        // Append to orders
        let (stream_seq_1, global_seq_1) = orders
            .append_alloc(
                StreamId(1),
                &OrderEvent {
                    order_id: "ord_001".to_string(),
                    customer_id: "cust_001".to_string(),
                    amount: 100,
                },
            )
            .unwrap();
        assert_eq!(global_seq_1.0, 0);

        // Append to users - should continue global sequence
        let (stream_seq_2, global_seq_2) = users
            .append_alloc(
                StreamId(1),
                &UserEvent {
                    user_id: "usr_001".to_string(),
                    email: "test@example.com".to_string(),
                    action: "registered".to_string(),
                },
            )
            .unwrap();
        assert_eq!(global_seq_2.0, 1);

        // Append to orders again
        let (stream_seq_3, global_seq_3) = orders
            .append_alloc(
                StreamId(2),
                &OrderEvent {
                    order_id: "ord_002".to_string(),
                    customer_id: "cust_002".to_string(),
                    amount: 200,
                },
            )
            .unwrap();
        assert_eq!(global_seq_3.0, 2);

        // Verify stream sequences are independent
        // orders stream_id=1 has seq 0, orders stream_id=2 has seq 0
        // users stream_id=1 has seq 0
        assert_eq!(stream_seq_1.0, 0);
        assert_eq!(stream_seq_2.0, 0);
        assert_eq!(stream_seq_3.0, 0);
    }

    #[test]
    fn test_same_stream_id_different_stream_names() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        let mut orders = varve
            .stream::<SimpleEvent, 1024>("orders")
            .expect("Failed to create orders stream");

        let mut users = varve
            .stream::<SimpleEvent, 1024>("users")
            .expect("Failed to create users stream");

        // Both use StreamId(1) but should have separate sequences
        let (order_seq, _) = orders
            .append(
                StreamId(1),
                &SimpleEvent {
                    id: 100,
                    timestamp: 1,
                    value: 1,
                },
            )
            .unwrap();
        assert_eq!(order_seq.0, 0);

        let (user_seq, _) = users
            .append(
                StreamId(1),
                &SimpleEvent {
                    id: 200,
                    timestamp: 2,
                    value: 2,
                },
            )
            .unwrap();
        assert_eq!(user_seq.0, 0); // Independent sequence!

        // Append more to orders with same StreamId(1)
        let (order_seq_2, _) = orders
            .append(
                StreamId(1),
                &SimpleEvent {
                    id: 101,
                    timestamp: 3,
                    value: 3,
                },
            )
            .unwrap();
        assert_eq!(order_seq_2.0, 1);

        // Users still at 0 for StreamId(1)
        let (user_seq_2, _) = users
            .append(
                StreamId(1),
                &SimpleEvent {
                    id: 201,
                    timestamp: 4,
                    value: 4,
                },
            )
            .unwrap();
        assert_eq!(user_seq_2.0, 1);
    }

    #[test]
    fn test_append_alloc_with_strings() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        let mut stream = varve
            .stream::<OrderEvent, 4096>("orders")
            .expect("Failed to create stream");

        let stream_id = StreamId(12345);
        let event = OrderEvent {
            order_id: "ord_abc123".to_string(),
            customer_id: "cust_xyz789".to_string(),
            amount: 9999,
        };

        let (stream_seq, global_seq) = stream
            .append_alloc(stream_id, &event)
            .expect("Failed to append event");

        assert_eq!(stream_seq.0, 0);
        assert_eq!(global_seq.0, 0);

        // Read back
        let bytes = stream.get_bytes(stream_id, stream_seq).unwrap().unwrap();
        let archived =
            rkyv::access::<rkyv::Archived<OrderEvent>, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(archived.order_id.as_str(), "ord_abc123");
        assert_eq!(archived.customer_id.as_str(), "cust_xyz789");
        assert_eq!(archived.amount, 9999);
    }

    #[test]
    fn test_batch_append() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        let mut stream = varve
            .stream::<SimpleEvent, 1024>("events")
            .expect("Failed to create stream");

        let stream_id = StreamId(1);
        let events: Vec<SimpleEvent> = (0..100)
            .map(|i| SimpleEvent {
                id: i,
                timestamp: 1702400000 + i,
                value: (i * 10) as i32,
            })
            .collect();

        let results = stream
            .append_batch(stream_id, &events)
            .expect("Failed to batch append");

        assert_eq!(results.len(), 100);
        for (i, (stream_seq, global_seq)) in results.iter().enumerate() {
            assert_eq!(stream_seq.0, i as u64);
            assert_eq!(global_seq.0, i as u64);
        }
    }

    #[test]
    fn test_global_iteration() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        // Create two different streams and interleave writes
        let mut orders = varve
            .stream::<OrderEvent, 4096>("orders")
            .expect("Failed to create orders stream");

        orders
            .append_alloc(
                StreamId(1),
                &OrderEvent {
                    order_id: "ord_001".to_string(),
                    customer_id: "cust_001".to_string(),
                    amount: 100,
                },
            )
            .unwrap();

        let mut users = varve
            .stream::<UserEvent, 4096>("users")
            .expect("Failed to create users stream");

        users
            .append_alloc(
                StreamId(1),
                &UserEvent {
                    user_id: "usr_001".to_string(),
                    email: "test@example.com".to_string(),
                    action: "registered".to_string(),
                },
            )
            .unwrap();

        // Get orders stream again to test shared global seq
        let mut orders2 = varve
            .stream::<OrderEvent, 4096>("orders")
            .expect("Failed to get orders stream");

        orders2
            .append_alloc(
                StreamId(2),
                &OrderEvent {
                    order_id: "ord_002".to_string(),
                    customer_id: "cust_002".to_string(),
                    amount: 200,
                },
            )
            .unwrap();

        // Read global events
        let reader = varve.global_reader();
        let iter = reader
            .iter_from(GlobalSequence(0))
            .expect("Failed to create iterator");
        let events = iter.collect_all().expect("Failed to collect events");

        assert_eq!(events.len(), 3);

        // Check stream names and global ordering
        assert_eq!(events[0].stream_name, "orders");
        assert_eq!(events[0].global_seq.0, 0);

        assert_eq!(events[1].stream_name, "users");
        assert_eq!(events[1].global_seq.0, 1);

        assert_eq!(events[2].stream_name, "orders");
        assert_eq!(events[2].global_seq.0, 2);
    }

    #[test]
    fn test_stream_reader_iteration() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        let mut stream = varve
            .stream::<SimpleEvent, 1024>("events")
            .expect("Failed to create stream");

        let stream_id = StreamId(42);

        // Append some events
        for i in 0..5u64 {
            stream
                .append(
                    stream_id,
                    &SimpleEvent {
                        id: i,
                        timestamp: 1000 + i,
                        value: (i * 2) as i32,
                    },
                )
                .unwrap();
        }

        // Use reader to iterate
        let reader = stream.reader();
        let iter = reader
            .iter_stream(stream_id, None)
            .expect("Failed to create iterator");
        let events = iter.collect_bytes().expect("Failed to collect bytes");

        assert_eq!(events.len(), 5);
        for (i, (seq, bytes)) in events.iter().enumerate() {
            assert_eq!(seq.0, i as u64);
            let archived =
                rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(bytes).unwrap();
            assert_eq!(archived.id, i as u64);
        }
    }

    #[test]
    fn test_stream_reader_iteration_from_sequence() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        let mut stream = varve
            .stream::<SimpleEvent, 1024>("events")
            .expect("Failed to create stream");

        let stream_id = StreamId(1);

        // Append 10 events
        for i in 0..10u64 {
            stream
                .append(
                    stream_id,
                    &SimpleEvent {
                        id: i,
                        timestamp: 1000 + i,
                        value: (i * 2) as i32,
                    },
                )
                .unwrap();
        }

        // Iterate from sequence 5
        let reader = stream.reader();
        let iter = reader
            .iter_stream(stream_id, Some(StreamSequence(5)))
            .expect("Failed to create iterator");
        let events = iter.collect_bytes().expect("Failed to collect bytes");

        assert_eq!(events.len(), 5);
        for (i, (seq, _)) in events.iter().enumerate() {
            assert_eq!(seq.0, (5 + i) as u64);
        }
    }

    #[test]
    fn test_persistence_across_reopen() {
        let dir = tempdir().expect("Failed to create temp dir");

        // Write some events
        {
            let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");
            let mut stream = varve
                .stream::<SimpleEvent, 1024>("events")
                .expect("Failed to create stream");

            let stream_id = StreamId(1);
            stream
                .append(
                    stream_id,
                    &SimpleEvent {
                        id: 0,
                        timestamp: 1000,
                        value: 10,
                    },
                )
                .unwrap();
            stream
                .append(
                    stream_id,
                    &SimpleEvent {
                        id: 1,
                        timestamp: 1001,
                        value: 20,
                    },
                )
                .unwrap();
        }

        // Reopen and verify
        {
            let mut varve = Varve::new(dir.path()).expect("Failed to reopen Varve");

            // Global sequence should continue
            assert_eq!(varve.next_global_seq().0, 2);

            let mut stream = varve
                .stream::<SimpleEvent, 1024>("events")
                .expect("Failed to get stream");

            let stream_id = StreamId(1);

            // Append more
            let (stream_seq, global_seq) = stream
                .append(
                    stream_id,
                    &SimpleEvent {
                        id: 2,
                        timestamp: 1002,
                        value: 30,
                    },
                )
                .unwrap();

            assert_eq!(stream_seq.0, 2);
            assert_eq!(global_seq.0, 2);

            // Verify all events are readable
            let reader = stream.reader();
            let iter = reader.iter_stream(stream_id, None).unwrap();
            let events = iter.collect_bytes().unwrap();
            assert_eq!(events.len(), 3);
        }
    }

    #[test]
    #[cfg(feature = "notify")]
    fn test_watcher_initializes_to_next_global_seq_on_open_and_reopen() {
        let dir = tempdir().expect("Failed to create temp dir");

        // Fresh DB
        {
            let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

            assert_eq!(
                varve.watcher().committed_next_global_seq(),
                varve.next_global_seq()
            );

            let mut stream = varve
                .stream::<u64, 64>("events")
                .expect("Failed to create stream");

            stream.append(StreamId(1), &1u64).unwrap();
            stream.append(StreamId(1), &2u64).unwrap();

            assert_eq!(varve.next_global_seq(), GlobalSequence(2));
            assert_eq!(
                varve.watcher().committed_next_global_seq(),
                GlobalSequence(2)
            );
        }

        // Reopen should initialize watcher from persisted data.
        let varve = Varve::new(dir.path()).expect("Failed to reopen Varve");
        assert_eq!(varve.next_global_seq(), GlobalSequence(2));
        assert_eq!(
            varve.watcher().committed_next_global_seq(),
            GlobalSequence(2)
        );
    }

    // ============================================
    // PathCreation tests
    // ============================================

    struct PathCreationHarness {
        _dir: TempDir,
        nested_path: PathBuf,
    }

    #[fixture]
    fn path_creation_harness() -> PathCreationHarness {
        let dir = tempdir().expect("Failed to create temp dir");
        let nested_path = dir.path().join("a").join("b").join("c").join("db");
        PathCreationHarness {
            _dir: dir,
            nested_path,
        }
    }

    #[template]
    #[rstest]
    #[case::none(PathCreation::None)]
    #[case::parents(PathCreation::Parents)]
    #[case::all(PathCreation::All)]
    fn path_creation_variants(#[case] mode: PathCreation) {}

    #[apply(path_creation_variants)]
    fn path_creation_behavior(
        #[case] mode: PathCreation,
        path_creation_harness: PathCreationHarness,
    ) {
        let nested_path = &path_creation_harness.nested_path;
        let parent = nested_path.parent().unwrap();

        // Initially nothing exists
        assert!(!parent.exists());
        assert!(!nested_path.exists());

        let config = VarveConfig {
            path_creation: mode,
            ..Default::default()
        };

        let result = Varve::with_config(nested_path, config);

        match mode {
            PathCreation::None => {
                assert!(result.is_err(), "None should fail on missing path");
                assert!(!parent.exists());
            }
            PathCreation::Parents => {
                // Parents created, db dir may or may not exist depending on LMDB
                assert!(parent.exists(), "Parents should create parent directories");
            }
            PathCreation::All => {
                assert!(result.is_ok(), "All should succeed");
                assert!(nested_path.exists(), "All should create full path");
            }
        }
    }

    #[rstest]
    fn path_creation_all_db_is_usable(path_creation_harness: PathCreationHarness) {
        let config = VarveConfig {
            path_creation: PathCreation::All,
            ..Default::default()
        };

        let mut varve =
            Varve::with_config(&path_creation_harness.nested_path, config).expect("Should open");

        let mut stream = varve
            .stream::<SimpleEvent, 1024>("test")
            .expect("Should create stream");

        let (seq, _) = stream
            .append(
                StreamId(1),
                &SimpleEvent {
                    id: 42,
                    timestamp: 1000,
                    value: 99,
                },
            )
            .expect("Should append");

        assert_eq!(seq.0, 0);
    }
}
