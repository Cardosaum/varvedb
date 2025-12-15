// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use heed::{Env, EnvOpenOptions, Error as HeedError, RoTxn, WithTls};

use crate::constants;
use crate::stream::{Stream, StreamCore};
use crate::types::{
    GlobalEventRecord, GlobalEventsDb, GlobalSequence, StreamEventsDb, StreamMetaDb,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Heed(#[from] HeedError),
    #[error(transparent)]
    Rkyv(#[from] rkyv::rancor::Error),
    #[error(transparent)]
    Stream(#[from] crate::stream::StreamError),
    #[error("Database not found: {0}")]
    DatabaseNotFound(String),
    #[error("Stream already exists: {0}")]
    StreamAlreadyExists(String),
}

#[derive(Debug, Clone)]
pub struct VarveConfig {
    pub max_dbs: u32,
    pub map_size: usize,
}

impl Default for VarveConfig {
    fn default() -> Self {
        Self {
            max_dbs: constants::DEFAULT_MAX_DBS,
            map_size: constants::DEFAULT_MAP_SIZE,
        }
    }
}

/// Core shared state for Varve (used for reader access)
#[allow(dead_code)]
struct VarveCore {
    env: Env,
    global_db: GlobalEventsDb,
    meta_db: StreamMetaDb,
    /// Stream databases indexed by name (reserved for future reader access)
    stream_dbs: HashMap<String, StreamEventsDb>,
}

/// A single-open event store handle with stream-based organization.
///
/// - **Writes** require `&mut self` (single-writer by construction; no locks).
/// - Use stream handles for typed access to specific streams.
/// - Use [`Varve::global_reader`] for reading events across all streams.
pub struct Varve {
    core: Arc<VarveCore>,
    /// Mutable env handle for creating new stream databases
    env: Env,
    /// Next global sequence number
    next_global_seq: u64,
    /// Stream databases (mutable for lazy creation)
    stream_dbs: HashMap<String, StreamEventsDb>,
}

impl Varve {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::with_config(path, VarveConfig::default())
    }

    pub fn with_config(path: impl AsRef<Path>, config: VarveConfig) -> Result<Self, Error> {
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

        // Create or open the stream metadata database
        let meta_db: StreamMetaDb = {
            let mut wtxn = env.write_txn()?;
            let db = env.create_database(&mut wtxn, Some(constants::STREAM_META_DB_NAME))?;
            wtxn.commit()?;
            db
        };

        // Get the next global sequence
        let next_global_seq = {
            let rtxn = env.read_txn()?;
            match global_db.last(&rtxn)? {
                Some((last_key, _)) => last_key.saturating_add(1),
                None => 0,
            }
        };

        let core = Arc::new(VarveCore {
            env: env.clone(),
            global_db,
            meta_db,
            stream_dbs: HashMap::new(),
        });

        Ok(Self {
            core,
            env,
            next_global_seq,
            stream_dbs: HashMap::new(),
        })
    }

    /// Get the current next global sequence
    pub fn next_global_seq(&self) -> GlobalSequence {
        GlobalSequence(self.next_global_seq)
    }

    /// Create or get a typed stream handle.
    ///
    /// The stream name is used to create a separate LMDB database for efficient
    /// prefix-based iteration.
    ///
    /// # Type Parameters
    /// - `T`: The event payload type (must implement rkyv serialization)
    /// - `N`: The serialization buffer size (must be large enough for your events)
    pub fn stream<T, const N: usize>(&mut self, name: &str) -> Result<Stream<T, N>, Error> {
        // Get or create the stream database
        let events_db = self.get_or_create_stream_db(name)?;

        let stream_core = Arc::new(StreamCore {
            env: self.env.clone(),
            stream_name: name.to_string(),
            events_db,
            meta_db: self.core.meta_db,
            global_db: self.core.global_db,
        });

        Ok(Stream::new(stream_core, self.next_global_seq))
    }

    /// Get or create a stream database
    fn get_or_create_stream_db(&mut self, name: &str) -> Result<StreamEventsDb, Error> {
        if let Some(db) = self.stream_dbs.get(name) {
            return Ok(*db);
        }

        let db_name = format!("{}{}", constants::STREAM_DB_PREFIX, name);
        let db: StreamEventsDb = {
            let mut wtxn = self.env.write_txn()?;
            let db = self.env.create_database(&mut wtxn, Some(&db_name))?;
            wtxn.commit()?;
            db
        };

        self.stream_dbs.insert(name.to_string(), db);
        Ok(db)
    }

    /// Create a reader for global event iteration
    pub fn global_reader(&self) -> GlobalReader {
        GlobalReader {
            env: self.env.clone(),
            global_db: self.core.global_db,
            scratch: rkyv::util::AlignedVec::new(),
        }
    }

    /// Update the internal next_global_seq after stream operations
    ///
    /// This should be called after using a Stream to keep the Varve's
    /// sequence counter in sync.
    pub fn sync_global_seq(&mut self) -> Result<(), Error> {
        let rtxn = self.env.read_txn()?;
        self.next_global_seq = match self.core.global_db.last(&rtxn)? {
            Some((last_key, _)) => last_key.saturating_add(1),
            None => 0,
        };
        Ok(())
    }
}

/// A reader for iterating events across all streams in global order.
pub struct GlobalReader {
    env: Env,
    global_db: GlobalEventsDb,
    scratch: rkyv::util::AlignedVec<16>,
}

impl GlobalReader {
    /// Get a single event by global sequence
    pub fn get(&mut self, global_seq: GlobalSequence) -> Result<Option<GlobalEventRecord>, Error> {
        let rtxn = self.env.read_txn()?;
        let bytes = self.global_db.get(&rtxn, &global_seq.0)?;
        match bytes {
            Some(b) => {
                self.scratch.clear();
                self.scratch.extend_from_slice(b);
                Ok(GlobalEventRecord::from_bytes(&self.scratch))
            }
            None => Ok(None),
        }
    }

    /// Iterate all events from a given global sequence
    pub fn iter_from(&self, from: GlobalSequence) -> Result<GlobalIterator<'_>, Error> {
        let rtxn = self.env.read_txn()?;
        Ok(GlobalIterator {
            db: self.global_db,
            rtxn,
            from: from.0,
        })
    }
}

impl Clone for GlobalReader {
    fn clone(&self) -> Self {
        Self {
            env: self.env.clone(),
            global_db: self.global_db,
            scratch: rkyv::util::AlignedVec::new(),
        }
    }
}

/// Iterator over global events
pub struct GlobalIterator<'a> {
    db: GlobalEventsDb,
    rtxn: RoTxn<'a, WithTls>,
    from: u64,
}

impl<'a> GlobalIterator<'a> {
    /// Collect all events as GlobalEventRecords
    pub fn collect_all(self) -> Result<Vec<(GlobalSequence, GlobalEventRecord)>, Error> {
        let mut results = Vec::new();
        let iter = self.db.range(&self.rtxn, &(self.from..))?;
        for item in iter {
            let (seq, bytes) = item?;
            if let Some(record) = GlobalEventRecord::from_bytes(bytes) {
                results.push((GlobalSequence(seq), record));
            }
        }
        Ok(results)
    }

    /// Iterate and apply a function to each event
    pub fn for_each<F>(self, mut f: F) -> Result<(), Error>
    where
        F: FnMut(GlobalSequence, GlobalEventRecord),
    {
        let iter = self.db.range(&self.rtxn, &(self.from..))?;
        for item in iter {
            let (seq, bytes) = item?;
            if let Some(record) = GlobalEventRecord::from_bytes(bytes) {
                f(GlobalSequence(seq), record);
            }
        }
        Ok(())
    }
}

// =============================================================================
// Re-export stream types
// =============================================================================

pub use crate::stream::{HighSerializer, LowSerializer, StreamError};

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::{Archive, Deserialize, Serialize};
    use tempfile::tempdir;

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

        let stream_id = crate::types::StreamId(100);
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

        let stream_id = crate::types::StreamId(1);

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
                .get_bytes(stream_id, crate::types::StreamSequence(i))
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

        let stream_id_1 = crate::types::StreamId(1);
        let stream_id_2 = crate::types::StreamId(2);

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
            .get_bytes(stream_id_1, crate::types::StreamSequence(0))
            .unwrap()
            .unwrap();
        let archived =
            rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(archived.id, 100);

        let bytes = stream
            .get_bytes(stream_id_2, crate::types::StreamSequence(0))
            .unwrap()
            .unwrap();
        let archived =
            rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(archived.id, 200);
    }

    #[test]
    fn test_append_alloc_with_strings() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        let mut stream = varve
            .stream::<OrderEvent, 4096>("orders")
            .expect("Failed to create stream");

        let stream_id = crate::types::StreamId(12345);
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

        let stream_id = crate::types::StreamId(1);
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

        // Create two different streams
        {
            let mut orders = varve
                .stream::<OrderEvent, 4096>("orders")
                .expect("Failed to create orders stream");

            orders
                .append_alloc(
                    crate::types::StreamId(1),
                    &OrderEvent {
                        order_id: "ord_001".to_string(),
                        customer_id: "cust_001".to_string(),
                        amount: 100,
                    },
                )
                .unwrap();
        }

        // Sync global sequence
        varve.sync_global_seq().unwrap();

        {
            let mut users = varve
                .stream::<UserEvent, 4096>("users")
                .expect("Failed to create users stream");

            users
                .append_alloc(
                    crate::types::StreamId(1),
                    &UserEvent {
                        user_id: "usr_001".to_string(),
                        email: "test@example.com".to_string(),
                        action: "registered".to_string(),
                    },
                )
                .unwrap();
        }

        varve.sync_global_seq().unwrap();

        {
            let mut orders = varve
                .stream::<OrderEvent, 4096>("orders")
                .expect("Failed to create orders stream");

            orders
                .append_alloc(
                    crate::types::StreamId(2),
                    &OrderEvent {
                        order_id: "ord_002".to_string(),
                        customer_id: "cust_002".to_string(),
                        amount: 200,
                    },
                )
                .unwrap();
        }

        // Read global events
        let reader = varve.global_reader();
        let iter = reader
            .iter_from(crate::types::GlobalSequence(0))
            .expect("Failed to create iterator");
        let events = iter.collect_all().expect("Failed to collect events");

        assert_eq!(events.len(), 3);

        // Check stream names
        assert_eq!(events[0].1.stream_name, "orders");
        assert_eq!(events[1].1.stream_name, "users");
        assert_eq!(events[2].1.stream_name, "orders");

        // Check global sequences
        assert_eq!(events[0].0 .0, 0);
        assert_eq!(events[1].0 .0, 1);
        assert_eq!(events[2].0 .0, 2);
    }

    #[test]
    fn test_stream_reader_iteration() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut varve = Varve::new(dir.path()).expect("Failed to create Varve");

        let mut stream = varve
            .stream::<SimpleEvent, 1024>("events")
            .expect("Failed to create stream");

        let stream_id = crate::types::StreamId(42);

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

        let stream_id = crate::types::StreamId(1);

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
            .iter_stream(stream_id, Some(crate::types::StreamSequence(5)))
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

            let stream_id = crate::types::StreamId(1);
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

            let stream_id = crate::types::StreamId(1);

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
}
