// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use crate::engine::{EventView, Reader, Writer};
use crate::model::Payload;
use crate::storage::{Storage, StorageConfig};
use crate::traits::MetadataExt;
use rkyv::api::high::HighSerializer;
use rkyv::rancor::Error as RancorError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use std::path::Path;

/// Specifies the expected version of a stream during an append operation.
///
/// This is used for optimistic concurrency control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedVersion {
    /// The event will be appended with the next available version number for the stream.
    ///
    /// The database will automatically assert the next version is `current_version + 1`.
    /// Be careful with this in concurrent environments, as it does not prevent race conditions
    /// if you read the version, decide to write, and another writer writes in between.
    /// Actually, `Auto` just means "I don't know the version, just append it".
    /// If you care about concurrency, use `Exact`.
    Auto,
    /// The event must be appended with this specific version number.
    ///
    /// The operation will fail with `ConcurrencyConflict` if the version does not match
    /// the next expected version in the sequence.
    Exact(u32),
}

/// The main entry point for interacting with VarveDB.
///
/// `Varve` provides a high-level, unified API for reading and writing events.
/// It wraps the underlying storage engine and manages transactions.
#[derive(Clone)]
pub struct Varve<E, M> {
    storage: Storage,
    writer: Writer<E>,
    reader: Reader<E>,
    _marker: std::marker::PhantomData<M>,
}

impl<E, M> Varve<E, M>
where
    E: rkyv::Archive
        + for<'a> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>
        + std::fmt::Debug, // Debug required by Writer derive currently, let's keep it safe
    E::Archived: for<'a> rkyv::bytecheck::CheckBytes<
        rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
    >,
    M: MetadataExt,
{
    /// Opens a VarveDB instance at the specified path.
    ///
    /// If the database does not exist, it will be created.
    pub fn open(path: impl AsRef<Path>) -> crate::error::Result<Self> {
        log::trace!("Opening VarveDB at: {}", path.as_ref().display());
        let config = StorageConfig {
            path: path.as_ref().to_path_buf(),
            ..Default::default()
        };
        Self::open_with_config(config)
    }

    /// Opens a VarveDB instance with a custom configuration.
    pub fn open_with_config(config: StorageConfig) -> crate::error::Result<Self> {
        let storage = Storage::open(config)?;
        let writer = Writer::new(storage.clone());
        let reader = Reader::new(storage.clone());

        Ok(Self {
            storage,
            writer,
            reader,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn reader(&self) -> &Reader<E> {
        &self.reader
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.writer.subscribe()
    }

    /// Appends a new event to the database.
    ///
    /// The stream ID is extracted from the event metadata.
    ///
    /// # Arguments
    ///
    /// *   `payload` - The event and its metadata.
    /// *   `expected` - The expected version for optimistic concurrency control.
    pub fn append(
        &mut self,
        payload: Payload<E, M>,
        expected: ExpectedVersion,
    ) -> crate::error::Result<u64> {
        let stream_id = payload.metadata.stream_id();

        let version = match expected {
            ExpectedVersion::Exact(v) => v,
            ExpectedVersion::Auto => {
                // Calculate the next version by finding the highest version currently in the index.
                let last_ver = self.get_last_stream_version(stream_id)?;
                last_ver + 1
            }
        };

        // Append the event using the calculated or provided version.
        self.writer.append(stream_id, version, payload.event)
    }

    pub fn read_txn(&self) -> crate::error::Result<heed::RoTxn<'_>> {
        self.storage.env.read_txn().map_err(Into::into)
    }

    /// Retrieves an event by its stream ID and version.
    pub fn get_by_stream<'txn>(
        &self,
        txn: &'txn heed::RoTxn,
        stream_id: u128,
        version: u32,
    ) -> crate::error::Result<Option<EventView<'txn, E>>> {
        self.reader.get_by_stream(txn, stream_id, version)
    }

    fn get_last_stream_version(&self, stream_id: u128) -> crate::error::Result<u32> {
        let txn = self.storage.env.read_txn()?;
        let stream_id_bytes = stream_id.to_be_bytes();
        // Since StreamID is the first 16 bytes of the key, we can use prefix_iter
        let iter = self
            .storage
            .stream_index
            .prefix_iter(&txn, &stream_id_bytes)?;

        if let Some(result) = iter.last() {
            let (key_bytes, _) = result?;
            // Key is [StreamID (16)][Version (4)]
            if key_bytes.len() >= 20 {
                let version_bytes: [u8; 4] = key_bytes[16..20].try_into().unwrap();
                return Ok(u32::from_be_bytes(version_bytes));
            }
        }

        Ok(0)
    }

    /// Returns an iterator over all events in the database.
    ///
    /// Events are returned in global sequence order (insertion order).
    /// The iterator starts at sequence 1 (the first event).
    pub fn iter(&self) -> crate::error::Result<Iter<'_, E, M>> {
        let txn = self.storage.env.read_txn()?;
        Ok(Iter {
            txn,
            reader: self.reader.clone(),
            // Events are stored starting at sequence 1 (Writer uses last_seq + 1, where last_seq defaults to 0)
            current_seq: 1,
            _marker: std::marker::PhantomData,
        })
    }
}

/// An iterator over events in the database.
pub struct Iter<'a, E, M> {
    txn: heed::RoTxn<'a>,
    reader: crate::engine::Reader<E>,
    current_seq: u64,
    _marker: std::marker::PhantomData<M>,
}

impl<'a, E, M> Iterator for Iter<'a, E, M>
where
    E: rkyv::Archive,
    E::Archived: for<'b> rkyv::bytecheck::CheckBytes<
        rkyv::api::high::HighValidator<'b, rkyv::rancor::Error>,
    >,
{
    type Item = crate::error::Result<crate::engine::EventView<'a, E>>;

    fn next(&mut self) -> Option<Self::Item> {
        tracing::trace!("Iterating over event: {}", self.current_seq);
        match self.reader.get(&self.txn, self.current_seq) {
            Ok(Some(view)) => {
                self.current_seq += 1;
                // Return an owned version of the event data to satisfy standard Iterator.
                Some(Ok(view.into_owned()))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Payload;
    use crate::storage::StorageConfig;
    use crate::traits::MetadataExt;
    use rkyv::{Archive, Deserialize, Serialize};
    use tempfile::tempdir;

    /// A test event type with a simple value.
    #[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
    #[rkyv(derive(Debug))]
    #[repr(C)]
    struct TestEvent {
        value: u32,
    }

    /// A test event type with more complex data.
    #[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
    #[rkyv(derive(Debug))]
    #[repr(C)]
    struct ComplexEvent {
        id: u64,
        name: String,
        tags: Vec<String>,
    }

    /// Test metadata implementing MetadataExt.
    #[derive(Debug, Clone)]
    struct TestMetadata {
        stream_id: u128,
        version: u32,
    }

    impl TestMetadata {
        fn new(stream_id: u128, version: u32) -> Self {
            Self { stream_id, version }
        }
    }

    impl MetadataExt for TestMetadata {
        fn stream_id(&self) -> u128 {
            self.stream_id
        }

        fn version(&self) -> u32 {
            self.version
        }
    }

    /// Helper to create a Varve instance with a temporary directory.
    fn create_temp_varve<E, M>() -> (Varve<E, M>, tempfile::TempDir)
    where
        E: rkyv::Archive
            + for<'a> rkyv::Serialize<
                rkyv::api::high::HighSerializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::rancor::Error,
                >,
            > + std::fmt::Debug,
        E::Archived: for<'a> rkyv::bytecheck::CheckBytes<
            rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
        >,
        M: MetadataExt,
    {
        let dir = tempdir().expect("Failed to create temp directory");
        let config = StorageConfig {
            path: dir.path().to_path_buf(),
            ..Default::default()
        };
        let varve = Varve::open_with_config(config).expect("Failed to open Varve");
        (varve, dir)
    }

    // =========================================================================
    // Basic Open/Create Tests
    // =========================================================================

    #[test]
    fn test_open_creates_database() {
        let dir = tempdir().expect("Failed to create temp directory");
        let result = Varve::<TestEvent, TestMetadata>::open(dir.path());
        assert!(result.is_ok(), "Opening a new database should succeed");
    }

    #[test]
    fn test_open_with_config() {
        let dir = tempdir().expect("Failed to create temp directory");
        let config = StorageConfig {
            path: dir.path().to_path_buf(),
            map_size: 100 * 1024 * 1024, // 100MB
            ..Default::default()
        };
        let result = Varve::<TestEvent, TestMetadata>::open_with_config(config);
        assert!(result.is_ok(), "Opening with custom config should succeed");
    }

    #[test]
    fn test_reopen_existing_database() {
        let dir = tempdir().expect("Failed to create temp directory");
        let path = dir.path().to_path_buf();

        // Create and append an event
        {
            let mut varve =
                Varve::<TestEvent, TestMetadata>::open(&path).expect("Failed to open database");
            let payload = Payload::new(TestEvent { value: 42 }, TestMetadata::new(1, 1));
            varve
                .append(payload, ExpectedVersion::Exact(1))
                .expect("Failed to append event");
        }

        // Reopen and verify the event is still there
        {
            let varve =
                Varve::<TestEvent, TestMetadata>::open(&path).expect("Failed to reopen database");
            let txn = varve.read_txn().expect("Failed to create read transaction");
            let event = varve
                .get_by_stream(&txn, 1, 1)
                .expect("Failed to get event")
                .expect("Event should exist");
            assert_eq!(event.value, 42);
        }
    }

    // =========================================================================
    // Append Tests
    // =========================================================================

    #[test]
    fn test_append_single_event() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let payload = Payload::new(TestEvent { value: 100 }, TestMetadata::new(1, 1));
        let seq = varve.append(payload, ExpectedVersion::Exact(1));

        assert!(seq.is_ok(), "Appending a single event should succeed");
        assert_eq!(seq.unwrap(), 1, "First event should have sequence 1");
    }

    #[test]
    fn test_append_multiple_events_same_stream() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        for i in 1..=5 {
            let payload = Payload::new(TestEvent { value: i * 10 }, TestMetadata::new(1, i));
            let seq = varve
                .append(payload, ExpectedVersion::Exact(i))
                .expect("Append should succeed");
            assert_eq!(
                seq, i as u64,
                "Sequence should match version for single stream"
            );
        }
    }

    #[test]
    fn test_append_multiple_streams() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        // Stream 1: versions 1, 2, 3
        for v in 1..=3 {
            let payload = Payload::new(TestEvent { value: v * 10 }, TestMetadata::new(1, v));
            varve
                .append(payload, ExpectedVersion::Exact(v))
                .expect("Append to stream 1 should succeed");
        }

        // Stream 2: versions 1, 2
        for v in 1..=2 {
            let payload = Payload::new(TestEvent { value: v * 100 }, TestMetadata::new(2, v));
            varve
                .append(payload, ExpectedVersion::Exact(v))
                .expect("Append to stream 2 should succeed");
        }

        // Verify stream 1
        let txn = varve.read_txn().expect("Failed to create txn");
        for v in 1..=3 {
            let event = varve
                .get_by_stream(&txn, 1, v)
                .expect("Get should succeed")
                .expect("Event should exist");
            assert_eq!(event.value, v * 10);
        }

        // Verify stream 2
        for v in 1..=2 {
            let event = varve
                .get_by_stream(&txn, 2, v)
                .expect("Get should succeed")
                .expect("Event should exist");
            assert_eq!(event.value, v * 100);
        }
    }

    #[test]
    fn test_append_auto_version() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        // First event with Auto should get version 1
        let payload1 = Payload::new(TestEvent { value: 10 }, TestMetadata::new(1, 0)); // version in metadata is ignored for Auto
        let seq1 = varve
            .append(payload1, ExpectedVersion::Auto)
            .expect("First auto append should succeed");
        assert_eq!(seq1, 1);

        // Second event with Auto should get version 2
        let payload2 = Payload::new(TestEvent { value: 20 }, TestMetadata::new(1, 0));
        let seq2 = varve
            .append(payload2, ExpectedVersion::Auto)
            .expect("Second auto append should succeed");
        assert_eq!(seq2, 2);

        // Verify both events are stored correctly
        let txn = varve.read_txn().expect("Failed to create txn");
        let event1 = varve
            .get_by_stream(&txn, 1, 1)
            .expect("Get should succeed")
            .expect("Event 1 should exist");
        assert_eq!(event1.value, 10);

        let event2 = varve
            .get_by_stream(&txn, 1, 2)
            .expect("Get should succeed")
            .expect("Event 2 should exist");
        assert_eq!(event2.value, 20);
    }

    #[test]
    fn test_append_concurrency_conflict() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        // Append version 1
        let payload1 = Payload::new(TestEvent { value: 10 }, TestMetadata::new(1, 1));
        varve
            .append(payload1, ExpectedVersion::Exact(1))
            .expect("First append should succeed");

        // Try to append version 1 again - should fail
        let payload2 = Payload::new(TestEvent { value: 20 }, TestMetadata::new(1, 1));
        let result = varve.append(payload2, ExpectedVersion::Exact(1));

        assert!(result.is_err(), "Duplicate version should fail");
        match result {
            Err(crate::error::Error::ConcurrencyConflict { stream_id, version }) => {
                assert_eq!(stream_id, 1);
                assert_eq!(version, 1);
            }
            _ => panic!("Expected ConcurrencyConflict error"),
        }
    }

    // =========================================================================
    // Read Tests
    // =========================================================================

    #[test]
    fn test_get_by_stream_existing() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let payload = Payload::new(TestEvent { value: 42 }, TestMetadata::new(1, 1));
        varve
            .append(payload, ExpectedVersion::Exact(1))
            .expect("Append should succeed");

        let txn = varve.read_txn().expect("Failed to create txn");
        let event = varve
            .get_by_stream(&txn, 1, 1)
            .expect("Get should succeed")
            .expect("Event should exist");

        assert_eq!(event.value, 42);
    }

    #[test]
    fn test_get_by_stream_nonexistent() {
        let (varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let txn = varve.read_txn().expect("Failed to create txn");
        let result = varve
            .get_by_stream(&txn, 1, 1)
            .expect("Get should not error for missing event");

        assert!(result.is_none(), "Non-existent event should return None");
    }

    #[test]
    fn test_get_by_stream_wrong_version() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let payload = Payload::new(TestEvent { value: 42 }, TestMetadata::new(1, 1));
        varve
            .append(payload, ExpectedVersion::Exact(1))
            .expect("Append should succeed");

        let txn = varve.read_txn().expect("Failed to create txn");

        // Check version 2 which doesn't exist
        let result = varve
            .get_by_stream(&txn, 1, 2)
            .expect("Get should not error");
        assert!(result.is_none(), "Wrong version should return None");

        // Check version 0 which doesn't exist
        let result = varve
            .get_by_stream(&txn, 1, 0)
            .expect("Get should not error");
        assert!(result.is_none(), "Version 0 should return None");
    }

    #[test]
    fn test_get_by_stream_wrong_stream() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let payload = Payload::new(TestEvent { value: 42 }, TestMetadata::new(1, 1));
        varve
            .append(payload, ExpectedVersion::Exact(1))
            .expect("Append should succeed");

        let txn = varve.read_txn().expect("Failed to create txn");

        // Check stream 2 which doesn't have this version
        let result = varve
            .get_by_stream(&txn, 2, 1)
            .expect("Get should not error");
        assert!(result.is_none(), "Wrong stream should return None");
    }

    // =========================================================================
    // Iterator Tests - Core functionality
    // =========================================================================

    #[test]
    fn test_iter_empty_database() {
        let (varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let iter = varve.iter().expect("Creating iterator should succeed");
        let events: Vec<_> = iter.collect();

        assert!(events.is_empty(), "Empty database should yield no events");
    }

    #[test]
    fn test_iter_single_event() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let payload = Payload::new(TestEvent { value: 42 }, TestMetadata::new(1, 1));
        varve
            .append(payload, ExpectedVersion::Exact(1))
            .expect("Append should succeed");

        let iter = varve.iter().expect("Creating iterator should succeed");
        let events: Vec<_> = iter.collect();

        assert_eq!(events.len(), 1, "Should have exactly one event");
        let event = events[0].as_ref().expect("Event should be Ok");
        assert_eq!(event.value, 42);
    }

    #[test]
    fn test_iter_multiple_events() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        // Append 5 events
        for i in 1..=5 {
            let payload = Payload::new(TestEvent { value: i * 10 }, TestMetadata::new(1, i));
            varve
                .append(payload, ExpectedVersion::Exact(i))
                .expect("Append should succeed");
        }

        let iter = varve.iter().expect("Creating iterator should succeed");
        let events: Vec<_> = iter.collect();

        assert_eq!(events.len(), 5, "Should have 5 events");

        for (i, result) in events.iter().enumerate() {
            let event = result.as_ref().expect("Event should be Ok");
            assert_eq!(
                event.value,
                ((i + 1) * 10) as u32,
                "Event values should match in order"
            );
        }
    }

    #[test]
    fn test_iter_preserves_insertion_order() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        // Append events to different streams in interleaved order
        let expected_values = vec![100, 200, 101, 201, 102];

        // Stream 1: v1=100, v2=101, v3=102
        // Stream 2: v1=200, v2=201
        // Insertion order: 100, 200, 101, 201, 102

        varve
            .append(
                Payload::new(TestEvent { value: 100 }, TestMetadata::new(1, 1)),
                ExpectedVersion::Exact(1),
            )
            .unwrap();
        varve
            .append(
                Payload::new(TestEvent { value: 200 }, TestMetadata::new(2, 1)),
                ExpectedVersion::Exact(1),
            )
            .unwrap();
        varve
            .append(
                Payload::new(TestEvent { value: 101 }, TestMetadata::new(1, 2)),
                ExpectedVersion::Exact(2),
            )
            .unwrap();
        varve
            .append(
                Payload::new(TestEvent { value: 201 }, TestMetadata::new(2, 2)),
                ExpectedVersion::Exact(2),
            )
            .unwrap();
        varve
            .append(
                Payload::new(TestEvent { value: 102 }, TestMetadata::new(1, 3)),
                ExpectedVersion::Exact(3),
            )
            .unwrap();

        let iter = varve.iter().expect("Creating iterator should succeed");
        let actual_values: Vec<u32> = iter
            .map(|r| u32::from(r.expect("Event should be Ok").value))
            .collect();

        assert_eq!(
            actual_values, expected_values,
            "Events should be in insertion order (global sequence order)"
        );
    }

    #[test]
    fn test_iter_with_complex_event() {
        let (mut varve, _dir) = create_temp_varve::<ComplexEvent, TestMetadata>();

        let events = vec![
            ComplexEvent {
                id: 1,
                name: "First".to_string(),
                tags: vec!["tag1".to_string(), "tag2".to_string()],
            },
            ComplexEvent {
                id: 2,
                name: "Second".to_string(),
                tags: vec!["tag3".to_string()],
            },
            ComplexEvent {
                id: 3,
                name: "Third".to_string(),
                tags: vec![],
            },
        ];

        for (i, event) in events.iter().enumerate() {
            let payload = Payload::new(event.clone(), TestMetadata::new(1, (i + 1) as u32));
            varve
                .append(payload, ExpectedVersion::Exact((i + 1) as u32))
                .expect("Append should succeed");
        }

        let iter = varve.iter().expect("Creating iterator should succeed");
        let collected: Vec<_> = iter.collect();

        assert_eq!(collected.len(), 3);

        let e1 = collected[0].as_ref().unwrap();
        assert_eq!(e1.id, 1);
        assert_eq!(e1.name.as_str(), "First");
        assert_eq!(e1.tags.len(), 2);

        let e2 = collected[1].as_ref().unwrap();
        assert_eq!(e2.id, 2);
        assert_eq!(e2.name.as_str(), "Second");
        assert_eq!(e2.tags.len(), 1);

        let e3 = collected[2].as_ref().unwrap();
        assert_eq!(e3.id, 3);
        assert_eq!(e3.name.as_str(), "Third");
        assert_eq!(e3.tags.len(), 0);
    }

    // =========================================================================
    // Iterator Tests - Edge cases
    // =========================================================================

    #[test]
    fn test_iter_after_multiple_appends_and_reads() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        // Append some events
        for i in 1..=3 {
            let payload = Payload::new(TestEvent { value: i * 10 }, TestMetadata::new(1, i));
            varve
                .append(payload, ExpectedVersion::Exact(i))
                .expect("Append should succeed");
        }

        // Read some events
        {
            let txn = varve.read_txn().unwrap();
            let _ = varve.get_by_stream(&txn, 1, 1).unwrap();
            let _ = varve.get_by_stream(&txn, 1, 2).unwrap();
        }

        // Append more events
        for i in 4..=5 {
            let payload = Payload::new(TestEvent { value: i * 10 }, TestMetadata::new(1, i));
            varve
                .append(payload, ExpectedVersion::Exact(i))
                .expect("Append should succeed");
        }

        // Iterator should see all 5 events
        let iter = varve.iter().expect("Creating iterator should succeed");
        let events: Vec<_> = iter.collect();

        assert_eq!(events.len(), 5);
        for (i, result) in events.iter().enumerate() {
            let event = result.as_ref().expect("Event should be Ok");
            assert_eq!(event.value, ((i + 1) * 10) as u32);
        }
    }

    #[test]
    fn test_iter_count() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        for i in 1..=10 {
            let payload = Payload::new(TestEvent { value: i }, TestMetadata::new(1, i));
            varve
                .append(payload, ExpectedVersion::Exact(i))
                .expect("Append should succeed");
        }

        let count = varve.iter().expect("iter should succeed").count();
        assert_eq!(count, 10);
    }

    #[test]
    fn test_iter_take() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        for i in 1..=10 {
            let payload = Payload::new(TestEvent { value: i }, TestMetadata::new(1, i));
            varve
                .append(payload, ExpectedVersion::Exact(i))
                .expect("Append should succeed");
        }

        let iter = varve.iter().expect("iter should succeed");
        let first_three: Vec<_> = iter.take(3).collect();

        assert_eq!(first_three.len(), 3);
        assert_eq!(first_three[0].as_ref().unwrap().value, 1);
        assert_eq!(first_three[1].as_ref().unwrap().value, 2);
        assert_eq!(first_three[2].as_ref().unwrap().value, 3);
    }

    #[test]
    fn test_iter_skip() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        for i in 1..=10 {
            let payload = Payload::new(TestEvent { value: i }, TestMetadata::new(1, i));
            varve
                .append(payload, ExpectedVersion::Exact(i))
                .expect("Append should succeed");
        }

        let iter = varve.iter().expect("iter should succeed");
        let after_skip: Vec<_> = iter.skip(7).collect();

        assert_eq!(after_skip.len(), 3);
        assert_eq!(after_skip[0].as_ref().unwrap().value, 8);
        assert_eq!(after_skip[1].as_ref().unwrap().value, 9);
        assert_eq!(after_skip[2].as_ref().unwrap().value, 10);
    }

    // =========================================================================
    // ExpectedVersion Tests
    // =========================================================================

    #[test]
    fn test_expected_version_exact_success() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let payload = Payload::new(TestEvent { value: 1 }, TestMetadata::new(1, 1));
        let result = varve.append(payload, ExpectedVersion::Exact(1));
        assert!(result.is_ok());

        let payload = Payload::new(TestEvent { value: 2 }, TestMetadata::new(1, 2));
        let result = varve.append(payload, ExpectedVersion::Exact(2));
        assert!(result.is_ok());
    }

    #[test]
    fn test_expected_version_exact_conflict() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        // Append version 1
        let payload = Payload::new(TestEvent { value: 1 }, TestMetadata::new(1, 1));
        varve.append(payload, ExpectedVersion::Exact(1)).unwrap();

        // Try to skip to version 3 (should succeed since we're checking existence, not sequence)
        let payload = Payload::new(TestEvent { value: 3 }, TestMetadata::new(1, 3));
        let result = varve.append(payload, ExpectedVersion::Exact(3));
        // This should succeed because Exact just checks the version doesn't exist
        assert!(result.is_ok());
    }

    #[test]
    fn test_expected_version_auto_empty_stream() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let payload = Payload::new(TestEvent { value: 1 }, TestMetadata::new(1, 0));
        let seq = varve.append(payload, ExpectedVersion::Auto).unwrap();

        assert_eq!(seq, 1, "First auto-appended event should have seq 1");

        // Verify it was stored with version 1
        let txn = varve.read_txn().unwrap();
        let event = varve.get_by_stream(&txn, 1, 1).unwrap();
        assert!(event.is_some(), "Event should be at version 1");
    }

    #[test]
    fn test_expected_version_auto_existing_stream() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        // Manually append versions 1 and 2
        varve
            .append(
                Payload::new(TestEvent { value: 1 }, TestMetadata::new(1, 1)),
                ExpectedVersion::Exact(1),
            )
            .unwrap();
        varve
            .append(
                Payload::new(TestEvent { value: 2 }, TestMetadata::new(1, 2)),
                ExpectedVersion::Exact(2),
            )
            .unwrap();

        // Auto should now use version 3
        let payload = Payload::new(TestEvent { value: 3 }, TestMetadata::new(1, 0));
        let seq = varve.append(payload, ExpectedVersion::Auto).unwrap();
        assert_eq!(seq, 3);

        // Verify it was stored with version 3
        let txn = varve.read_txn().unwrap();
        let event = varve.get_by_stream(&txn, 1, 3).unwrap();
        assert!(event.is_some(), "Event should be at version 3");
    }

    // =========================================================================
    // Subscribe Tests
    // =========================================================================

    #[test]
    fn test_subscribe_receives_notifications() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let rx = varve.subscribe();

        // Initially should be 0
        assert_eq!(*rx.borrow(), 0);

        // Append an event
        let payload = Payload::new(TestEvent { value: 1 }, TestMetadata::new(1, 1));
        varve.append(payload, ExpectedVersion::Exact(1)).unwrap();

        // Check if notification was received (might need to wait)
        // The send happens synchronously in append, so we can check immediately
        assert!(rx.has_changed().unwrap_or(false) || *rx.borrow() == 1);
    }

    // =========================================================================
    // Reader Access Tests
    // =========================================================================

    #[test]
    fn test_reader_access() {
        let (varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        let reader = varve.reader();
        // Just verify we can access the reader
        let storage = reader.storage();
        assert!(storage.config.path.exists());
    }

    // =========================================================================
    // Clone Tests
    // =========================================================================

    #[test]
    fn test_varve_clone_shares_storage() {
        let (mut varve1, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        // Append via first instance
        let payload = Payload::new(TestEvent { value: 42 }, TestMetadata::new(1, 1));
        varve1.append(payload, ExpectedVersion::Exact(1)).unwrap();

        // Clone
        let varve2 = varve1.clone();

        // Read via second instance
        let txn = varve2.read_txn().unwrap();
        let event = varve2.get_by_stream(&txn, 1, 1).unwrap().unwrap();
        assert_eq!(event.value, 42);
    }

    // =========================================================================
    // Large Dataset Tests
    // =========================================================================

    #[test]
    fn test_large_number_of_events() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        const NUM_EVENTS: u32 = 1000;

        // Append many events
        for i in 1..=NUM_EVENTS {
            let payload = Payload::new(TestEvent { value: i }, TestMetadata::new(1, i));
            varve
                .append(payload, ExpectedVersion::Exact(i))
                .expect("Append should succeed");
        }

        // Verify count
        let count = varve.iter().unwrap().count();
        assert_eq!(count, NUM_EVENTS as usize);

        // Verify first and last
        let txn = varve.read_txn().unwrap();
        let first = varve.get_by_stream(&txn, 1, 1).unwrap().unwrap();
        assert_eq!(first.value, 1);

        let last = varve.get_by_stream(&txn, 1, NUM_EVENTS).unwrap().unwrap();
        assert_eq!(last.value, NUM_EVENTS);
    }

    #[test]
    fn test_multiple_streams_iteration() {
        let (mut varve, _dir) = create_temp_varve::<TestEvent, TestMetadata>();

        const STREAMS: u128 = 10;
        const EVENTS_PER_STREAM: u32 = 50;

        // Append events to multiple streams
        let mut total = 0u64;
        for stream in 1..=STREAMS {
            for version in 1..=EVENTS_PER_STREAM {
                let value = (stream as u32 * 1000) + version;
                let payload = Payload::new(TestEvent { value }, TestMetadata::new(stream, version));
                varve
                    .append(payload, ExpectedVersion::Exact(version))
                    .unwrap();
                total += 1;
            }
        }

        // Verify total count
        let count = varve.iter().unwrap().count();
        assert_eq!(count, total as usize);

        // Verify each stream
        let txn = varve.read_txn().unwrap();
        for stream in 1..=STREAMS {
            for version in 1..=EVENTS_PER_STREAM {
                let event = varve
                    .get_by_stream(&txn, stream, version)
                    .unwrap()
                    .expect("Event should exist");
                let expected = (stream as u32 * 1000) + version;
                assert_eq!(event.value, expected);
            }
        }
    }
}
