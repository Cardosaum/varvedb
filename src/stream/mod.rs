// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Stream handle for typed event access.

use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use heed::{PutFlags, RoTxn, WithTls};
use rkyv::ser::allocator::Arena;

use crate::error::{Error, Result};
use crate::timed_dbg;
use crate::types::{GlobalEventRecord, GlobalSequence, StreamId, StreamKey, StreamSequence};

pub(crate) mod core;
mod reader;
mod serializer;

pub(crate) use core::StreamCore;
pub use reader::{StreamIterator, StreamReader};
pub use serializer::{HighSerializer, LowSerializer};

/// A typed stream handle for appending and reading events.
///
/// The type parameter `T` is the event payload type.
/// The const parameter `N` is the serialization buffer size.
pub struct Stream<T, const N: usize> {
    pub(crate) core: Arc<StreamCore>,
    serializer_buffer: [u8; N],
    _marker: PhantomData<T>,
}

impl<T, const N: usize> Stream<T, N> {
    pub(crate) fn new(core: Arc<StreamCore>) -> Self {
        Self {
            core,
            serializer_buffer: [0u8; N],
            _marker: PhantomData,
        }
    }

    /// Get the stream name.
    pub fn name(&self) -> &str {
        &self.core.stream_name
    }

    /// Get the current global sequence (next to be assigned).
    pub fn next_global_seq(&self) -> GlobalSequence {
        GlobalSequence(self.core.next_global_seq.load(Ordering::Relaxed))
    }

    // =========================================================================
    // Private serialization helpers
    // =========================================================================

    fn serialize_low<U>(&mut self, event: &U) -> Result<Vec<u8>>
    where
        U: for<'a> rkyv::Serialize<LowSerializer<'a>>,
    {
        let writer = rkyv::ser::writer::Buffer::from(&mut self.serializer_buffer);
        let mut serializer = rkyv::ser::Serializer::new(writer, (), ());
        rkyv::api::serialize_using::<_, rkyv::rancor::Error>(event, &mut serializer)?;
        let pos = serializer.into_writer().len();
        Ok(self.serializer_buffer[..pos].to_vec())
    }

    fn serialize_high<U>(&mut self, event: &U) -> Result<Vec<u8>>
    where
        U: for<'a> rkyv::Serialize<HighSerializer<'a>>,
    {
        let mut arena = Arena::new();
        let writer = rkyv::ser::writer::Buffer::from(&mut self.serializer_buffer);
        let sharing = rkyv::ser::sharing::Share::new();
        let mut serializer = rkyv::ser::Serializer::new(writer, arena.acquire(), sharing);
        rkyv::api::serialize_using::<_, rkyv::rancor::Error>(event, &mut serializer)?;
        let pos = serializer.into_writer().len();
        Ok(self.serializer_buffer[..pos].to_vec())
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    /// Get or initialize the next sequence for a stream_id.
    fn get_next_stream_seq(&self, rtxn: &RoTxn<WithTls>, stream_id: StreamId) -> Result<u64> {
        match self.core.meta_db.get(rtxn, &stream_id.0)? {
            Some(seq) => Ok(seq),
            None => Ok(0),
        }
    }

    /// Notify watchers that a commit succeeded and the committed watermark advanced.
    #[cfg(feature = "notify")]
    #[inline]
    fn notify_commit(&self, new_next_seq: GlobalSequence) {
        self.core.watcher.notify(new_next_seq);
    }

    /// Store event in global DB and index in stream index DB.
    fn store_event(
        &mut self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
        payload: &[u8],
    ) -> Result<GlobalSequence> {
        // Atomically get and increment the global sequence
        let global_seq_val = self.core.next_global_seq.fetch_add(1, Ordering::Relaxed);
        let global_seq = GlobalSequence(global_seq_val);
        let index_key = StreamKey::new(stream_id, stream_seq);

        let mut wtxn = self.core.env.write_txn()?;

        // Write payload to global events DB (primary storage)
        let global_record = GlobalEventRecord {
            stream_name: self.core.stream_name.clone(),
            stream_id,
            stream_seq,
            payload: payload.to_vec(),
        };
        let global_bytes = global_record.to_bytes();
        timed_dbg!("global_put", {
            self.core.global_db.put_with_flags(
                &mut wtxn,
                PutFlags::NO_OVERWRITE,
                &global_seq_val,
                &global_bytes,
            )
        })?;

        // Write pointer to stream index DB
        timed_dbg!("index_put", {
            self.core.index_db.put_with_flags(
                &mut wtxn,
                PutFlags::NO_OVERWRITE,
                &index_key,
                &global_seq_val,
            )
        })?;

        // Update stream metadata (next sequence)
        self.core
            .meta_db
            .put(&mut wtxn, &stream_id.0, &(stream_seq.0 + 1))?;

        timed_dbg!("commit", wtxn.commit())?;

        // Notify watchers AFTER successful commit
        #[cfg(feature = "notify")]
        self.notify_commit(GlobalSequence(global_seq_val + 1));

        Ok(global_seq)
    }

    /// Store multiple events in a single transaction.
    fn store_batch(
        &mut self,
        stream_id: StreamId,
        start_stream_seq: StreamSequence,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<(StreamSequence, GlobalSequence)>> {
        let count = payloads.len();
        let mut results = Vec::with_capacity(count);

        // Reserve global sequences atomically
        let start_global_seq = self
            .core
            .next_global_seq
            .fetch_add(count as u64, Ordering::Relaxed);

        let mut wtxn = self.core.env.write_txn()?;

        let mut current_stream_seq = start_stream_seq.0;
        let mut current_global_seq = start_global_seq;

        timed_dbg!(format!("batch_put({count})"), {
            for payload in payloads {
                let stream_seq = StreamSequence(current_stream_seq);
                let global_seq = GlobalSequence(current_global_seq);
                let index_key = StreamKey::new(stream_id, stream_seq);

                // Write payload to global events DB
                let global_record = GlobalEventRecord {
                    stream_name: self.core.stream_name.clone(),
                    stream_id,
                    stream_seq,
                    payload,
                };
                let global_bytes = global_record.to_bytes();
                self.core.global_db.put_with_flags(
                    &mut wtxn,
                    PutFlags::NO_OVERWRITE,
                    &current_global_seq,
                    &global_bytes,
                )?;

                // Write pointer to stream index DB
                self.core.index_db.put_with_flags(
                    &mut wtxn,
                    PutFlags::NO_OVERWRITE,
                    &index_key,
                    &current_global_seq,
                )?;

                results.push((stream_seq, global_seq));
                current_stream_seq += 1;
                current_global_seq += 1;
            }
            Ok::<_, Error>(())
        })?;

        // Update stream metadata
        self.core
            .meta_db
            .put(&mut wtxn, &stream_id.0, &current_stream_seq)?;

        timed_dbg!("batch_commit", wtxn.commit())?;

        // Notify watchers AFTER successful commit
        #[cfg(feature = "notify")]
        self.notify_commit(GlobalSequence(current_global_seq));

        Ok(results)
    }

    // =========================================================================
    // Public append API
    // =========================================================================

    /// Append an event using a non-allocating serializer (best for POD / fixed-size types).
    ///
    /// Returns (stream_sequence, global_sequence).
    pub fn append(
        &mut self,
        stream_id: StreamId,
        event: &T,
    ) -> Result<(StreamSequence, GlobalSequence)>
    where
        T: for<'a> rkyv::Serialize<LowSerializer<'a>>,
    {
        let payload = timed_dbg!("serialize", self.serialize_low(event))?;

        // Get next stream sequence
        let rtxn = self.core.env.read_txn()?;
        let stream_seq = StreamSequence(self.get_next_stream_seq(&rtxn, stream_id)?);
        drop(rtxn);

        let global_seq = self.store_event(stream_id, stream_seq, &payload)?;
        Ok((stream_seq, global_seq))
    }

    /// Append an event using an allocating serializer (supports Strings, Vecs, etc).
    ///
    /// Returns (stream_sequence, global_sequence).
    pub fn append_alloc(
        &mut self,
        stream_id: StreamId,
        event: &T,
    ) -> Result<(StreamSequence, GlobalSequence)>
    where
        T: for<'a> rkyv::Serialize<HighSerializer<'a>>,
    {
        let payload = timed_dbg!("serialize", self.serialize_high(event))?;

        // Get next stream sequence
        let rtxn = self.core.env.read_txn()?;
        let stream_seq = StreamSequence(self.get_next_stream_seq(&rtxn, stream_id)?);
        drop(rtxn);

        let global_seq = self.store_event(stream_id, stream_seq, &payload)?;
        Ok((stream_seq, global_seq))
    }

    /// Append a batch of events using a non-allocating serializer in a single transaction.
    ///
    /// Returns the (stream_sequence, global_sequence) pairs for each event.
    pub fn append_batch(
        &mut self,
        stream_id: StreamId,
        events: &[T],
    ) -> Result<Vec<(StreamSequence, GlobalSequence)>>
    where
        T: for<'a> rkyv::Serialize<LowSerializer<'a>>,
    {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let event_count = events.len();

        let payloads = timed_dbg!(format!("batch_serialize({event_count})"), {
            let mut payloads = Vec::with_capacity(event_count);
            for event in events {
                payloads.push(self.serialize_low(event)?);
            }
            Ok::<_, Error>(payloads)
        })?;

        // Get starting stream sequence
        let rtxn = self.core.env.read_txn()?;
        let start_seq = StreamSequence(self.get_next_stream_seq(&rtxn, stream_id)?);
        drop(rtxn);

        timed_dbg!(
            format!("batch_total({event_count})"),
            self.store_batch(stream_id, start_seq, payloads)
        )
    }

    /// Append a batch of events using an allocating serializer in a single transaction.
    ///
    /// Returns the (stream_sequence, global_sequence) pairs for each event.
    pub fn append_batch_alloc(
        &mut self,
        stream_id: StreamId,
        events: &[T],
    ) -> Result<Vec<(StreamSequence, GlobalSequence)>>
    where
        T: for<'a> rkyv::Serialize<HighSerializer<'a>>,
    {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let event_count = events.len();

        let payloads = timed_dbg!(format!("batch_serialize({event_count})"), {
            let mut payloads = Vec::with_capacity(event_count);
            for event in events {
                payloads.push(self.serialize_high(event)?);
            }
            Ok::<_, Error>(payloads)
        })?;

        // Get starting stream sequence
        let rtxn = self.core.env.read_txn()?;
        let start_seq = StreamSequence(self.get_next_stream_seq(&rtxn, stream_id)?);
        drop(rtxn);

        timed_dbg!(
            format!("batch_total({event_count})"),
            self.store_batch(stream_id, start_seq, payloads)
        )
    }

    // =========================================================================
    // Public read API
    // =========================================================================

    /// Get a single event by stream_id and stream_sequence.
    ///
    /// Returns the raw bytes of the event payload (fetched from global DB).
    pub fn get_bytes(
        &self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
    ) -> Result<Option<Vec<u8>>> {
        let index_key = StreamKey::new(stream_id, stream_seq);
        let rtxn = self.core.env.read_txn()?;

        // Look up global_seq from index
        let Some(global_seq) = self.core.index_db.get(&rtxn, &index_key)? else {
            return Ok(None);
        };

        // Fetch payload from global DB
        let Some(global_bytes) = self.core.global_db.get(&rtxn, &global_seq)? else {
            return Ok(None);
        };

        // Extract payload from global record
        let Some(payload) = GlobalEventRecord::payload_from_bytes(global_bytes) else {
            return Ok(None);
        };

        Ok(Some(payload.to_vec()))
    }

    /// Create a reader for this stream.
    pub fn reader(&self) -> StreamReader<T> {
        StreamReader {
            core: Arc::clone(&self.core),
            scratch: rkyv::util::AlignedVec::new(),
            _marker: PhantomData,
        }
    }
}

#[cfg(all(test, feature = "notify"))]
mod notify_tests {
    use rstest::fixture;
    use rstest::rstest;
    use tempfile::TempDir;

    use crate::types::{GlobalSequence, StreamId};
    use crate::Varve;

    struct TestDb {
        varve: Varve,
        _dir: TempDir,
    }

    #[fixture]
    fn db() -> TestDb {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        let varve = Varve::new(dir.path()).expect("Failed to create Varve");
        TestDb { varve, _dir: dir }
    }

    #[rstest]
    fn append_notifies_after_commit(mut db: TestDb) {
        let watcher = db.varve.watcher();
        assert_eq!(watcher.committed_next_global_seq(), GlobalSequence(0));

        let mut stream = db
            .varve
            .stream::<u64, 64>("events")
            .expect("Failed to create stream");

        stream
            .append(StreamId(1), &123u64)
            .expect("Failed to append");

        assert_eq!(watcher.committed_next_global_seq(), GlobalSequence(1));
    }

    #[rstest]
    fn append_batch_notifies_after_commit(mut db: TestDb) {
        let watcher = db.varve.watcher();
        assert_eq!(watcher.committed_next_global_seq(), GlobalSequence(0));

        let mut stream = db
            .varve
            .stream::<u64, 64>("events")
            .expect("Failed to create stream");

        let events = [1u64, 2u64, 3u64];
        stream
            .append_batch(StreamId(1), &events)
            .expect("Failed to batch append");

        assert_eq!(watcher.committed_next_global_seq(), GlobalSequence(3));
    }

    #[rstest]
    fn append_batch_empty_does_not_notify(mut db: TestDb) {
        let watcher = db.varve.watcher();
        assert_eq!(watcher.committed_next_global_seq(), GlobalSequence(0));

        let mut stream = db
            .varve
            .stream::<u64, 64>("events")
            .expect("Failed to create stream");

        let empty: [u64; 0] = [];
        let res = stream
            .append_batch(StreamId(1), &empty)
            .expect("Failed to batch append");

        assert!(res.is_empty());
        assert_eq!(watcher.committed_next_global_seq(), GlobalSequence(0));
    }
}
