// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Stream handle for typed event access.

use std::marker::PhantomData;
use std::sync::Arc;

use heed::{Env, PutFlags, RoTxn, WithTls};
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::Arena;

use crate::timed_dbg;
use crate::types::{
    EventMeta, GlobalEventRecord, GlobalEventsDb, GlobalSequence, StreamEventsDb, StreamId,
    StreamKey, StreamMetaDb, StreamSequence,
};

/// Error type for stream operations
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    Rkyv(#[from] rkyv::rancor::Error),
    #[error("Stream not found: {0}")]
    StreamNotFound(String),
}

/// Zero-allocation serializer for fixed-size types.
pub type LowSerializer<'a> =
    Strategy<rkyv::ser::Serializer<rkyv::ser::writer::Buffer<'a>, (), ()>, rkyv::rancor::Error>;

/// Allocating serializer for arbitrary types.
pub type HighSerializer<'a> = Strategy<
    rkyv::ser::Serializer<
        rkyv::ser::writer::Buffer<'a>,
        rkyv::ser::allocator::ArenaHandle<'a>,
        rkyv::ser::sharing::Share,
    >,
    rkyv::rancor::Error,
>;

/// Shared core state for stream operations
pub(crate) struct StreamCore {
    pub env: Env,
    pub stream_name: String,
    pub events_db: StreamEventsDb,
    pub meta_db: StreamMetaDb,
    pub global_db: GlobalEventsDb,
}

/// A typed stream handle for appending and reading events.
///
/// The type parameter `T` is the event payload type.
/// The const parameter `N` is the serialization buffer size.
pub struct Stream<T, const N: usize> {
    core: Arc<StreamCore>,
    next_global_seq: u64,
    serializer_buffer: [u8; N],
    _marker: PhantomData<T>,
}

impl<T, const N: usize> Stream<T, N> {
    pub(crate) fn new(core: Arc<StreamCore>, next_global_seq: u64) -> Self {
        Self {
            core,
            next_global_seq,
            serializer_buffer: [0u8; N],
            _marker: PhantomData,
        }
    }

    /// Get the stream name
    pub fn name(&self) -> &str {
        &self.core.stream_name
    }

    /// Get the current global sequence (next to be assigned)
    pub fn next_global_seq(&self) -> GlobalSequence {
        GlobalSequence(self.next_global_seq)
    }

    // =========================================================================
    // Private serialization helpers
    // =========================================================================

    fn serialize_low<U>(&mut self, event: &U) -> Result<Vec<u8>, StreamError>
    where
        U: for<'a> rkyv::Serialize<LowSerializer<'a>>,
    {
        let writer = rkyv::ser::writer::Buffer::from(&mut self.serializer_buffer);
        let mut serializer = rkyv::ser::Serializer::new(writer, (), ());
        rkyv::api::serialize_using::<_, rkyv::rancor::Error>(event, &mut serializer)?;
        let pos = serializer.into_writer().len();
        Ok(self.serializer_buffer[..pos].to_vec())
    }

    fn serialize_high<U>(&mut self, event: &U) -> Result<Vec<u8>, StreamError>
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

    /// Get or initialize the next sequence for a stream_id
    fn get_next_stream_seq(
        &self,
        rtxn: &RoTxn<WithTls>,
        stream_id: StreamId,
    ) -> Result<u64, StreamError> {
        match self.core.meta_db.get(rtxn, &stream_id.0)? {
            Some(seq) => Ok(seq),
            None => Ok(0),
        }
    }

    /// Store event in both stream DB and global DB
    fn store_event(
        &mut self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
        payload: &[u8],
    ) -> Result<GlobalSequence, StreamError> {
        let global_seq = GlobalSequence(self.next_global_seq);
        let key = StreamKey::new(stream_id, stream_seq);

        let mut wtxn = self.core.env.write_txn()?;

        // Write to stream events DB
        timed_dbg!("stream_put", {
            self.core
                .events_db
                .put_with_flags(&mut wtxn, PutFlags::NO_OVERWRITE, &key, payload)
        })?;

        // Write to global events DB
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
                &global_seq.0,
                &global_bytes,
            )
        })?;

        // Update stream metadata (next sequence)
        self.core
            .meta_db
            .put(&mut wtxn, &stream_id.0, &(stream_seq.0 + 1))?;

        timed_dbg!("commit", wtxn.commit())?;

        self.next_global_seq += 1;
        Ok(global_seq)
    }

    /// Store multiple events in a single transaction
    fn store_batch(
        &mut self,
        stream_id: StreamId,
        start_stream_seq: StreamSequence,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<(StreamSequence, GlobalSequence)>, StreamError> {
        let count = payloads.len();
        let mut results = Vec::with_capacity(count);

        let mut wtxn = self.core.env.write_txn()?;

        let mut current_stream_seq = start_stream_seq.0;
        let mut current_global_seq = self.next_global_seq;

        timed_dbg!(format!("batch_put({count})"), {
            for payload in payloads {
                let stream_seq = StreamSequence(current_stream_seq);
                let global_seq = GlobalSequence(current_global_seq);
                let key = StreamKey::new(stream_id, stream_seq);

                // Write to stream events DB
                self.core.events_db.put_with_flags(
                    &mut wtxn,
                    PutFlags::NO_OVERWRITE,
                    &key,
                    &payload,
                )?;

                // Write to global events DB
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
                    &global_seq.0,
                    &global_bytes,
                )?;

                results.push((stream_seq, global_seq));
                current_stream_seq += 1;
                current_global_seq += 1;
            }
            Ok::<_, StreamError>(())
        })?;

        // Update stream metadata
        self.core
            .meta_db
            .put(&mut wtxn, &stream_id.0, &current_stream_seq)?;

        timed_dbg!("batch_commit", wtxn.commit())?;

        self.next_global_seq = current_global_seq;
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
    ) -> Result<(StreamSequence, GlobalSequence), StreamError>
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
    ) -> Result<(StreamSequence, GlobalSequence), StreamError>
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
    ) -> Result<Vec<(StreamSequence, GlobalSequence)>, StreamError>
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
            Ok::<_, StreamError>(payloads)
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
    ) -> Result<Vec<(StreamSequence, GlobalSequence)>, StreamError>
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
            Ok::<_, StreamError>(payloads)
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
    /// Returns the raw bytes of the event payload.
    pub fn get_bytes(
        &self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
    ) -> Result<Option<Vec<u8>>, StreamError> {
        let key = StreamKey::new(stream_id, stream_seq);
        let rtxn = self.core.env.read_txn()?;
        let bytes = self.core.events_db.get(&rtxn, &key)?;
        Ok(bytes.map(|b| b.to_vec()))
    }

    /// Get a single event and return an archived view with validation.
    pub fn get_archived(
        &self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
    ) -> Result<Option<(EventMeta, rkyv::util::AlignedVec<16>)>, StreamError>
    where
        T: rkyv::Archive,
        rkyv::Archived<T>: rkyv::Portable
            + for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            >,
    {
        let Some(bytes) = self.get_bytes(stream_id, stream_seq)? else {
            return Ok(None);
        };

        // Validate the archived data
        let _ = rkyv::access::<rkyv::Archived<T>, rkyv::rancor::Error>(&bytes)?;

        // Get global sequence from the meta (we need to look it up)
        // For now, we return 0 as we don't store the global_seq mapping per-event
        // This could be enhanced with a reverse index if needed
        let meta = EventMeta {
            stream_id,
            stream_seq,
            global_seq: GlobalSequence(0), // Would need reverse lookup
        };

        let mut aligned = rkyv::util::AlignedVec::new();
        aligned.extend_from_slice(&bytes);
        Ok(Some((meta, aligned)))
    }

    /// Create a reader for this stream
    pub fn reader(&self) -> StreamReader<T> {
        StreamReader {
            core: Arc::clone(&self.core),
            scratch: rkyv::util::AlignedVec::new(),
            _marker: PhantomData,
        }
    }
}

/// A cheap, cloneable reader view for a stream.
pub struct StreamReader<T> {
    core: Arc<StreamCore>,
    scratch: rkyv::util::AlignedVec<16>,
    _marker: PhantomData<T>,
}

impl<T> StreamReader<T> {
    /// Get the stream name
    pub fn name(&self) -> &str {
        &self.core.stream_name
    }

    /// Get raw bytes for an event
    pub fn get_bytes(
        &mut self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
    ) -> Result<Option<&[u8]>, StreamError> {
        let key = StreamKey::new(stream_id, stream_seq);
        let rtxn = self.core.env.read_txn()?;
        let bytes = self.core.events_db.get(&rtxn, &key)?;
        match bytes {
            Some(b) => {
                self.scratch.clear();
                self.scratch.extend_from_slice(b);
                Ok(Some(&self.scratch))
            }
            None => Ok(None),
        }
    }

    /// Get an archived view with validation
    pub fn get_archived(
        &mut self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
    ) -> Result<Option<&rkyv::Archived<T>>, StreamError>
    where
        T: rkyv::Archive,
        rkyv::Archived<T>: rkyv::Portable
            + for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            >,
    {
        let Some(bytes) = self.get_bytes(stream_id, stream_seq)? else {
            return Ok(None);
        };
        let archived = rkyv::access::<rkyv::Archived<T>, rkyv::rancor::Error>(bytes)?;
        Ok(Some(archived))
    }

    /// Get an archived view without validation
    ///
    /// # Safety
    /// The bytes stored must be a valid archived `T`.
    pub unsafe fn get_archived_unchecked(
        &mut self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
    ) -> Result<Option<&rkyv::Archived<T>>, StreamError>
    where
        T: rkyv::Archive,
        rkyv::Archived<T>: rkyv::Portable,
    {
        let Some(bytes) = self.get_bytes(stream_id, stream_seq)? else {
            return Ok(None);
        };
        Ok(Some(unsafe {
            rkyv::access_unchecked::<rkyv::Archived<T>>(bytes)
        }))
    }

    /// Iterate all events for a specific stream_id, starting from an optional sequence.
    pub fn iter_stream(
        &self,
        stream_id: StreamId,
        from: Option<StreamSequence>,
    ) -> Result<StreamIterator<'_>, StreamError> {
        let rtxn = self.core.env.read_txn()?;
        let start_key = StreamKey::new(stream_id, from.unwrap_or(StreamSequence(0)));
        // We'll create a range from start_key to the maximum key for this stream_id
        let end_key = StreamKey::new(StreamId(stream_id.0 + 1), StreamSequence(0));

        Ok(StreamIterator {
            db: self.core.events_db,
            rtxn,
            start_key,
            end_key,
            stream_id,
        })
    }
}

impl<T> Clone for StreamReader<T> {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            scratch: rkyv::util::AlignedVec::new(),
            _marker: PhantomData,
        }
    }
}

/// Iterator over events in a single stream
pub struct StreamIterator<'a> {
    db: StreamEventsDb,
    rtxn: RoTxn<'a, WithTls>,
    start_key: StreamKey,
    end_key: StreamKey,
    stream_id: StreamId,
}

impl<'a> StreamIterator<'a> {
    /// Collect all events as raw bytes
    pub fn collect_bytes(self) -> Result<Vec<(StreamSequence, Vec<u8>)>, StreamError> {
        let mut results = Vec::new();
        let iter = self.db.range(&self.rtxn, &(self.start_key..self.end_key))?;
        for item in iter {
            let (key, value) = item?;
            if key.stream_id != self.stream_id {
                break;
            }
            results.push((key.stream_seq, value.to_vec()));
        }
        Ok(results)
    }
}
