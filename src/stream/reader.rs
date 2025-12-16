// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Stream reader and iterator types.

use std::marker::PhantomData;
use std::sync::Arc;

use heed::{RoTxn, WithTls};

use crate::error::Result;
use crate::stream::core::StreamCore;
use crate::types::{
    GlobalEventRecord, GlobalEventsDb, StreamId, StreamIndexDb, StreamKey, StreamSequence,
};

/// A cheap, cloneable reader view for a stream.
///
/// Readers can be cloned and shared across threads to provide concurrent
/// read access to a stream without blocking writers.
pub struct StreamReader<T> {
    pub(crate) core: Arc<StreamCore>,
    pub(crate) scratch: rkyv::util::AlignedVec<16>,
    pub(crate) _marker: PhantomData<T>,
}

impl<T> StreamReader<T> {
    /// Get the stream name.
    pub fn name(&self) -> &str {
        &self.core.stream_name
    }

    /// Get raw bytes for an event (fetched from global DB via index).
    ///
    /// Returns `None` if the event does not exist.
    pub fn get_bytes(
        &mut self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
    ) -> Result<Option<&[u8]>> {
        let index_key = StreamKey::new(stream_id, stream_seq);
        let rtxn = self.core.env.read_txn()?;

        // Look up global_seq from index
        let Some(global_seq) = self.core.index_db.get(&rtxn, &index_key)? else {
            return Ok(None);
        };

        // Fetch from global DB
        let Some(global_bytes) = self.core.global_db.get(&rtxn, &global_seq)? else {
            return Ok(None);
        };

        // Extract payload and copy to scratch buffer
        let Some(payload) = GlobalEventRecord::payload_from_bytes(global_bytes) else {
            return Ok(None);
        };

        self.scratch.clear();
        self.scratch.extend_from_slice(payload);
        Ok(Some(&self.scratch))
    }

    /// Get an archived view with validation.
    ///
    /// This validates the archived data before returning it.
    pub fn get_archived(
        &mut self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
    ) -> Result<Option<&rkyv::Archived<T>>>
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

    /// Get an archived view without validation.
    ///
    /// # Safety
    /// The bytes stored must be a valid archived `T`.
    pub unsafe fn get_archived_unchecked(
        &mut self,
        stream_id: StreamId,
        stream_seq: StreamSequence,
    ) -> Result<Option<&rkyv::Archived<T>>>
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
    ///
    /// Events are fetched from the global DB via the stream index.
    pub fn iter_stream(
        &self,
        stream_id: StreamId,
        from: Option<StreamSequence>,
    ) -> Result<StreamIterator<'_>> {
        let rtxn = self.core.env.read_txn()?;
        let start_key = StreamKey::new(stream_id, from.unwrap_or(StreamSequence(0)));
        let end_key = StreamKey::new(StreamId(stream_id.0 + 1), StreamSequence(0));

        Ok(StreamIterator {
            index_db: self.core.index_db,
            global_db: self.core.global_db,
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

/// Iterator over events in a single stream (fetches from global DB via index).
pub struct StreamIterator<'a> {
    pub(crate) index_db: StreamIndexDb,
    pub(crate) global_db: GlobalEventsDb,
    pub(crate) rtxn: RoTxn<'a, WithTls>,
    pub(crate) start_key: StreamKey,
    pub(crate) end_key: StreamKey,
    pub(crate) stream_id: StreamId,
}

impl<'a> StreamIterator<'a> {
    /// Collect all events as raw bytes (fetched from global DB).
    pub fn collect_bytes(self) -> Result<Vec<(StreamSequence, Vec<u8>)>> {
        let mut results = Vec::new();
        let iter = self
            .index_db
            .range(&self.rtxn, &(self.start_key..self.end_key))?;

        for item in iter {
            let (key, global_seq) = item?;
            if key.stream_id != self.stream_id {
                break;
            }

            // Fetch payload from global DB
            if let Some(global_bytes) = self.global_db.get(&self.rtxn, &global_seq)? {
                if let Some(payload) = GlobalEventRecord::payload_from_bytes(global_bytes) {
                    results.push((key.stream_seq, payload.to_vec()));
                }
            }
        }

        Ok(results)
    }

    /// Iterate and apply a function to each event.
    pub fn for_each<F>(self, mut f: F) -> Result<()>
    where
        F: FnMut(StreamSequence, &[u8]),
    {
        let iter = self
            .index_db
            .range(&self.rtxn, &(self.start_key..self.end_key))?;

        for item in iter {
            let (key, global_seq) = item?;
            if key.stream_id != self.stream_id {
                break;
            }

            // Fetch payload from global DB
            if let Some(global_bytes) = self.global_db.get(&self.rtxn, &global_seq)? {
                if let Some(payload) = GlobalEventRecord::payload_from_bytes(global_bytes) {
                    f(key.stream_seq, payload);
                }
            }
        }

        Ok(())
    }
}
