// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Global event reading functionality.
//!
//! This module provides types for reading events across all streams in global order.

use heed::{Env, RoTxn, WithTls};

use crate::error::Result;
use crate::event::GlobalEvent;
use crate::types::{GlobalEventRecord, GlobalEventsDb, GlobalSequence};

/// A reader for iterating events across all streams in global order.
///
/// This provides a view of all events in the order they were committed,
/// regardless of which stream they belong to.
pub struct GlobalReader {
    pub(crate) env: Env,
    pub(crate) global_db: GlobalEventsDb,
    pub(crate) scratch: rkyv::util::AlignedVec<16>,
}

impl GlobalReader {
    /// Get a single event by global sequence.
    ///
    /// Returns `None` if the event does not exist at the given sequence number.
    pub fn get(&mut self, global_seq: GlobalSequence) -> Result<Option<GlobalEvent>> {
        let rtxn = self.env.read_txn()?;
        let bytes = self.global_db.get(&rtxn, &global_seq.0)?;
        match bytes {
            Some(b) => {
                self.scratch.clear();
                self.scratch.extend_from_slice(b);
                match GlobalEventRecord::from_bytes(&self.scratch) {
                    Some(record) => Ok(Some(GlobalEvent {
                        global_seq,
                        stream_name: record.stream_name,
                        stream_id: record.stream_id,
                        stream_seq: record.stream_seq,
                        payload: record.payload,
                    })),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    /// Get raw bytes for an event by global sequence.
    ///
    /// This is more efficient than `get` if you only need the payload bytes.
    pub fn get_bytes(&mut self, global_seq: GlobalSequence) -> Result<Option<&[u8]>> {
        let rtxn = self.env.read_txn()?;
        let bytes = self.global_db.get(&rtxn, &global_seq.0)?;
        match bytes {
            Some(b) => {
                self.scratch.clear();
                self.scratch.extend_from_slice(b);
                Ok(GlobalEventRecord::payload_from_bytes(&self.scratch))
            }
            None => Ok(None),
        }
    }

    /// Iterate all events from a given global sequence.
    ///
    /// Events are returned in global order starting from `from` (inclusive).
    pub fn iter_from(&self, from: GlobalSequence) -> Result<GlobalIterator<'_>> {
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

/// Iterator over global events.
///
/// Iterates events in global sequence order, starting from the sequence
/// specified when the iterator was created.
pub struct GlobalIterator<'a> {
    pub(crate) db: GlobalEventsDb,
    pub(crate) rtxn: RoTxn<'a, WithTls>,
    pub(crate) from: u64,
}

impl<'a> GlobalIterator<'a> {
    /// Collect all events as GlobalEvents.
    ///
    /// This consumes the iterator and returns all events as a vector.
    pub fn collect_all(self) -> Result<Vec<GlobalEvent>> {
        let mut results = Vec::new();
        let iter = self.db.range(&self.rtxn, &(self.from..))?;
        for item in iter {
            let (seq, bytes) = item?;
            if let Some(record) = GlobalEventRecord::from_bytes(bytes) {
                results.push(GlobalEvent {
                    global_seq: GlobalSequence(seq),
                    stream_name: record.stream_name,
                    stream_id: record.stream_id,
                    stream_seq: record.stream_seq,
                    payload: record.payload,
                });
            }
        }
        Ok(results)
    }

    /// Iterate and apply a function to each event.
    ///
    /// This consumes the iterator and calls the provided function for each event.
    pub fn for_each<F>(self, mut f: F) -> Result<()>
    where
        F: FnMut(GlobalEvent),
    {
        let iter = self.db.range(&self.rtxn, &(self.from..))?;
        for item in iter {
            let (seq, bytes) = item?;
            if let Some(record) = GlobalEventRecord::from_bytes(bytes) {
                f(GlobalEvent {
                    global_seq: GlobalSequence(seq),
                    stream_name: record.stream_name,
                    stream_id: record.stream_id,
                    stream_seq: record.stream_seq,
                    payload: record.payload,
                });
            }
        }
        Ok(())
    }
}
