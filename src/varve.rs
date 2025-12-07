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
    pub fn iter(&self) -> crate::error::Result<Iter<'_, E, M>> {
        let txn = self.storage.env.read_txn()?;
        Ok(Iter {
            txn,
            reader: self.reader.clone(),
            current_seq: 1, // Events start at 1
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
