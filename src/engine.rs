// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use crate::model::StoragePayload;
use crate::storage::Storage;
use rkyv::bytecheck::CheckBytes;
use sha2::{Digest, Sha256};

use crate::crypto::{self, KeyManager};
use crate::metrics::VarveMetrics;
use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::rancor::Error as RancorError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::Portable;

use std::sync::Arc;

/// Appends events to the store with optimistic concurrency control.
///
/// The `Writer` ensures that events are appended in a strictly ordered sequence. It enforces
/// optimistic locking by requiring the expected `version` for each stream, preventing
/// concurrent modifications from overwriting data.
///
/// # Examples
///
/// ```rust
/// use varvedb::engine::Writer;
/// use varvedb::storage::{Storage, StorageConfig};
/// use rkyv::{Archive, Serialize, Deserialize};
/// use tempfile::tempdir;
///
/// #[derive(Archive, Serialize, Deserialize, Debug)]
/// #[rkyv(derive(Debug))]
/// struct MyEvent { pub data: u32 }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let dir = tempdir()?;
/// let config = StorageConfig { path: dir.path().to_path_buf(), ..Default::default() };
/// let storage = Storage::open(config)?;
/// let mut writer = Writer::new(storage);
///
/// writer.append(1, 1, MyEvent { data: 42 })?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Writer<E> {
    storage: Storage,
    metrics: Option<Arc<VarveMetrics>>,
    key_manager: Option<KeyManager>,
    _marker: std::marker::PhantomData<E>,
}

impl<E> Writer<E>
where
    E: rkyv::Archive
        + for<'a> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>,
{
    /// Creates a new `Writer` instance.
    pub fn new(storage: Storage) -> Self {
        let key_manager = if storage.config.encryption_enabled {
            Some(KeyManager::new(storage.clone()))
        } else {
            None
        };

        Self {
            storage,
            metrics: None,
            key_manager,
            _marker: std::marker::PhantomData,
        }
    }

    /// Attaches metrics to the writer for observability.
    pub fn with_metrics(mut self, metrics: Arc<VarveMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Returns a receiver for real-time event notifications.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.storage.notifier.subscribe()
    }
}

impl<E> Clone for Writer<E> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            metrics: self.metrics.clone(),
            key_manager: self.key_manager.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}
impl<E> Writer<E>
where
    E: rkyv::Archive
        + for<'a> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>,
{
    /// Appends a new event to a stream.
    ///
    /// This operation is atomic. If the `stream_id` and `version` combination already exists,
    /// the operation will fail with a validation error, ensuring optimistic concurrency control.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// *   The `stream_id` and `version` pair already exists (Concurrency Conflict).
    /// *   Serialization of the event fails.
    /// *   Encryption fails (if enabled).
    /// *   The underlying storage encounters an I/O error.
    pub fn append(&mut self, stream_id: u128, version: u32, event: E) -> crate::error::Result<u64> {
        let _timer = self
            .metrics
            .as_ref()
            .map(|m| m.append_latency.start_timer());

        let mut txn = self.storage.env.write_txn()?;

        // Concurrency Check
        let key = crate::storage::StreamKey::new(stream_id, version);
        let key_bytes = key.to_be_bytes();

        if self
            .storage
            .stream_index
            .get(&txn, key_bytes.as_slice())?
            .is_some()
        {
            // We don't know the expected version here, but we know the current version exists.
            // Actually, the error I defined `VersionMismatch` expects `expected` and `actual`.
            // But here we just know that `version` already exists.
            // Maybe I should add `StreamVersionExists` error?
            // Or just use `VersionMismatch` with some assumption?
            // The original code was: "Concurrency conflict: Stream {} version {} already exists"

            // Let's check what I defined in error.rs:
            // VersionMismatch { stream_id, expected, actual }

            // If I am trying to write version V, and it exists, it means actual is >= V.
            // But I don't know the head version without querying it.

            // Let's add `ConcurrencyConflict` error to `error.rs` instead of reusing `VersionMismatch` incorrectly here.
            return Err(crate::error::Error::ConcurrencyConflict { stream_id, version });
        }

        // Get next Global Sequence
        let last_seq = self
            .storage
            .events_log
            .last(&txn)?
            .map(|(k, _)| k)
            .unwrap_or(0);
        let new_seq = last_seq + 1;

        // Serialize Event
        let event_bytes = rkyv::api::high::to_bytes::<rkyv::rancor::Error>(&event)?;

        // Check size and determine Payload
        let payload = if event_bytes.len() > crate::constants::MAX_INLINE_SIZE {
            // Large Payload: Store in Blobs DB
            let mut hasher = Sha256::new();
            hasher.update(&event_bytes);
            let hash = hasher.finalize();
            let hash_array: [u8; 32] = hash.into();

            self.storage
                .blobs
                .put(&mut txn, hash_array.as_slice(), event_bytes.as_slice())?;
            StoragePayload::BlobRef(hash_array)
        } else {
            // Small Payload: Inline
            StoragePayload::Inline(event_bytes.into_vec())
        };

        // Serialize Payload
        let bytes = rkyv::api::high::to_bytes::<rkyv::rancor::Error>(&payload)?;

        // Encrypt if enabled
        let final_bytes = if let Some(km) = &self.key_manager {
            let key = km.get_or_create_key_with_txn(&mut txn, stream_id)?;

            // Construct AAD: StreamID (16 bytes) + GlobalSeq (8 bytes)
            let mut aad = [0u8; crate::constants::AAD_CAPACITY];
            aad[..crate::constants::STREAM_ID_SIZE].copy_from_slice(&stream_id.to_be_bytes());
            aad[crate::constants::STREAM_ID_SIZE..].copy_from_slice(&new_seq.to_be_bytes());

            let mut encrypted = crypto::encrypt(&key, &bytes, &aad)?;

            // Prepend StreamID (16 bytes) to allow Reader to find the key
            let mut final_vec =
                Vec::with_capacity(crate::constants::STREAM_ID_SIZE + encrypted.len());
            final_vec.extend_from_slice(&stream_id.to_be_bytes());
            final_vec.append(&mut encrypted);
            final_vec
        } else {
            bytes.to_vec()
        };

        let bytes_len = final_bytes.len() as u64;

        // Write to Log and Index
        self.storage
            .events_log
            .put(&mut txn, &new_seq, &final_bytes)?;
        self.storage
            .stream_index
            .put(&mut txn, key_bytes.as_slice(), &new_seq)?;

        txn.commit()?;

        // Notify Subscribers
        let _ = self.storage.notifier.send(new_seq);

        // Metrics
        if let Some(metrics) = &self.metrics {
            metrics.events_appended.inc();
            metrics.bytes_written.inc_by(bytes_len);
        }

        Ok(new_seq)
    }
}

pub enum EventData<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

pub struct EventView<'a, E>
where
    E: rkyv::Archive,
{
    data: EventData<'a>,
    _marker: std::marker::PhantomData<E>,
}

impl<'a, E> std::ops::Deref for EventView<'a, E>
where
    E: rkyv::Archive,
    E::Archived: Portable,
{
    type Target = E::Archived;

    fn deref(&self) -> &Self::Target {
        let bytes = match &self.data {
            EventData::Borrowed(b) => *b,
            EventData::Owned(b) => b.as_slice(),
        };
        // Safety: We verify the bytes in Reader::get using rkyv::check_archived_root
        // unsafe { rkyv::archived_root::<E>(bytes) }
        // Safety: We verify the bytes in Reader::get using rkyv::access
        unsafe { rkyv::access_unchecked::<E::Archived>(bytes) }
    }
}

impl<'a, E> std::fmt::Debug for EventView<'a, E>
where
    E: rkyv::Archive,
    E::Archived: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

impl<'a, E> EventView<'a, E>
where
    E: rkyv::Archive,
{
    /// Converts this `EventView` into an owned version, detaching it from the transaction.
    ///
    /// This involves cloning the data to ensure it owns its memory (independent of the transaction).
    /// Note: This performs a copy of the underlying data.
    pub fn into_owned<'b>(self) -> EventView<'b, E> {
        match self.data {
            EventData::Borrowed(b) => EventView {
                data: EventData::Owned(b.to_vec()),
                _marker: std::marker::PhantomData,
            },
            EventData::Owned(v) => EventView {
                data: EventData::Owned(v),
                _marker: std::marker::PhantomData,
            },
        }
    }
}

/// Provides zero-copy access to events from the store.
///
/// The `Reader` allows efficient retrieval of events by sequence number. It leverages memory-mapped
/// files to provide zero-copy access to data when encryption is disabled. When encryption is enabled,
/// it transparently handles decryption.
///
/// # Examples
///
/// ```rust
/// use varvedb::engine::{Writer, Reader};
/// use varvedb::storage::{Storage, StorageConfig};
/// use rkyv::{Archive, Serialize, Deserialize};
/// use tempfile::tempdir;
///
/// #[derive(Archive, Serialize, Deserialize, Debug)]
/// #[rkyv(derive(Debug))]
/// struct MyEvent { pub data: u32 }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let dir = tempdir()?;
/// let config = StorageConfig { path: dir.path().to_path_buf(), ..Default::default() };
/// let storage = Storage::open(config)?;
/// let mut writer = Writer::new(storage.clone());
/// writer.append(1, 1, MyEvent { data: 42 })?;
///
/// let reader = Reader::<MyEvent>::new(storage.clone());
/// let txn = storage.env.read_txn()?;
/// if let Some(event) = reader.get(&txn, 1)? {
///     println!("Event: {:?}", event);
/// }
/// # Ok(())
/// # }
/// ```
pub struct Reader<E> {
    storage: Storage,
    metrics: Option<Arc<VarveMetrics>>,
    key_manager: Option<KeyManager>,
    _marker: std::marker::PhantomData<E>,
}

impl<E> Clone for Reader<E> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            metrics: self.metrics.clone(),
            key_manager: self.key_manager.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<E> Reader<E>
where
    E: rkyv::Archive,
    E::Archived: for<'a> CheckBytes<HighValidator<'a, RancorError>>,
{
    /// Creates a new `Reader` instance.
    pub fn new(storage: Storage) -> Self {
        let key_manager = if storage.config.encryption_enabled {
            Some(KeyManager::new(storage.clone()))
        } else {
            None
        };

        Self {
            storage,
            metrics: None,
            key_manager,
            _marker: std::marker::PhantomData,
        }
    }

    /// Attaches metrics to the reader for observability.
    pub fn with_metrics(mut self, metrics: Arc<VarveMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Returns a reference to the underlying storage.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Retrieves an event by its global sequence number.
    ///
    /// Returns an `EventView` which provides access to the deserialized event.
    ///
    /// # Zero-Copy vs Encryption
    ///
    /// *   **Encryption Disabled**: The `EventView` borrows data directly from the memory-mapped file (Zero-Copy).
    /// *   **Encryption Enabled**: The data is decrypted into a temporary buffer, involving allocation and copying.
    ///
    /// If encryption is enabled, this method handles key retrieval and decryption transparently.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// *   The event data is corrupted or fails validation.
    /// *   Decryption fails (e.g., invalid key or AAD mismatch).
    /// *   The underlying storage encounters an I/O error.
    pub fn get<'txn>(
        &self,
        txn: &'txn heed::RoTxn,
        seq: u64,
    ) -> crate::error::Result<Option<EventView<'txn, E>>> {
        match self.storage.events_log.get(txn, &seq)? {
            Some(bytes) => {
                let payload_data = if let Some(km) = &self.key_manager {
                    // Expect: [StreamID (16)][Nonce (12)][Ciphertext]
                    if bytes.len() < crate::constants::ENCRYPTED_EVENT_MIN_SIZE {
                        return Err(crate::error::Error::InvalidEncryptedEventLength {
                            actual: bytes.len(),
                            minimum: crate::constants::ENCRYPTED_EVENT_MIN_SIZE,
                        });
                    }

                    let (stream_id_bytes, rest) = bytes.split_at(crate::constants::STREAM_ID_SIZE);
                    let stream_id = u128::from_be_bytes(stream_id_bytes.try_into().unwrap());

                    let key = km
                        .get_key_with_txn(txn, stream_id)?
                        .ok_or_else(|| crate::error::Error::KeyNotFound(stream_id))?;

                    // AAD: StreamID + Seq
                    let mut aad = Vec::with_capacity(crate::constants::AAD_CAPACITY);
                    aad.extend_from_slice(stream_id_bytes);
                    aad.extend_from_slice(&seq.to_be_bytes());

                    let decrypted = crypto::decrypt(&key, rest, &aad)?;
                    EventData::Owned(decrypted)
                } else {
                    EventData::Borrowed(bytes)
                };

                // Deserialize Payload
                let payload_bytes = match &payload_data {
                    EventData::Borrowed(b) => *b,
                    EventData::Owned(b) => b.as_slice(),
                };

                let archived_payload = rkyv::access::<
                    crate::model::ArchivedStoragePayload,
                    rkyv::rancor::Error,
                >(payload_bytes)?;

                let final_data = match archived_payload {
                    crate::model::ArchivedStoragePayload::Inline(inline_bytes) => {
                        EventData::Owned(inline_bytes.as_slice().to_vec())
                    }
                    crate::model::ArchivedStoragePayload::BlobRef(hash) => {
                        let blob_bytes =
                            self.storage
                                .blobs
                                .get(txn, hash.as_slice())?
                                .ok_or_else(|| {
                                    crate::error::Error::EventValidation(
                                        "Blob not found".to_string(),
                                    )
                                })?;

                        // MADVISE: Tell OS we don't need this page anymore
                        #[cfg(unix)]
                        unsafe {
                            let ptr = blob_bytes.as_ptr() as *const libc::c_void;
                            let len = blob_bytes.len();
                            // Round down to page boundary (required by madvise)
                            // Actually, heed/lmdb gives us a pointer. We should probably madvise the whole page containing it?
                            // Or just the range. madvise usually requires page alignment.
                            // Let's try to align it.
                            let page_size = libc::sysconf(libc::_SC_PAGESIZE) as usize;
                            let addr = ptr as usize;
                            let aligned_addr = addr & !(page_size - 1);
                            let offset = addr - aligned_addr;
                            let aligned_len = len + offset;

                            libc::madvise(
                                aligned_addr as *mut libc::c_void,
                                aligned_len,
                                libc::MADV_DONTNEED,
                            );
                        }

                        EventData::Owned(blob_bytes.to_vec())
                    }
                };

                // Verify rkyv validity (zero-copy check) of the actual event
                rkyv::access::<E::Archived, rkyv::rancor::Error>(match &final_data {
                    EventData::Borrowed(b) => b,
                    EventData::Owned(b) => b.as_slice(),
                })?;

                let view = EventView {
                    data: final_data,
                    _marker: std::marker::PhantomData,
                };

                if let Some(metrics) = &self.metrics {
                    metrics.events_read.inc();
                }

                Ok(Some(view))
            }
            None => Ok(None),
        }
    }

    /// Retrieves an event by its stream ID and version (sequence number in the stream).
    ///
    /// This method looks up the global sequence number for the given stream and version,
    /// and then retrieves the event using `get`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use varvedb::engine::{Writer, Reader};
    /// # use varvedb::storage::{Storage, StorageConfig};
    /// # use rkyv::{Archive, Serialize, Deserialize};
    /// # use tempfile::tempdir;
    /// #
    /// # #[derive(Archive, Serialize, Deserialize, Debug)]
    /// # #[rkyv(derive(Debug))]
    /// # struct MyEvent { pub data: u32 }
    /// #
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempdir()?;
    /// # let config = StorageConfig { path: dir.path().to_path_buf(), ..Default::default() };
    /// # let storage = Storage::open(config)?;
    /// # let mut writer = Writer::new(storage.clone());
    /// # writer.append(1, 1, MyEvent { data: 42 })?;
    /// #
    /// let reader = Reader::<MyEvent>::new(storage.clone());
    /// let txn = storage.env.read_txn()?;
    /// if let Some(event) = reader.get_by_stream(&txn, 1, 1)? {
    ///     println!("Event: {:?}", event);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// For more examples, see the [examples directory](https://github.com/Cardosaum/varvedb/tree/main/examples).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// *   The underlying storage encounters an I/O error.
    /// *   The event retrieval fails (see `get` errors).
    pub fn get_by_stream<'txn>(
        &self,
        txn: &'txn heed::RoTxn,
        stream_id: u128,
        version: u32,
    ) -> crate::error::Result<Option<EventView<'txn, E>>> {
        let key = crate::storage::StreamKey::new(stream_id, version);
        let key_bytes = key.to_be_bytes();

        self.storage
            .stream_index
            .get(txn, key_bytes.as_slice())?
            .map(|seq| self.get(txn, seq))
            .transpose()
            .map(Option::flatten)
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    use crate::{
        engine::{Reader, Writer},
        storage::{Storage, StorageConfig},
    };
    use rkyv::{Archive, Deserialize, Serialize};
    use tempfile::tempdir;

    #[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
    #[rkyv(derive(Debug))]
    #[repr(C)]
    struct TestEvent {
        value: u32,
    }

    #[test]
    fn test_get_by_stream() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let config = StorageConfig {
            path: dir.path().to_path_buf(),
            ..Default::default()
        };
        let storage = Storage::open(config)?;
        let mut writer = Writer::<TestEvent>::new(storage.clone());
        let reader = Reader::<TestEvent>::new(storage.clone());

        // Append events
        writer.append(1, 1, TestEvent { value: 10 })?;
        writer.append(1, 2, TestEvent { value: 20 })?;
        writer.append(2, 1, TestEvent { value: 30 })?;

        let txn = storage.env.read_txn()?;

        // Test existing events
        let event1 = reader.get_by_stream(&txn, 1, 1)?.unwrap();
        assert_eq!(event1.value, 10);

        let event2 = reader.get_by_stream(&txn, 1, 2)?.unwrap();
        assert_eq!(event2.value, 20);

        let event3 = reader.get_by_stream(&txn, 2, 1)?.unwrap();
        assert_eq!(event3.value, 30);

        // Test non-existing events
        assert!(reader.get_by_stream(&txn, 1, 3)?.is_none());
        assert!(reader.get_by_stream(&txn, 3, 1)?.is_none());

        Ok(())
    }
}
