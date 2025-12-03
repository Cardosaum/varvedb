// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use crate::model::Payload;
use crate::storage::Storage;
use sha2::{Digest, Sha256};

use crate::crypto::{self, KeyManager};
use crate::metrics::VarveMetrics;
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
/// #[archive(check_bytes)]
/// #[archive_attr(derive(Debug))]
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
pub struct Writer<E> {
    storage: Storage,
    metrics: Option<Arc<VarveMetrics>>,
    key_manager: Option<KeyManager>,
    _marker: std::marker::PhantomData<E>,
}

impl<E> Writer<E>
where
    E: rkyv::Archive
        + rkyv::Serialize<
            rkyv::ser::serializers::AllocSerializer<
                { crate::constants::DEFAULT_SERIALIZATION_BUFFER_SIZE },
            >,
        >,
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
        + rkyv::Serialize<
            rkyv::ser::serializers::AllocSerializer<
                { crate::constants::DEFAULT_SERIALIZATION_BUFFER_SIZE },
            >,
        >,
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
    pub fn append(&mut self, stream_id: u128, version: u32, event: E) -> crate::error::Result<()> {
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
        let event_bytes =
            rkyv::to_bytes::<_, { crate::constants::DEFAULT_SERIALIZATION_BUFFER_SIZE }>(&event)
                .map_err(|e| crate::error::Error::EventSerialization(e.to_string()))?;

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
            Payload::BlobRef(hash_array)
        } else {
            // Small Payload: Inline
            Payload::Inline(event_bytes.into_vec())
        };

        // Serialize Payload
        let bytes =
            rkyv::to_bytes::<_, { crate::constants::DEFAULT_SERIALIZATION_BUFFER_SIZE }>(&payload)
                .map_err(|e| crate::error::Error::EventSerialization(e.to_string()))?;

        // Encrypt if enabled
        let final_bytes = if let Some(km) = &self.key_manager {
            let key = km.get_or_create_key_with_txn(&mut txn, stream_id)?;

            // Construct AAD: StreamID (16 bytes) + GlobalSeq (8 bytes)
            let mut aad = Vec::with_capacity(crate::constants::AAD_CAPACITY);
            aad.extend_from_slice(&stream_id.to_be_bytes());
            aad.extend_from_slice(&new_seq.to_be_bytes());

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

        Ok(())
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
{
    type Target = E::Archived;

    fn deref(&self) -> &Self::Target {
        let bytes = match &self.data {
            EventData::Borrowed(b) => *b,
            EventData::Owned(b) => b.as_slice(),
        };
        // Safety: We verify the bytes in Reader::get using rkyv::check_archived_root
        unsafe { rkyv::archived_root::<E>(bytes) }
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
/// #[archive(check_bytes)]
/// #[archive_attr(derive(Debug))]
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

impl<E> Reader<E>
where
    E: rkyv::Archive,
    E::Archived: for<'a> rkyv::CheckBytes<rkyv::validation::validators::DefaultValidator<'a>>,
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

                let archived_payload = rkyv::check_archived_root::<Payload>(payload_bytes)
                    .map_err(|e| crate::error::Error::EventValidation(e.to_string()))?;

                let final_data = match archived_payload {
                    rkyv::Archived::<Payload>::Inline(inline_bytes) => {
                        EventData::Owned(inline_bytes.as_slice().to_vec())
                    }
                    rkyv::Archived::<Payload>::BlobRef(hash) => {
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
                rkyv::check_archived_root::<E>(match &final_data {
                    EventData::Borrowed(b) => b,
                    EventData::Owned(b) => b.as_slice(),
                })
                .map_err(|e| crate::error::Error::EventValidation(e.to_string()))?;

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
}

pub trait EventHandler<E>
where
    E: rkyv::Archive,
    E::Archived: for<'a> rkyv::CheckBytes<rkyv::validation::validators::DefaultValidator<'a>>,
{
    fn handle(&mut self, event: &E::Archived) -> crate::error::Result<()>;
}

/// Manages a reactive event loop for processing events.
///
/// The `Processor` subscribes to new event notifications and processes them sequentially
/// using the provided `EventHandler`. It automatically manages the consumer cursor,
/// ensuring at-least-once processing semantics.
///
/// # Examples
///
/// ```rust
/// use varvedb::engine::EventHandler;
/// use varvedb::storage::{Storage, StorageConfig};
/// use rkyv::{Archive, Serialize, Deserialize};
/// use tempfile::tempdir;
/// use std::sync::{Arc, Mutex};
///
/// #[derive(Archive, Serialize, Deserialize, Debug)]
/// #[archive(check_bytes)]
/// #[archive_attr(derive(Debug))]
/// struct MyEvent { pub data: u32 }
///
/// struct MyHandler { count: Arc<Mutex<u32>> }
/// impl EventHandler<MyEvent> for MyHandler {
///     fn handle(&mut self, event: &ArchivedMyEvent) -> varvedb::error::Result<()> {
///         let mut count = self.count.lock().unwrap();
///         *count += event.data;
///         Ok(())
///     }
/// }
/// ```
/// Configuration for the event processor.
#[derive(Clone, Copy, Debug)]
pub struct ProcessorConfig {
    /// Maximum number of events to process before committing the cursor.
    pub batch_size: usize,
    /// Maximum time to wait before committing the cursor, even if batch_size is not reached.
    pub batch_timeout: std::time::Duration,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: crate::constants::DEFAULT_BATCH_SIZE,
            batch_timeout: std::time::Duration::from_millis(
                crate::constants::DEFAULT_BATCH_TIMEOUT_MS,
            ),
        }
    }
}

pub struct Processor<E, H> {
    reader: Reader<E>,
    handler: H,
    consumer_id: u64,
    rx: tokio::sync::watch::Receiver<u64>,
    config: ProcessorConfig,
}

impl<E, H> Processor<E, H>
where
    E: rkyv::Archive,
    E::Archived: for<'a> rkyv::CheckBytes<rkyv::validation::validators::DefaultValidator<'a>>,
    H: EventHandler<E>,
{
    /// Creates a new `Processor`.
    ///
    /// *   `consumer_name`: A unique identifier for this consumer. Used to persist the cursor position.
    pub fn new(
        reader: Reader<E>,
        handler: H,
        consumer_name: &str,
        rx: tokio::sync::watch::Receiver<u64>,
    ) -> Self {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        consumer_name.hash(&mut hasher);
        let consumer_id = hasher.finish();

        Self {
            reader,
            handler,
            consumer_id,
            rx,
            config: ProcessorConfig::default(),
        }
    }

    /// Sets the configuration for the processor.
    pub fn with_config(mut self, config: ProcessorConfig) -> Self {
        self.config = config;
        self
    }

    /// Starts the event processing loop.
    ///
    /// This method runs indefinitely, processing events as they arrive.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// *   The underlying storage encounters an I/O error.
    /// *   The event handler returns an error.
    /// *   The event notification channel is closed.
    pub async fn run(&mut self) -> crate::error::Result<()> {
        // Initial cursor load
        let mut current_seq = {
            let txn = self.reader.storage.env.read_txn()?;
            self.reader
                .storage
                .consumer_cursors
                .get(&txn, &self.consumer_id)?
                .unwrap_or(0)
        };

        loop {
            let head_seq = *self.rx.borrow();

            if current_seq < head_seq {
                // Synchronously process the backlog to ensure RoTxn is not held across await
                current_seq = self.process_backlog(current_seq, head_seq)?;
            }

            // Check if we are caught up
            if current_seq >= *self.rx.borrow() {
                // Wait for new events
                self.rx.changed().await.map_err(|_| {
                    crate::error::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "Sender dropped",
                    ))
                })?;
            }
        }
    }

    /// Processes events from current_seq to target_seq using batched transactions.
    /// This method is synchronous to ensure RoTxn is not held across await points.
    fn process_backlog(
        &mut self,
        mut current_seq: u64,
        target_seq: u64,
    ) -> crate::error::Result<u64> {
        let mut pending_updates = 0;
        let mut last_commit = std::time::Instant::now();
        let mut read_txn: Option<heed::RoTxn> = None;

        while current_seq < target_seq {
            // Ensure we have a read transaction
            if read_txn.is_none() {
                read_txn = Some(self.reader.storage.env.read_txn()?);
            }
            let txn = read_txn.as_ref().unwrap();

            let mut processed_any = false;
            let mut reached_snapshot_end = false;

            // Inner loop: process batch
            while current_seq < target_seq {
                let next_seq = current_seq + 1;
                if let Some(event) = self.reader.get(txn, next_seq)? {
                    self.handler.handle(&event)?;
                    current_seq = next_seq;
                    pending_updates += 1;
                    processed_any = true;
                } else {
                    reached_snapshot_end = true;
                    break;
                }

                if pending_updates >= self.config.batch_size {
                    break;
                }
            }

            // Commit if needed
            if pending_updates >= self.config.batch_size
                || (processed_any && last_commit.elapsed() >= self.config.batch_timeout)
            {
                let mut wtxn = self.reader.storage.env.write_txn()?;
                self.reader.storage.consumer_cursors.put(
                    &mut wtxn,
                    &self.consumer_id,
                    &current_seq,
                )?;
                wtxn.commit()?;
                pending_updates = 0;
                last_commit = std::time::Instant::now();
            }

            if reached_snapshot_end {
                read_txn = None;
            }
        }

        // Final commit if any pending updates remain
        if pending_updates > 0 {
            let mut wtxn = self.reader.storage.env.write_txn()?;
            self.reader
                .storage
                .consumer_cursors
                .put(&mut wtxn, &self.consumer_id, &current_seq)?;
            wtxn.commit()?;
        }

        Ok(current_seq)
    }
}
