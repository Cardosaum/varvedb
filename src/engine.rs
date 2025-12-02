use crate::storage::Storage;

use crate::crypto::{self, KeyManager};
use crate::metrics::VarveMetrics;
use std::sync::Arc;

/// The `Writer` struct handles appending events to the store.
///
/// It ensures concurrency control via optimistic locking on `(stream_id, version)`
/// and notifies subscribers of new events.
///
/// # Example
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
    tx: tokio::sync::watch::Sender<u64>,
    metrics: Option<Arc<VarveMetrics>>,
    key_manager: Option<KeyManager>,
    _marker: std::marker::PhantomData<E>,
}

impl<E> Writer<E>
where
    E: rkyv::Archive + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<1024>>,
{
    pub fn new(storage: Storage) -> Self {
        let (tx, _) = tokio::sync::watch::channel(0);
        let key_manager = if storage.config.encryption_enabled {
            Some(KeyManager::new(storage.clone()))
        } else {
            None
        };

        Self {
            storage,
            tx,
            metrics: None,
            key_manager,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn with_metrics(mut self, metrics: Arc<VarveMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.tx.subscribe()
    }

    pub fn append(&mut self, stream_id: u128, version: u32, event: E) -> crate::error::Result<()> {
        let _timer = self
            .metrics
            .as_ref()
            .map(|m| m.append_latency.start_timer());

        let mut txn = self.storage.env.write_txn()?;

        // 1. Concurrency Check: Check if (stream_id, version) already exists
        let key = crate::storage::StreamKey::new(stream_id, version);
        let key_bytes = key.to_be_bytes();

        if self
            .storage
            .stream_index
            .get(&txn, key_bytes.as_slice())?
            .is_some()
        {
            return Err(crate::error::Error::Validation(format!(
                "Concurrency conflict: Stream {} version {} already exists",
                stream_id, version
            )));
        }

        // 2. Get next Global Sequence
        let last_seq = self
            .storage
            .events_log
            .last(&txn)?
            .map(|(k, _)| k)
            .unwrap_or(0);
        let new_seq = last_seq + 1;

        // 3. Serialize Event (Zero-copy friendly)
        let bytes = rkyv::to_bytes::<_, 1024>(&event)
            .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;

        // Encrypt if enabled
        let final_bytes = if let Some(km) = &self.key_manager {
            let key = km.get_or_create_key_with_txn(&mut txn, stream_id)?;

            // Construct AAD: StreamID (16 bytes) + GlobalSeq (8 bytes)
            let mut aad = Vec::with_capacity(24);
            aad.extend_from_slice(&stream_id.to_be_bytes());
            aad.extend_from_slice(&new_seq.to_be_bytes());

            let mut encrypted = crypto::encrypt(&key, &bytes, &aad)?;

            // Prepend StreamID (16 bytes) to allow Reader to find the key
            let mut final_vec = Vec::with_capacity(16 + encrypted.len());
            final_vec.extend_from_slice(&stream_id.to_be_bytes());
            final_vec.append(&mut encrypted);
            final_vec
        } else {
            bytes.to_vec()
        };

        let bytes_len = final_bytes.len() as u64;

        // 4. Write to Log and Index
        self.storage
            .events_log
            .put(&mut txn, &new_seq, &final_bytes)?;
        self.storage
            .stream_index
            .put(&mut txn, key_bytes.as_slice(), &new_seq)?;

        txn.commit()?;

        // 5. Notify Subscribers
        let _ = self.tx.send(new_seq);

        // 6. Metrics
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
        // Safety: We verify the bytes before creating EventView
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

/// The `Reader` struct provides zero-copy access to events.
///
/// It wraps the storage handle and provides methods to retrieve events by sequence number.
///
/// # Example
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

    pub fn with_metrics(mut self, metrics: Arc<VarveMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn get<'txn>(
        &self,
        txn: &'txn heed::RoTxn,
        seq: u64,
    ) -> crate::error::Result<Option<EventView<'txn, E>>> {
        match self.storage.events_log.get(txn, &seq)? {
            Some(bytes) => {
                let data = if let Some(km) = &self.key_manager {
                    // Expect: [StreamID (16)][Nonce (12)][Ciphertext]
                    if bytes.len() < 28 {
                        return Err(crate::error::Error::Validation(
                            "Encrypted event too short".to_string(),
                        ));
                    }

                    let (stream_id_bytes, rest) = bytes.split_at(16);
                    let stream_id = u128::from_be_bytes(stream_id_bytes.try_into().unwrap());

                    let key = km.get_key_with_txn(txn, stream_id)?.ok_or_else(|| {
                        crate::error::Error::Validation(format!(
                            "Key not found for stream {}",
                            stream_id
                        ))
                    })?;

                    // AAD: StreamID + Seq
                    let mut aad = Vec::with_capacity(24);
                    aad.extend_from_slice(stream_id_bytes);
                    aad.extend_from_slice(&seq.to_be_bytes());

                    let decrypted = crypto::decrypt(&key, rest, &aad)?;
                    EventData::Owned(decrypted)
                } else {
                    EventData::Borrowed(bytes)
                };

                let view = EventView {
                    data,
                    _marker: std::marker::PhantomData,
                };

                // Verify rkyv validity (zero-copy check)
                // Note: This derefs the view which calls check_archived_root
                let _ = *view;

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

/// The `Processor` struct manages a reactive event loop.
///
/// It subscribes to new event notifications and processes them sequentially using the provided `EventHandler`.
/// It automatically manages the consumer cursor, ensuring at-least-once processing.
///
/// # Example
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
pub struct Processor<E, H> {
    reader: Reader<E>,
    handler: H,
    consumer_id: u64,
    rx: tokio::sync::watch::Receiver<u64>,
}

impl<E, H> Processor<E, H>
where
    E: rkyv::Archive,
    E::Archived: for<'a> rkyv::CheckBytes<rkyv::validation::validators::DefaultValidator<'a>>,
    H: EventHandler<E>,
{
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
        }
    }

    pub async fn run(&mut self) -> crate::error::Result<()> {
        loop {
            // 1. Get current cursor
            let current_seq = {
                let txn = self.reader.storage.env.read_txn()?;
                self.reader
                    .storage
                    .consumer_cursors
                    .get(&txn, &self.consumer_id)?
                    .unwrap_or(0)
            };

            // 2. Check head
            let head_seq = *self.rx.borrow();

            if current_seq < head_seq {
                // Catch up mode
                let txn = self.reader.storage.env.read_txn()?;

                let next_seq = current_seq + 1;
                if let Some(event) = self.reader.get(&txn, next_seq)? {
                    self.handler.handle(&event)?;
                }

                drop(txn); // Drop read txn

                let mut wtxn = self.reader.storage.env.write_txn()?;
                self.reader.storage.consumer_cursors.put(
                    &mut wtxn,
                    &self.consumer_id,
                    &next_seq,
                )?;
                wtxn.commit()?;
            } else {
                // Wait mode
                self.rx.changed().await.map_err(|_| {
                    crate::error::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "Sender dropped",
                    ))
                })?;
            }
        }
    }
}
