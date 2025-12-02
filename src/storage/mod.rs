use crate::error::Result;
use heed::{Database, Env, EnvOpenOptions, types::*};
use std::path::PathBuf;

// Type Aliases for readability
pub type EventLogDb = Database<U64<heed::byteorder::BE>, Bytes>;
pub type StreamIndexDb = Database<Bytes, U64<heed::byteorder::BE>>;
pub type ConsumerCursorDb = Database<U64<heed::byteorder::BE>, U64<heed::byteorder::BE>>;
pub type KeyStoreDb = Database<U128<heed::byteorder::BE>, Bytes>; // StreamID -> Key (32 bytes)

pub struct StreamKey {
    pub stream_id: u128,
    pub version: u32,
}

impl StreamKey {
    pub fn new(stream_id: u128, version: u32) -> Self {
        Self { stream_id, version }
    }

    pub fn to_be_bytes(&self) -> [u8; 20] {
        let mut buf = [0u8; 20];
        buf[0..16].copy_from_slice(&self.stream_id.to_be_bytes());
        buf[16..20].copy_from_slice(&self.version.to_be_bytes());
        buf
    }
}

/// Configuration for opening a VarveDB storage environment.
#[derive(Clone)]
pub struct StorageConfig {
    /// Path to the LMDB environment directory or file.
    pub path: PathBuf,
    /// Maximum size of the memory map in bytes (default: 10TB).
    pub map_size: usize,
    /// Maximum number of named databases (default: 10).
    pub max_dbs: u32,
    /// Whether to create the directory if it doesn't exist (default: true).
    pub create_dir: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("varvedb.mdb"),
            map_size: 10 * 1024 * 1024 * 1024, // 10TB
            max_dbs: 10,
            create_dir: true,
        }
    }
}

/// The main storage handle holding the LMDB environment and database handles.
///
/// This struct is cheap to clone (Arc-like semantics via `heed::Env`).
#[derive(Clone)]
pub struct Storage {
    /// Underlying LMDB environment.
    pub env: Env,
    // Buckets
    /// Maps Global Sequence Number (u64) -> Event Bytes.
    pub events_log: EventLogDb,
    /// Maps Stream ID + Version -> Global Sequence Number.
    pub stream_index: StreamIndexDb, // Key: StreamID+Ver (16+4 bytes)
    /// Maps Consumer ID -> Last Processed Global Sequence Number.
    pub consumer_cursors: ConsumerCursorDb,
    /// Maps Stream ID -> Encryption Key (32 bytes).
    pub keystore: KeyStoreDb,
}

impl Storage {
    pub fn open(config: StorageConfig) -> Result<Self> {
        if config.create_dir {
            std::fs::create_dir_all(&config.path)?;
        }

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(config.map_size)
                .max_dbs(config.max_dbs)
                .open(&config.path)?
        };

        let mut txn = env.write_txn()?;
        let events_log = env.create_database(&mut txn, Some("events_log"))?;
        let stream_index = env.create_database(&mut txn, Some("stream_index"))?;
        let consumer_cursors = env.create_database(&mut txn, Some("consumer_cursors"))?;
        let keystore = env.create_database(&mut txn, Some("keystore"))?;
        txn.commit()?;

        Ok(Self {
            env,
            events_log,
            stream_index,
            consumer_cursors,
            keystore,
        })
    }
}
