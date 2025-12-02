// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use crate::error::Result;
use heed::{types::*, Database, Env, EnvOpenOptions};
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
///
/// This struct controls the physical layout and behavior of the underlying LMDB environment.
#[derive(Clone)]
pub struct StorageConfig {
    /// The directory where the database files will be stored.
    ///
    /// If `create_dir` is true, this directory will be created if it does not exist.
    pub path: PathBuf,

    /// The maximum size of the memory map in bytes.
    ///
    /// This value determines the maximum size of the database. It should be set large enough
    /// to accommodate the expected data volume, as resizing requires reopening the environment.
    /// The default is 10TB, which is effectively "unlimited" on 64-bit systems as it only
    /// reserves virtual address space, not physical RAM.
    pub map_size: usize,

    /// The maximum number of named databases.
    ///
    /// VarveDB uses a fixed number of internal databases (currently 4), but this can be
    /// increased if custom buckets are needed in the future.
    pub max_dbs: u32,

    /// Whether to create the directory if it doesn't exist.
    pub create_dir: bool,

    /// Enables encryption at rest for all events.
    ///
    /// When enabled, all event payloads are encrypted using AES-256-GCM before being written to disk.
    /// This requires a `master_key` to be provided.
    pub encryption_enabled: bool,

    /// The master key used to encrypt per-stream keys.
    ///
    /// Required if `encryption_enabled` is true. This key should be 32 bytes (256 bits) and
    /// must be kept secure. Losing this key will render the database unreadable.
    pub master_key: Option<[u8; 32]>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("varvedb.mdb"),
            map_size: 10 * 1024 * 1024 * 1024, // 10TB
            max_dbs: 10,
            create_dir: true,
            encryption_enabled: false,
            master_key: None,
        }
    }
}

/// A handle to the underlying storage engine.
///
/// `Storage` wraps the LMDB environment and provides access to the internal databases (buckets).
/// It is cheap to clone and shares the underlying environment handle, making it suitable for
/// concurrent access across threads.
#[derive(Clone)]
pub struct Storage {
    /// The raw LMDB environment.
    pub env: Env,
    // Buckets
    /// Maps Global Sequence Number (u64) -> Event Bytes.
    pub events_log: EventLogDb,
    /// Maps Stream ID + Version -> Global Sequence Number.
    pub stream_index: StreamIndexDb, // Key: StreamID+Ver (16+4 bytes)
    /// Maps Consumer ID -> Last Processed Global Sequence Number.
    pub consumer_cursors: ConsumerCursorDb,
    /// Maps Stream ID -> Encrypted Key (variable length).
    pub keystore: KeyStoreDb,
    /// The configuration used to open this storage.
    pub config: StorageConfig,
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
            config,
        })
    }
}
