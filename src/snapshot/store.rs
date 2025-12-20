// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Snapshot store (separate LMDB environment).

use std::path::Path;

use heed::byteorder::BigEndian;
use heed::types::{Bytes, U64};
use heed::{Database, Env, EnvOpenOptions};

use crate::constants;
use crate::error::Result;
use crate::snapshot::advice::compute_advice;
use crate::snapshot::codecs::SnapshotKeyCodec;
use crate::snapshot::keys::{encode_global_scope_key, encode_stream_scope_key};
use crate::snapshot::{
    SnapshotAdvice, SnapshotPolicy, SnapshotReader, SnapshotScope, SnapshotWriter,
};
use crate::types::{GlobalSequence, StreamId, StreamSequence};

/// Configuration for opening a snapshot store.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotConfig {
    /// Maximum number of named databases in the snapshot environment.
    pub max_dbs: u32,
    /// Maximum size of the snapshot environment memory map.
    pub map_size: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            max_dbs: 8,
            map_size: 64 * 1024 * 1024, // 64MB
        }
    }
}

pub(crate) type SnapshotLatestDb = Database<SnapshotKeyCodec, U64<BigEndian>>;
pub(crate) type SnapshotDataDb = Database<SnapshotKeyCodec, Bytes>;

/// Handle to the snapshot subsystem.
///
/// This opens a separate LMDB environment dedicated to snapshots.
pub struct SnapshotStore {
    pub(crate) env: Env,
    pub(crate) latest_db: SnapshotLatestDb,
    pub(crate) data_db: SnapshotDataDb,
}

impl SnapshotStore {
    /// Open a snapshot store at `path` using default configuration.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_config(path, SnapshotConfig::default())
    }

    /// Open a snapshot store at `path` using custom configuration.
    pub fn with_config(path: impl AsRef<Path>, config: SnapshotConfig) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref())?;

        let env = unsafe {
            EnvOpenOptions::new()
                .read_txn_with_tls()
                .max_dbs(config.max_dbs)
                .map_size(config.map_size)
                .open(path)?
        };

        let (latest_db, data_db) = {
            let mut wtxn = env.write_txn()?;
            let latest_db: SnapshotLatestDb =
                env.create_database(&mut wtxn, Some(constants::SNAPSHOT_LATEST_DB_NAME))?;
            let data_db: SnapshotDataDb =
                env.create_database(&mut wtxn, Some(constants::SNAPSHOT_DATA_DB_NAME))?;
            wtxn.commit()?;
            (latest_db, data_db)
        };

        Ok(Self {
            env,
            latest_db,
            data_db,
        })
    }

    /// Create a cheap reader for snapshots.
    pub fn reader(&self) -> SnapshotReader {
        SnapshotReader::new(self.env.clone(), self.latest_db, self.data_db)
    }

    /// Create a writer for snapshots.
    pub fn writer(&self) -> SnapshotWriter {
        SnapshotWriter::new(self.env.clone(), self.latest_db, self.data_db)
    }

    /// Advisory: should you snapshot this stream scope given what you have *applied*?
    pub fn due_stream(
        &self,
        stream_name: &str,
        stream_id: StreamId,
        applied: StreamSequence,
        policy: SnapshotPolicy,
    ) -> Result<SnapshotAdvice> {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        self.due_scope_key(&scope_key, applied.0, policy)
    }

    /// Advisory: should you snapshot this global scope given what you have *applied*?
    pub fn due_global(
        &self,
        projection_name: &str,
        applied: GlobalSequence,
        policy: SnapshotPolicy,
    ) -> Result<SnapshotAdvice> {
        let scope_key = encode_global_scope_key(projection_name);
        self.due_scope_key(&scope_key, applied.0, policy)
    }

    /// Advisory: generic scope (cursor must match scope kind).
    pub fn due_scope(
        &self,
        scope: &SnapshotScope,
        applied: u64,
        policy: SnapshotPolicy,
    ) -> Result<SnapshotAdvice> {
        let scope_key = crate::snapshot::keys::encode_scope_key(scope);
        self.due_scope_key(&scope_key, applied, policy)
    }

    fn due_scope_key(
        &self,
        scope_key: &Vec<u8>,
        applied: u64,
        policy: SnapshotPolicy,
    ) -> Result<SnapshotAdvice> {
        let rtxn = self.env.read_txn()?;
        let last = self.latest_db.get(&rtxn, scope_key)?;
        Ok(compute_advice(last, applied, policy))
    }

    /// Convenience: open snapshot store at `<db_path>/snapshots`.
    pub fn open_default_under(db_path: impl AsRef<Path>) -> Result<Self> {
        Self::open(db_path.as_ref().join(constants::SNAPSHOT_DIR_NAME))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::constants;
    use crate::snapshot::{SnapshotPolicy, SnapshotStore};
    use crate::types::{StreamId, StreamSequence};

    #[test]
    fn open_default_under_creates_snapshot_dir() {
        let dir = tempfile::tempdir().expect("tempdir");

        let snapshot_dir = dir.path().join(constants::SNAPSHOT_DIR_NAME);
        assert!(!snapshot_dir.exists());

        let _store = SnapshotStore::open_default_under(dir.path()).expect("open_default_under");
        assert!(snapshot_dir.exists());
    }

    #[test]
    fn due_stream_without_snapshot_counts_from_zero() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SnapshotStore::open(dir.path()).expect("open");

        let policy = SnapshotPolicy::every_n_events(NonZeroU64::new(1).unwrap());
        let advice = store
            .due_stream("orders", StreamId(1), StreamSequence(0), policy)
            .expect("due_stream");

        // No snapshot yet => events_since = applied + 1.
        assert_eq!(advice.events_since_last_snapshot, 1);
        assert!(advice.should_snapshot);
    }
}
