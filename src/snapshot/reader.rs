// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Snapshot read APIs.

use heed::Env;

use crate::error::Result;
use crate::snapshot::keys::{
    decode_cursor_from_data_key, encode_data_key, encode_global_scope_key, encode_scope_key,
    encode_stream_scope_key,
};
use crate::snapshot::store::{SnapshotDataDb, SnapshotLatestDb};
use crate::snapshot::{SnapshotCursor, SnapshotScope};
use crate::types::{GlobalSequence, StreamId, StreamSequence};

/// A cheap, cloneable reader view for snapshots.
pub struct SnapshotReader {
    env: Env,
    latest_db: SnapshotLatestDb,
    data_db: SnapshotDataDb,
    scratch: rkyv::util::AlignedVec<16>,
}

impl SnapshotReader {
    pub(crate) fn new(env: Env, latest_db: SnapshotLatestDb, data_db: SnapshotDataDb) -> Self {
        Self {
            env,
            latest_db,
            data_db,
            scratch: rkyv::util::AlignedVec::new(),
        }
    }

    /// Latest cursor for a scope, if any.
    pub fn latest_cursor(&self, scope: &SnapshotScope) -> Result<Option<SnapshotCursor>> {
        match scope {
            SnapshotScope::Stream {
                stream_name,
                stream_id,
            } => Ok(self
                .latest_stream_cursor(stream_name, *stream_id)?
                .map(SnapshotCursor::Stream)),
            SnapshotScope::Global { projection_name } => Ok(self
                .latest_global_cursor(projection_name)?
                .map(SnapshotCursor::Global)),
        }
    }

    pub fn latest_stream_cursor(
        &self,
        stream_name: &str,
        stream_id: StreamId,
    ) -> Result<Option<StreamSequence>> {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        let rtxn = self.env.read_txn()?;
        Ok(self
            .latest_db
            .get(&rtxn, &scope_key)?
            .map(StreamSequence))
    }

    pub fn latest_global_cursor(&self, projection_name: &str) -> Result<Option<GlobalSequence>> {
        let scope_key = encode_global_scope_key(projection_name);
        let rtxn = self.env.read_txn()?;
        Ok(self
            .latest_db
            .get(&rtxn, &scope_key)?
            .map(GlobalSequence))
    }

    /// Load latest snapshot bytes for a stream scope.
    pub fn load_latest_stream_bytes(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
    ) -> Result<Option<(StreamSequence, &[u8])>> {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        self.load_latest_by_scope_key(&scope_key).map(|opt| {
            opt.map(|(cursor, bytes)| (StreamSequence(cursor), bytes))
        })
    }

    /// Load latest snapshot bytes for a global scope.
    pub fn load_latest_global_bytes(
        &mut self,
        projection_name: &str,
    ) -> Result<Option<(GlobalSequence, &[u8])>> {
        let scope_key = encode_global_scope_key(projection_name);
        self.load_latest_by_scope_key(&scope_key).map(|opt| {
            opt.map(|(cursor, bytes)| (GlobalSequence(cursor), bytes))
        })
    }

    /// Load latest snapshot bytes for a generic scope.
    pub fn load_latest_bytes(
        &mut self,
        scope: &SnapshotScope,
    ) -> Result<Option<(SnapshotCursor, &[u8])>> {
        let scope_key = encode_scope_key(scope);
        let Some((cursor, bytes)) = self.load_latest_by_scope_key(&scope_key)? else {
            return Ok(None);
        };

        let cursor = match scope {
            SnapshotScope::Stream { .. } => SnapshotCursor::Stream(StreamSequence(cursor)),
            SnapshotScope::Global { .. } => SnapshotCursor::Global(GlobalSequence(cursor)),
        };

        Ok(Some((cursor, bytes)))
    }

    /// Load latest snapshot bytes at-or-before a stream cursor.
    pub fn load_stream_at_or_before_bytes(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
        target: StreamSequence,
    ) -> Result<Option<(StreamSequence, &[u8])>> {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        self.load_at_or_before_by_scope_key(&scope_key, target.0)
            .map(|opt| opt.map(|(c, b)| (StreamSequence(c), b)))
    }

    /// Load latest snapshot bytes at-or-before a global cursor.
    pub fn load_global_at_or_before_bytes(
        &mut self,
        projection_name: &str,
        target: GlobalSequence,
    ) -> Result<Option<(GlobalSequence, &[u8])>> {
        let scope_key = encode_global_scope_key(projection_name);
        self.load_at_or_before_by_scope_key(&scope_key, target.0)
            .map(|opt| opt.map(|(c, b)| (GlobalSequence(c), b)))
    }

    /// Load latest snapshot bytes at-or-before a generic cursor (u64).
    pub fn load_at_or_before_bytes(
        &mut self,
        scope: &SnapshotScope,
        target: u64,
    ) -> Result<Option<(SnapshotCursor, &[u8])>> {
        let scope_key = encode_scope_key(scope);
        let Some((cursor, bytes)) = self.load_at_or_before_by_scope_key(&scope_key, target)? else {
            return Ok(None);
        };

        let cursor = match scope {
            SnapshotScope::Stream { .. } => SnapshotCursor::Stream(StreamSequence(cursor)),
            SnapshotScope::Global { .. } => SnapshotCursor::Global(GlobalSequence(cursor)),
        };

        Ok(Some((cursor, bytes)))
    }

    // ---------------------------------------------------------------------
    // Archived helpers (typed reads)
    // ---------------------------------------------------------------------

    pub fn load_latest_stream_archived<S>(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
    ) -> Result<Option<(StreamSequence, &rkyv::Archived<S>)>>
    where
        S: rkyv::Archive,
        rkyv::Archived<S>: rkyv::Portable
            + for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            >,
    {
        let Some((cursor, bytes)) = self.load_latest_stream_bytes(stream_name, stream_id)? else {
            return Ok(None);
        };
        let archived = rkyv::access::<rkyv::Archived<S>, rkyv::rancor::Error>(bytes)?;
        Ok(Some((cursor, archived)))
    }

    pub fn load_latest_global_archived<S>(
        &mut self,
        projection_name: &str,
    ) -> Result<Option<(GlobalSequence, &rkyv::Archived<S>)>>
    where
        S: rkyv::Archive,
        rkyv::Archived<S>: rkyv::Portable
            + for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            >,
    {
        let Some((cursor, bytes)) = self.load_latest_global_bytes(projection_name)? else {
            return Ok(None);
        };
        let archived = rkyv::access::<rkyv::Archived<S>, rkyv::rancor::Error>(bytes)?;
        Ok(Some((cursor, archived)))
    }

    pub fn load_stream_at_or_before_archived<S>(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
        target: StreamSequence,
    ) -> Result<Option<(StreamSequence, &rkyv::Archived<S>)>>
    where
        S: rkyv::Archive,
        rkyv::Archived<S>: rkyv::Portable
            + for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            >,
    {
        let Some((cursor, bytes)) =
            self.load_stream_at_or_before_bytes(stream_name, stream_id, target)?
        else {
            return Ok(None);
        };
        let archived = rkyv::access::<rkyv::Archived<S>, rkyv::rancor::Error>(bytes)?;
        Ok(Some((cursor, archived)))
    }

    pub fn load_global_at_or_before_archived<S>(
        &mut self,
        projection_name: &str,
        target: GlobalSequence,
    ) -> Result<Option<(GlobalSequence, &rkyv::Archived<S>)>>
    where
        S: rkyv::Archive,
        rkyv::Archived<S>: rkyv::Portable
            + for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            >,
    {
        let Some((cursor, bytes)) = self.load_global_at_or_before_bytes(projection_name, target)?
        else {
            return Ok(None);
        };
        let archived = rkyv::access::<rkyv::Archived<S>, rkyv::rancor::Error>(bytes)?;
        Ok(Some((cursor, archived)))
    }

    // ---------------------------------------------------------------------
    // Internal helpers
    // ---------------------------------------------------------------------

    fn load_latest_by_scope_key(&mut self, scope_key: &[u8]) -> Result<Option<(u64, &[u8])>> {
        let scope_key_vec = scope_key.to_vec();
        let rtxn = self.env.read_txn()?;
        let Some(cursor) = self.latest_db.get(&rtxn, &scope_key_vec)? else {
            return Ok(None);
        };
        let data_key = encode_data_key(scope_key, cursor);
        let Some(bytes) = self.data_db.get(&rtxn, &data_key)? else {
            return Ok(None);
        };

        self.scratch.clear();
        self.scratch.extend_from_slice(bytes);
        Ok(Some((cursor, &self.scratch)))
    }

    fn load_at_or_before_by_scope_key(
        &mut self,
        scope_key: &[u8],
        target: u64,
    ) -> Result<Option<(u64, &[u8])>> {
        let rtxn = self.env.read_txn()?;
        let end_key = encode_data_key(scope_key, target);
        let mut iter = self.data_db.rev_range(&rtxn, &(..=end_key))?;

        let Some(item) = iter.next() else {
            return Ok(None);
        };
        let (key, bytes) = item?;

        if !key.as_ref().starts_with(scope_key) {
            return Ok(None);
        }
        let Some(cursor) = decode_cursor_from_data_key(scope_key, key.as_ref()) else {
            return Ok(None);
        };

        self.scratch.clear();
        self.scratch.extend_from_slice(bytes);
        Ok(Some((cursor, &self.scratch)))
    }
}

impl Clone for SnapshotReader {
    fn clone(&self) -> Self {
        Self {
            env: self.env.clone(),
            latest_db: self.latest_db,
            data_db: self.data_db,
            scratch: rkyv::util::AlignedVec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::snapshot::SnapshotStore;
    use crate::types::{StreamId, StreamSequence};

    #[test]
    fn load_at_or_before_does_not_bleed_across_scopes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SnapshotStore::open(dir.path()).expect("open");

        let mut writer = store.writer();
        let mut reader = store.reader();

        // Two different scopes.
        writer
            .save_stream_bytes("orders", StreamId(1), StreamSequence(10), b"orders@10")
            .unwrap();
        writer
            .save_stream_bytes("users", StreamId(1), StreamSequence(9), b"users@9")
            .unwrap();

        // Query orders scope at-or-before 9 should be None (orders has only 10).
        assert!(reader
            .load_stream_at_or_before_bytes("orders", StreamId(1), StreamSequence(9))
            .unwrap()
            .is_none());

        // Query orders at-or-before 10 should return orders@10.
        let (cursor, bytes) = reader
            .load_stream_at_or_before_bytes("orders", StreamId(1), StreamSequence(10))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(10));
        assert_eq!(bytes, b"orders@10");

        // Query users at-or-before 10 should return users@9 (latest <= 10).
        let (cursor, bytes) = reader
            .load_stream_at_or_before_bytes("users", StreamId(1), StreamSequence(10))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(9));
        assert_eq!(bytes, b"users@9");
    }
}


