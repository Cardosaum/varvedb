// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Snapshot write APIs.

use std::marker::PhantomData;

use rkyv::api::high::HighSerializer;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;

use crate::error::Result;
use crate::snapshot::advice::compute_advice;
use crate::snapshot::keys::{
    decode_cursor_from_data_key, encode_data_key, encode_global_scope_key, encode_scope_key,
    encode_stream_scope_key,
};
use crate::snapshot::store::{SnapshotDataDb, SnapshotLatestDb};
use crate::snapshot::{
    SnapshotAdvice, SnapshotGlobalScope, SnapshotPolicy, SnapshotRetention, SnapshotScope,
    SnapshotStreamScope,
};
use crate::types::{GlobalSequence, StreamId, StreamSequence};

pub trait CursorU64: Copy {
    fn to_u64(self) -> u64;
}

impl CursorU64 for StreamSequence {
    fn to_u64(self) -> u64 {
        self.0
    }
}

impl CursorU64 for GlobalSequence {
    fn to_u64(self) -> u64 {
        self.0
    }
}

/// Scoped snapshot writer that precomputes the scope key once.
pub struct ScopedSnapshotWriter<C> {
    writer: SnapshotWriter,
    scope_key: Vec<u8>,
    _marker: PhantomData<C>,
}

impl<C> ScopedSnapshotWriter<C>
where
    C: CursorU64,
{
    pub fn due(&self, applied: C, policy: SnapshotPolicy) -> Result<SnapshotAdvice> {
        self.writer
            .due_scope_key(&self.scope_key, applied.to_u64(), policy)
    }

    pub fn save_bytes(&mut self, at: C, snapshot_bytes: &[u8]) -> Result<()> {
        self.writer
            .save_by_scope_key(&self.scope_key, at.to_u64(), snapshot_bytes)
    }

    pub fn save_bytes_if_due(
        &mut self,
        applied: C,
        policy: SnapshotPolicy,
        snapshot_bytes: &[u8],
    ) -> Result<SnapshotAdvice> {
        self.writer.save_bytes_if_due_by_scope_key(
            &self.scope_key,
            applied.to_u64(),
            policy,
            snapshot_bytes,
        )
    }

    pub fn save_bytes_if_due_and_prune(
        &mut self,
        applied: C,
        policy: SnapshotPolicy,
        retention: SnapshotRetention,
        snapshot_bytes: &[u8],
    ) -> Result<SnapshotAdvice> {
        let advice = self.save_bytes_if_due(applied, policy, snapshot_bytes)?;
        if advice.should_snapshot {
            let _ = self.prune(retention)?;
        }
        Ok(advice)
    }

    pub fn save_if_due<S>(
        &mut self,
        applied: C,
        policy: SnapshotPolicy,
        snapshot: &S,
    ) -> Result<SnapshotAdvice>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let advice = self.due(applied, policy)?;
        if !advice.should_snapshot {
            return Ok(advice);
        }

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(snapshot)?;
        self.save_bytes(applied, bytes.as_slice())?;
        Ok(advice)
    }

    pub fn save_if_due_and_prune<S>(
        &mut self,
        applied: C,
        policy: SnapshotPolicy,
        retention: SnapshotRetention,
        snapshot: &S,
    ) -> Result<SnapshotAdvice>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let advice = self.save_if_due(applied, policy, snapshot)?;
        if advice.should_snapshot {
            let _ = self.prune(retention)?;
        }
        Ok(advice)
    }

    pub fn prune(&mut self, retention: SnapshotRetention) -> Result<u64> {
        self.writer.prune_by_scope_key(&self.scope_key, retention)
    }

    pub fn into_inner(self) -> SnapshotWriter {
        self.writer
    }
}

/// A stream-scoped snapshot writer.
///
/// This precomputes the scope key once, improving ergonomics (fewer params) and
/// avoiding repeated allocations when snapshotting the same stream scope in a loop.
pub type StreamSnapshotWriter = ScopedSnapshotWriter<StreamSequence>;

/// A global/projection-scoped snapshot writer.
pub type GlobalSnapshotWriter = ScopedSnapshotWriter<GlobalSequence>;

/// Writer for snapshots.
pub struct SnapshotWriter {
    env: heed::Env,
    latest_db: SnapshotLatestDb,
    data_db: SnapshotDataDb,
}

impl SnapshotWriter {
    pub(crate) fn new(
        env: heed::Env,
        latest_db: SnapshotLatestDb,
        data_db: SnapshotDataDb,
    ) -> Self {
        Self {
            env,
            latest_db,
            data_db,
        }
    }

    /// Create a stream-scoped writer (recommended for tight loops).
    pub fn for_stream(self, stream_name: &str, stream_id: StreamId) -> StreamSnapshotWriter {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        ScopedSnapshotWriter {
            writer: self,
            scope_key,
            _marker: PhantomData,
        }
    }

    /// Create a stream-scoped writer from a borrowed scope wrapper.
    pub fn for_stream_scope(self, scope: SnapshotStreamScope<'_>) -> StreamSnapshotWriter {
        self.for_stream(scope.stream_name, scope.stream_id)
    }

    /// Create a global/projection-scoped writer (recommended for tight loops).
    pub fn for_global(self, projection_name: &str) -> GlobalSnapshotWriter {
        let scope_key = encode_global_scope_key(projection_name);
        ScopedSnapshotWriter {
            writer: self,
            scope_key,
            _marker: PhantomData,
        }
    }

    /// Create a global/projection-scoped writer from a borrowed scope wrapper.
    pub fn for_global_scope(self, scope: SnapshotGlobalScope<'_>) -> GlobalSnapshotWriter {
        self.for_global(scope.projection_name)
    }

    // ---------------------------------------------------------------------
    // Bytes-level API (always supported)
    // ---------------------------------------------------------------------

    /// Save a stream snapshot **only if** the policy says it's due.
    ///
    /// Important: this does **not** compute/serialize anything. You provide bytes.
    pub fn save_stream_bytes_if_due(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
        applied: StreamSequence,
        policy: SnapshotPolicy,
        snapshot_bytes: &[u8],
    ) -> Result<SnapshotAdvice> {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        self.save_bytes_if_due_by_scope_key(&scope_key, applied.0, policy, snapshot_bytes)
    }

    /// Save a stream snapshot if due, then prune old snapshots for the scope.
    pub fn save_stream_bytes_if_due_and_prune(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
        applied: StreamSequence,
        policy: SnapshotPolicy,
        retention: SnapshotRetention,
        snapshot_bytes: &[u8],
    ) -> Result<SnapshotAdvice> {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        self.save_bytes_if_due_and_prune_by_scope_key(
            &scope_key,
            applied.0,
            policy,
            retention,
            snapshot_bytes,
        )
    }

    /// Save a global snapshot **only if** the policy says it's due.
    ///
    /// Important: this does **not** compute/serialize anything. You provide bytes.
    pub fn save_global_bytes_if_due(
        &mut self,
        projection_name: &str,
        applied: GlobalSequence,
        policy: SnapshotPolicy,
        snapshot_bytes: &[u8],
    ) -> Result<SnapshotAdvice> {
        let scope_key = encode_global_scope_key(projection_name);
        self.save_bytes_if_due_by_scope_key(&scope_key, applied.0, policy, snapshot_bytes)
    }

    /// Save a global snapshot if due, then prune old snapshots for the scope.
    pub fn save_global_bytes_if_due_and_prune(
        &mut self,
        projection_name: &str,
        applied: GlobalSequence,
        policy: SnapshotPolicy,
        retention: SnapshotRetention,
        snapshot_bytes: &[u8],
    ) -> Result<SnapshotAdvice> {
        let scope_key = encode_global_scope_key(projection_name);
        self.save_bytes_if_due_and_prune_by_scope_key(
            &scope_key,
            applied.0,
            policy,
            retention,
            snapshot_bytes,
        )
    }

    /// Save a snapshot **only if** the policy says it's due (generic scope).
    ///
    /// `applied` is the cursor you have applied in your code.
    pub fn save_bytes_if_due(
        &mut self,
        scope: &SnapshotScope,
        applied: u64,
        policy: SnapshotPolicy,
        snapshot_bytes: &[u8],
    ) -> Result<SnapshotAdvice> {
        let scope_key = encode_scope_key(scope);
        self.save_bytes_if_due_by_scope_key(&scope_key, applied, policy, snapshot_bytes)
    }

    /// Save a snapshot if due (generic scope), then prune old snapshots for the scope.
    pub fn save_bytes_if_due_and_prune(
        &mut self,
        scope: &SnapshotScope,
        applied: u64,
        policy: SnapshotPolicy,
        retention: SnapshotRetention,
        snapshot_bytes: &[u8],
    ) -> Result<SnapshotAdvice> {
        let scope_key = encode_scope_key(scope);
        self.save_bytes_if_due_and_prune_by_scope_key(
            &scope_key,
            applied,
            policy,
            retention,
            snapshot_bytes,
        )
    }

    pub fn save_stream_bytes(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
        at: StreamSequence,
        snapshot_bytes: &[u8],
    ) -> Result<()> {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        self.save_by_scope_key(&scope_key, at.0, snapshot_bytes)
    }

    pub fn save_global_bytes(
        &mut self,
        projection_name: &str,
        at: GlobalSequence,
        snapshot_bytes: &[u8],
    ) -> Result<()> {
        let scope_key = encode_global_scope_key(projection_name);
        self.save_by_scope_key(&scope_key, at.0, snapshot_bytes)
    }

    pub fn save_bytes(
        &mut self,
        scope: &SnapshotScope,
        at: u64,
        snapshot_bytes: &[u8],
    ) -> Result<()> {
        let scope_key = encode_scope_key(scope);
        self.save_by_scope_key(&scope_key, at, snapshot_bytes)
    }

    // ---------------------------------------------------------------------
    // Typed API (rkyv convenience)
    // ---------------------------------------------------------------------

    /// Serialize and save a snapshot using rkyv.
    ///
    /// Note: this always serializes and writes. If you want “only if due”, use
    /// `save_stream_if_due{_and_prune}`.
    pub fn save_stream<S>(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
        at: StreamSequence,
        snapshot: &S,
    ) -> Result<()>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(snapshot)?;
        self.save_stream_bytes(stream_name, stream_id, at, bytes.as_slice())
    }

    /// Serialize and save a snapshot using rkyv.
    ///
    /// Note: this always serializes and writes. If you want “only if due”, use
    /// `save_global_if_due{_and_prune}`.
    pub fn save_global<S>(
        &mut self,
        projection_name: &str,
        at: GlobalSequence,
        snapshot: &S,
    ) -> Result<()>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(snapshot)?;
        self.save_global_bytes(projection_name, at, bytes.as_slice())
    }

    /// Serialize and save a snapshot using rkyv.
    ///
    /// Note: this always serializes and writes. If you want “only if due”, use
    /// `save_if_due{_and_prune}`.
    pub fn save<S>(&mut self, scope: &SnapshotScope, at: u64, snapshot: &S) -> Result<()>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(snapshot)?;
        self.save_bytes(scope, at, bytes.as_slice())
    }

    /// Serialize and save a stream snapshot **only if** the policy says it's due.
    ///
    /// Important: this avoids the rkyv serialization cost unless the snapshot is due.
    pub fn save_stream_if_due<S>(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
        applied: StreamSequence,
        policy: SnapshotPolicy,
        snapshot: &S,
    ) -> Result<SnapshotAdvice>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        self.save_typed_if_due_by_scope_key(&scope_key, applied.0, policy, snapshot)
    }

    /// Serialize and save a stream snapshot if due, then prune old snapshots for the scope.
    pub fn save_stream_if_due_and_prune<S>(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
        applied: StreamSequence,
        policy: SnapshotPolicy,
        retention: SnapshotRetention,
        snapshot: &S,
    ) -> Result<SnapshotAdvice>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        self.save_typed_if_due_and_prune_by_scope_key(
            &scope_key, applied.0, policy, retention, snapshot,
        )
    }

    /// Serialize and save a global snapshot **only if** the policy says it's due.
    ///
    /// Important: this avoids the rkyv serialization cost unless the snapshot is due.
    pub fn save_global_if_due<S>(
        &mut self,
        projection_name: &str,
        applied: GlobalSequence,
        policy: SnapshotPolicy,
        snapshot: &S,
    ) -> Result<SnapshotAdvice>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let scope_key = encode_global_scope_key(projection_name);
        self.save_typed_if_due_by_scope_key(&scope_key, applied.0, policy, snapshot)
    }

    /// Serialize and save a global snapshot if due, then prune old snapshots for the scope.
    pub fn save_global_if_due_and_prune<S>(
        &mut self,
        projection_name: &str,
        applied: GlobalSequence,
        policy: SnapshotPolicy,
        retention: SnapshotRetention,
        snapshot: &S,
    ) -> Result<SnapshotAdvice>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let scope_key = encode_global_scope_key(projection_name);
        self.save_typed_if_due_and_prune_by_scope_key(
            &scope_key, applied.0, policy, retention, snapshot,
        )
    }

    /// Serialize and save a snapshot **only if** the policy says it's due (generic scope).
    ///
    /// Important: this avoids the rkyv serialization cost unless the snapshot is due.
    pub fn save_if_due<S>(
        &mut self,
        scope: &SnapshotScope,
        applied: u64,
        policy: SnapshotPolicy,
        snapshot: &S,
    ) -> Result<SnapshotAdvice>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let scope_key = encode_scope_key(scope);
        self.save_typed_if_due_by_scope_key(&scope_key, applied, policy, snapshot)
    }

    /// Serialize and save a snapshot if due (generic scope), then prune old snapshots for the scope.
    pub fn save_if_due_and_prune<S>(
        &mut self,
        scope: &SnapshotScope,
        applied: u64,
        policy: SnapshotPolicy,
        retention: SnapshotRetention,
        snapshot: &S,
    ) -> Result<SnapshotAdvice>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let scope_key = encode_scope_key(scope);
        self.save_typed_if_due_and_prune_by_scope_key(
            &scope_key, applied, policy, retention, snapshot,
        )
    }

    // ---------------------------------------------------------------------
    // Retention / pruning
    // ---------------------------------------------------------------------

    pub fn prune_stream(
        &mut self,
        stream_name: &str,
        stream_id: StreamId,
        retention: SnapshotRetention,
    ) -> Result<u64> {
        let scope_key = encode_stream_scope_key(stream_name, stream_id);
        self.prune_by_scope_key(&scope_key, retention)
    }

    pub fn prune_global(
        &mut self,
        projection_name: &str,
        retention: SnapshotRetention,
    ) -> Result<u64> {
        let scope_key = encode_global_scope_key(projection_name);
        self.prune_by_scope_key(&scope_key, retention)
    }

    pub fn prune(&mut self, scope: &SnapshotScope, retention: SnapshotRetention) -> Result<u64> {
        let scope_key = encode_scope_key(scope);
        self.prune_by_scope_key(&scope_key, retention)
    }

    // ---------------------------------------------------------------------
    // Internal helpers
    // ---------------------------------------------------------------------

    fn save_by_scope_key(
        &mut self,
        scope_key: &Vec<u8>,
        at: u64,
        snapshot_bytes: &[u8],
    ) -> Result<()> {
        let data_key = encode_data_key(scope_key.as_slice(), at);

        let mut wtxn = self.env.write_txn()?;

        // Write snapshot payload (overwrite allowed).
        self.data_db.put(&mut wtxn, &data_key, snapshot_bytes)?;

        // Update latest cursor only if this snapshot is newer.
        let current_latest = self.latest_db.get(&wtxn, scope_key)?;
        if current_latest.map_or(true, |c| at >= c) {
            self.latest_db.put(&mut wtxn, scope_key, &at)?;
        }

        wtxn.commit()?;
        Ok(())
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

    fn save_bytes_if_due_by_scope_key(
        &mut self,
        scope_key: &Vec<u8>,
        applied: u64,
        policy: SnapshotPolicy,
        snapshot_bytes: &[u8],
    ) -> Result<SnapshotAdvice> {
        let advice = self.due_scope_key(scope_key, applied, policy)?;
        if !advice.should_snapshot {
            return Ok(advice);
        }
        self.save_by_scope_key(scope_key, applied, snapshot_bytes)?;
        Ok(advice)
    }

    fn save_bytes_if_due_and_prune_by_scope_key(
        &mut self,
        scope_key: &Vec<u8>,
        applied: u64,
        policy: SnapshotPolicy,
        retention: SnapshotRetention,
        snapshot_bytes: &[u8],
    ) -> Result<SnapshotAdvice> {
        let advice =
            self.save_bytes_if_due_by_scope_key(scope_key, applied, policy, snapshot_bytes)?;
        if advice.should_snapshot {
            let _ = self.prune_by_scope_key(scope_key, retention)?;
        }
        Ok(advice)
    }

    fn save_typed_if_due_by_scope_key<S>(
        &mut self,
        scope_key: &Vec<u8>,
        applied: u64,
        policy: SnapshotPolicy,
        snapshot: &S,
    ) -> Result<SnapshotAdvice>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let advice = self.due_scope_key(scope_key, applied, policy)?;
        if !advice.should_snapshot {
            return Ok(advice);
        }

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(snapshot)?;
        self.save_by_scope_key(scope_key, applied, bytes.as_slice())?;
        Ok(advice)
    }

    fn save_typed_if_due_and_prune_by_scope_key<S>(
        &mut self,
        scope_key: &Vec<u8>,
        applied: u64,
        policy: SnapshotPolicy,
        retention: SnapshotRetention,
        snapshot: &S,
    ) -> Result<SnapshotAdvice>
    where
        S: for<'a> rkyv::Serialize<
            HighSerializer<AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>,
        >,
    {
        let advice = self.save_typed_if_due_by_scope_key(scope_key, applied, policy, snapshot)?;
        if advice.should_snapshot {
            let _ = self.prune_by_scope_key(scope_key, retention)?;
        }
        Ok(advice)
    }

    fn prune_by_scope_key(
        &mut self,
        scope_key: &Vec<u8>,
        retention: SnapshotRetention,
    ) -> Result<u64> {
        let keep_last = match retention {
            SnapshotRetention::KeepLast(n) => n.get(),
        };

        // Collect all snapshot keys/cursors for this scope.
        let rtxn = self.env.read_txn()?;
        let iter = self.data_db.range(&rtxn, &(scope_key.clone()..))?;
        let mut entries: Vec<(u64, Vec<u8>)> = Vec::new();

        for item in iter {
            let (key, _val) = item?;
            if !key.as_ref().starts_with(scope_key.as_slice()) {
                break;
            }
            let Some(cursor) = decode_cursor_from_data_key(scope_key.as_slice(), key.as_ref())
            else {
                continue;
            };
            entries.push((cursor, key.as_ref().to_vec()));
        }

        drop(rtxn);

        if entries.len() as u64 <= keep_last {
            return Ok(0);
        }

        entries.sort_by_key(|(cursor, _)| *cursor);
        let to_delete = entries.len() - (keep_last as usize);

        let mut wtxn = self.env.write_txn()?;
        let mut deleted = 0u64;

        for (_cursor, key) in entries.iter().take(to_delete) {
            // Ignore missing keys (best-effort); LMDB delete returns bool.
            let _ = self.data_db.delete(&mut wtxn, key)?;
            deleted += 1;
        }

        // Recompute latest cursor for scope (cheap because retention keeps this small).
        let mut newest: Option<u64> = None;
        for (cursor, _key) in entries.iter().skip(to_delete) {
            newest = Some(match newest {
                Some(prev) => prev.max(*cursor),
                None => *cursor,
            });
        }

        match newest {
            Some(cursor) => {
                self.latest_db.put(&mut wtxn, scope_key, &cursor)?;
            }
            None => {
                let _ = self.latest_db.delete(&mut wtxn, scope_key)?;
            }
        }

        wtxn.commit()?;
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::snapshot::{SnapshotPolicy, SnapshotRetention, SnapshotStore, SnapshotStreamScope};
    use crate::types::{GlobalSequence, StreamId, StreamSequence};

    #[test]
    fn bytes_if_due_writes_only_when_due() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SnapshotStore::open(dir.path()).expect("open");

        let policy = SnapshotPolicy::every_n_events(NonZeroU64::new(2).unwrap());
        let retention = SnapshotRetention::KeepLast(NonZeroU64::new(1).unwrap());

        let mut writer = store.writer();
        let mut reader = store.reader();

        // applied=0 => events_since=1 => not due.
        let advice = writer
            .save_stream_bytes_if_due_and_prune(
                "orders",
                StreamId(1),
                StreamSequence(0),
                policy,
                retention,
                b"nope",
            )
            .unwrap();
        assert!(!advice.should_snapshot);
        assert!(reader
            .load_latest_stream_bytes("orders", StreamId(1))
            .unwrap()
            .is_none());

        // applied=1 => events_since=2 => due.
        let advice = writer
            .save_stream_bytes_if_due_and_prune(
                "orders",
                StreamId(1),
                StreamSequence(1),
                policy,
                retention,
                b"yes",
            )
            .unwrap();
        assert!(advice.should_snapshot);

        let (cursor, bytes) = reader
            .load_latest_stream_bytes("orders", StreamId(1))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(1));
        assert_eq!(bytes, b"yes");
    }

    #[test]
    fn scoped_stream_writer_reuses_scope_key_and_has_shorter_api() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SnapshotStore::open(dir.path()).expect("open");

        let policy = SnapshotPolicy::every_n_events(NonZeroU64::new(1).unwrap());

        let mut writer = store
            .writer()
            .for_stream_scope(SnapshotStreamScope::new("orders", StreamId(7)));
        let mut reader = store.reader();

        let advice = writer
            .save_bytes_if_due(StreamSequence(0), policy, b"v0")
            .unwrap();
        assert!(advice.should_snapshot);

        let (cursor, bytes) = reader
            .load_latest_stream_bytes("orders", StreamId(7))
            .unwrap()
            .unwrap();
        assert_eq!(cursor, StreamSequence(0));
        assert_eq!(bytes, b"v0");
    }

    #[test]
    fn prune_global_keeps_latest_cursor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SnapshotStore::open(dir.path()).expect("open");

        let mut writer = store.writer();
        let mut reader = store.reader();

        writer
            .save_global_bytes("proj", GlobalSequence(1), b"one")
            .unwrap();
        writer
            .save_global_bytes("proj", GlobalSequence(2), b"two")
            .unwrap();

        let deleted = writer
            .prune_global(
                "proj",
                SnapshotRetention::KeepLast(NonZeroU64::new(1).unwrap()),
            )
            .unwrap();
        assert_eq!(deleted, 1);

        let (cursor, bytes) = reader.load_latest_global_bytes("proj").unwrap().unwrap();
        assert_eq!(cursor, GlobalSequence(2));
        assert_eq!(bytes, b"two");
    }
}
