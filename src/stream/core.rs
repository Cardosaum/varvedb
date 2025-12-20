// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Core stream state shared across stream handles.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use heed::Env;

use crate::types::{GlobalEventsDb, StreamIndexDb, StreamMetaDb};

#[cfg(feature = "notify")]
use crate::notify::WriteWatcher;

/// Shared core state for stream operations.
///
/// This is shared between the main Stream handle and any StreamReader clones.
pub(crate) struct StreamCore {
    pub env: Env,
    pub stream_name: String,
    pub index_db: StreamIndexDb,
    pub meta_db: StreamMetaDb,
    pub global_db: GlobalEventsDb,
    /// Shared global sequence counter (atomic for lock-free access)
    pub next_global_seq: Arc<AtomicU64>,
    /// Write notification watcher (optional, only when notify feature is enabled)
    #[cfg(feature = "notify")]
    pub watcher: WriteWatcher,
}
