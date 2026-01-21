// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! VarveDB - A high-performance, embedded, append-only event store for Rust.

// Module declarations - order matters for dependencies
pub mod constants;
#[macro_use]
pub mod utils;
pub mod config;
pub mod error;
pub mod event;
pub mod global;
pub mod log;
#[cfg(feature = "notify")]
pub mod notify;
#[cfg(feature = "snapshot")]
pub mod snapshot;
pub mod stream;
pub mod types;
pub mod varve;

// Re-export main types for convenience and backward compatibility
pub use config::{PathCreation, VarveConfig};
pub use error::{Error, Result};
pub use event::GlobalEvent;
pub use global::{GlobalIterator, GlobalReader};
#[cfg(feature = "notify")]
pub use notify::WriteWatcher;
#[cfg(feature = "snapshot")]
pub use snapshot::{
    GlobalSnapshotWriter, SnapshotAdvice, SnapshotConfig, SnapshotCursor, SnapshotGlobalScope,
    SnapshotPolicy, SnapshotReader, SnapshotRetention, SnapshotScope, SnapshotStore,
    SnapshotStreamScope, SnapshotWriter, StreamSnapshotWriter,
};
pub use stream::{HighSerializer, LowSerializer, Stream, StreamIterator, StreamReader};
pub use types::{GlobalSequence, StreamId, StreamKey, StreamSequence};
pub use varve::Varve;
