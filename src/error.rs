// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use thiserror::Error;

/// Custom error type for VarveDB operations.
#[derive(Error, Debug)]
pub enum Error {
    /// IO error occurred (e.g., file system issues).
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// LMDB storage error (via `heed`).
    #[error("LMDB error: {0}")]
    Heed(#[from] heed::Error),

    /// Serialization/Deserialization error (e.g., rkyv issues).
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Validation error (e.g., concurrency conflict, invalid input).
    #[error("Validation error: {0}")]
    Validation(String),

    /// Stream not found.
    #[error("Stream not found: {0}")]
    StreamNotFound(u128),

    /// Version mismatch.
    #[error("Version mismatch for stream {stream_id}: expected {expected}, got {actual}")]
    VersionMismatch {
        stream_id: u128,
        expected: u32,
        actual: u32,
    },

    /// Key not found.
    #[error("Key not found for stream {0}")]
    KeyNotFound(u128),

    /// Concurrency conflict.
    #[error("Concurrency conflict: Stream {stream_id} version {version} already exists")]
    ConcurrencyConflict { stream_id: u128, version: u32 },
}

pub type Result<T> = std::result::Result<T, Error>;
