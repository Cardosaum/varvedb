// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Error types for VarveDB.

use heed::Error as HeedError;
use rkyv::rancor::Error as RkyvError;

/// Main error type for VarveDB operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error from the underlying LMDB/heed database.
    #[error(transparent)]
    Heed(#[from] HeedError),

    /// Error from rkyv serialization/deserialization.
    #[error(transparent)]
    Rkyv(#[from] RkyvError),

    /// Database not found.
    #[error("Database not found: {0}")]
    DatabaseNotFound(String),

    /// Stream already exists.
    #[error("Stream already exists: {0}")]
    StreamAlreadyExists(String),

    /// Stream not found.
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    /// Event not found at global sequence.
    #[error("Event not found at global sequence {0}")]
    EventNotFound(u64),

    /// Error from logging subsystem.
    #[cfg(feature = "log")]
    #[error(transparent)]
    Log(#[from] LogError),
}

/// Error type for logging operations.
#[cfg(feature = "log")]
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error(transparent)]
    SetLogger(#[from] tracing_subscriber::util::TryInitError),
}

/// Result type alias for VarveDB operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(feature = "log")]
pub type LogResult<T> = std::result::Result<T, LogError>;
