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

    /// Event serialization failed.
    #[error("Event serialization failed: {0}")]
    EventSerialization(String),

    /// Event validation failed (e.g. invalid archive).
    #[error("Event validation failed: {0}")]
    EventValidation(String),

    /// Invalid encrypted event length.
    #[error("Invalid encrypted event length: expected at least {minimum}, got {actual}")]
    InvalidEncryptedEventLength { actual: usize, minimum: usize },

    /// Invalid key length.
    #[error("Invalid key length: expected {expected}, got {actual}")]
    InvalidKeyLength { actual: usize, expected: usize },

    /// Invalid ciphertext length.
    #[error("Invalid ciphertext length: expected at least {minimum}, got {actual}")]
    InvalidCiphertextLength { actual: usize, minimum: usize },

    /// Encryption failed.
    #[error("Encryption failed: {0}")]
    EncryptionError(String),

    /// Decryption failed.
    #[error("Decryption failed: {0}")]
    DecryptionError(String),

    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

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

impl rkyv::rancor::Fallible for Error {
    type Error = Self;
}

impl From<rkyv::rancor::Error> for Error {
    fn from(e: rkyv::rancor::Error) -> Self {
        Self::EventSerialization(e.to_string())
    }
}
