// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! # VarveDB
//!
//! A high-performance, embedded, append-only event store.
//!
//! VarveDB provides a persistent, ACID-compliant event log optimized for high-throughput event sourcing.
//! It is built on top of [LMDB](http://www.lmdb.tech/doc/) (via `heed`) for reliable transaction support
//! and uses [rkyv](https://rkyv.org/) for zero-copy deserialization, ensuring minimal overhead during reads.
//!
//! ## Features
//!
//! *   **Zero-Copy Access**: Events are mapped directly from disk to memory, avoiding costly deserialization steps (when encryption is disabled).
//! *   **Optimistic Concurrency**: Writes are guarded by stream versions, preventing race conditions in concurrent environments.
//! *   **Reactive Interface**: Real-time event subscriptions via `tokio::watch`.
//! *   **Authenticated Encryption**: Optional per-stream encryption (AES-256-GCM) with AAD binding to prevent tampering and replay attacks.
//! *   **GDPR Compliance**: Built-in crypto-shredding support via key deletion.
//!
//! ## Example
//!
//! ```rust
//! use varvedb::engine::{Writer, Reader};
//! use varvedb::storage::{Storage, StorageConfig};
//! use rkyv::{Archive, Serialize, Deserialize};
//! use tempfile::tempdir;
//!
//! #[derive(Archive, Serialize, Deserialize, Debug)]
//! #[archive(check_bytes)]
//! #[archive_attr(derive(Debug))]
//! struct MyEvent {
//!     pub id: u32,
//!     pub data: String,
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempdir()?;
//! let config = StorageConfig {
//!     path: dir.path().to_path_buf(),
//!     ..Default::default()
//! };
//!
//! let storage = Storage::open(config)?;
//!
//! // Append an event
//! let mut writer = Writer::new(storage.clone());
//! writer.append(1, 1, MyEvent { id: 1, data: "Hello".to_string() })?;
//!
//! // Read the event back
//! let reader = Reader::<MyEvent>::new(storage.clone());
//! let txn = storage.env.read_txn()?;
//!
//! if let Some(event) = reader.get(&txn, 1)? {
//!     println!("Read event: {:?}", event);
//! }
//! # Ok(())
//! # }
//! ```

pub mod constants;
pub mod crypto;
pub mod engine;
pub mod error;
pub mod metrics;
pub mod model;
pub mod storage;

pub use error::Error;
