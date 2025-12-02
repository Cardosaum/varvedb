//! # VarveDB
//!
//! VarveDB is a high-performance, embedded, append-only event store written in Rust.
//! It leverages [LMDB](http://www.lmdb.tech/doc/) (via `heed`) for ACID transactions and
//! [rkyv](https://rkyv.org/) for zero-copy deserialization, making it suitable for
//! high-throughput event sourcing applications.
//!
//! ## Key Features
//!
//! - **Zero-Copy Reads**: Events are accessed directly from memory-mapped files without deserialization overhead.
//! - **ACID Transactions**: All writes are atomic, consistent, isolated, and durable.
//! - **Optimistic Concurrency**: Prevents write conflicts using stream versions.
//! - **Reactive Event Bus**: Subscribe to real-time event updates via `tokio::watch`.
//! - **Crypto-Shredding**: Built-in support for per-stream encryption and key deletion (GDPR compliance).
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
//! // 1. Open Storage
//! let storage = Storage::open(config)?;
//!
//! // 2. Create Writer
//! let mut writer = Writer::new(storage.clone());
//!
//! // 3. Append Event
//! let event = MyEvent { id: 1, data: "Hello".to_string() };
//! writer.append(1, 1, event)?;
//!
//! // 4. Read Event
//! let reader = Reader::<MyEvent>::new(storage.clone());
//! let txn = storage.env.read_txn()?;
//! if let Some(archived_event) = reader.get(&txn, 1)? {
//!     println!("Read event: {:?}", archived_event);
//! }
//! # Ok(())
//! # }
//! ```

pub mod crypto;
pub mod engine;
pub mod error;
pub mod metrics;
pub mod storage;

pub use error::Error;
