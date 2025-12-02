//! # VarveDB
//!
//! VarveDB is a high-performance, embedded, append-only event store.
//!
//! ## Features
//! - Zero-copy reads using `rkyv`.
//! - ACID transactions via LMDB.
//! - Reactive event bus.
//! - Crypto-shredding support.

pub mod crypto;
pub mod engine;
pub mod error;
pub mod metrics;
pub mod storage;

pub use error::Error;
