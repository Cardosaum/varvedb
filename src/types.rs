// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use heed::byteorder::BigEndian;
use heed::types::{Bytes, U64};
use heed::{BoxedError, BytesDecode, BytesEncode, Database};
use std::borrow::Cow;

// =============================================================================
// Core Event Store Types
// =============================================================================

/// Unique identifier for a stream instance (e.g., a specific order, user, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct StreamId(pub u64);

impl From<u64> for StreamId {
    fn from(v: u64) -> Self {
        StreamId(v)
    }
}

impl From<StreamId> for u64 {
    fn from(v: StreamId) -> Self {
        v.0
    }
}

/// Sequence number within a specific stream (stream_name + stream_id)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamSequence(pub u64);

impl From<u64> for StreamSequence {
    fn from(v: u64) -> Self {
        StreamSequence(v)
    }
}

impl From<StreamSequence> for u64 {
    fn from(v: StreamSequence) -> Self {
        v.0
    }
}

/// Global sequence number across all streams
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct GlobalSequence(pub u64);

impl From<u64> for GlobalSequence {
    fn from(v: u64) -> Self {
        GlobalSequence(v)
    }
}

impl From<GlobalSequence> for u64 {
    fn from(v: GlobalSequence) -> Self {
        v.0
    }
}

/// An event with full metadata
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventMeta {
    pub stream_id: StreamId,
    pub stream_seq: StreamSequence,
    pub global_seq: GlobalSequence,
}

// =============================================================================
// LMDB Key Types
// =============================================================================

/// Composite key for stream events: [stream_id: u64 BE][stream_seq: u64 BE]
/// Total: 16 bytes, lexicographically ordered
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StreamKey {
    pub stream_id: StreamId,
    pub stream_seq: StreamSequence,
}

impl StreamKey {
    pub const SIZE: usize = 16;

    pub fn new(stream_id: StreamId, stream_seq: StreamSequence) -> Self {
        Self {
            stream_id,
            stream_seq,
        }
    }

    /// Create a prefix key for iterating all events of a stream_id
    pub fn prefix(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            stream_seq: StreamSequence(0),
        }
    }

    /// Encode to bytes (Big Endian for lexicographic ordering)
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.stream_id.0.to_be_bytes());
        buf[8..16].copy_from_slice(&self.stream_seq.0.to_be_bytes());
        buf
    }

    /// Decode from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::SIZE {
            return None;
        }
        let stream_id = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
        let stream_seq = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
        Some(Self {
            stream_id: StreamId(stream_id),
            stream_seq: StreamSequence(stream_seq),
        })
    }
}

/// heed codec for StreamKey
pub struct StreamKeyCodec;

impl<'a> BytesEncode<'a> for StreamKeyCodec {
    type EItem = StreamKey;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        Ok(Cow::Owned(item.to_bytes().to_vec()))
    }
}

impl<'a> BytesDecode<'a> for StreamKeyCodec {
    type DItem = StreamKey;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        StreamKey::from_bytes(bytes).ok_or_else(|| "Invalid StreamKey bytes".into())
    }
}

/// Key for stream metadata: [stream_id: u64 BE]
/// Maps to next_sequence for that stream
pub type StreamMetaKey = U64<BigEndian>;

// =============================================================================
// Global Event Record
// =============================================================================

/// Record stored in global_events DB
/// Contains: stream_name (length-prefixed) + stream_id + stream_seq + payload
#[derive(Debug, Clone)]
pub struct GlobalEventRecord {
    pub stream_name: String,
    pub stream_id: StreamId,
    pub stream_seq: StreamSequence,
    pub payload: Vec<u8>,
}

impl GlobalEventRecord {
    /// Encode the record to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let name_bytes = self.stream_name.as_bytes();
        let name_len = name_bytes.len() as u8;
        let mut buf = Vec::with_capacity(1 + name_bytes.len() + 8 + 8 + self.payload.len());
        buf.push(name_len);
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&self.stream_id.0.to_be_bytes());
        buf.extend_from_slice(&self.stream_seq.0.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Decode from bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }
        let name_len = bytes[0] as usize;
        if bytes.len() < 1 + name_len + 16 {
            return None;
        }
        let stream_name = std::str::from_utf8(&bytes[1..1 + name_len])
            .ok()?
            .to_string();
        let stream_id = u64::from_be_bytes(bytes[1 + name_len..1 + name_len + 8].try_into().ok()?);
        let stream_seq =
            u64::from_be_bytes(bytes[1 + name_len + 8..1 + name_len + 16].try_into().ok()?);
        let payload = bytes[1 + name_len + 16..].to_vec();
        Some(Self {
            stream_name,
            stream_id: StreamId(stream_id),
            stream_seq: StreamSequence(stream_seq),
            payload,
        })
    }
}

// =============================================================================
// Database Type Aliases
// =============================================================================

/// Legacy events database (for backwards compatibility during migration)
pub type SequenceKey = U64<BigEndian>;
pub type EventsDb = Database<SequenceKey, Bytes>;

/// Stream events database: StreamKey -> payload bytes
pub type StreamEventsDb = Database<StreamKeyCodec, Bytes>;

/// Global events database: global_seq -> GlobalEventRecord bytes
pub type GlobalEventsDb = Database<U64<BigEndian>, Bytes>;

/// Stream metadata database: stream_id -> next_sequence
pub type StreamMetaDb = Database<U64<BigEndian>, U64<BigEndian>>;
