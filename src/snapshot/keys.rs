// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Snapshot key encoding utilities.
//!
//! Keys are byte-encoded in a stable, prefix-friendly form so we can range-scan
//! and prune snapshots efficiently.

use crate::snapshot::SnapshotScope;
use crate::types::StreamId;

// =============================================================================
// Binary key format (stable)
// =============================================================================
//
// We intentionally keep these formats simple and prefix-friendly:
//
// scope_key:
//   Stream: [kind=1:u8][name_len:u64_be][stream_name:bytes][stream_id:u64_be]
//   Global: [kind=2:u8][name_len:u64_be][projection_name:bytes]
//
// data_key:
//   [scope_key:bytes][cursor:u64_be]
//
// Notes:
// - name_len exists to make the format self-describing for future decoding/debugging.
//   (We don't currently decode names; we mainly rely on prefix scans + suffix cursor decode.)
// - All integers are big-endian so lexicographic ordering matches numeric ordering.

const SCOPE_KIND_STREAM: u8 = 1;
const SCOPE_KIND_GLOBAL: u8 = 2;

const U64_BE_BYTES: usize = core::mem::size_of::<u64>();
const STREAM_ID_BYTES: usize = U64_BE_BYTES;
const CURSOR_BYTES: usize = U64_BE_BYTES;
const SCOPE_KIND_BYTES: usize = core::mem::size_of::<u8>();
const NAME_LEN_BYTES: usize = U64_BE_BYTES;

const STREAM_SCOPE_FIXED_BYTES: usize = SCOPE_KIND_BYTES + NAME_LEN_BYTES + STREAM_ID_BYTES;
const GLOBAL_SCOPE_FIXED_BYTES: usize = SCOPE_KIND_BYTES + NAME_LEN_BYTES;

pub fn encode_scope_key(scope: &SnapshotScope) -> Vec<u8> {
    match scope {
        SnapshotScope::Stream {
            stream_name,
            stream_id,
        } => encode_stream_scope_key(stream_name, *stream_id),
        SnapshotScope::Global { projection_name } => encode_global_scope_key(projection_name),
    }
}

pub fn encode_stream_scope_key(stream_name: &str, stream_id: StreamId) -> Vec<u8> {
    let name_bytes = stream_name.as_bytes();
    let name_len: u64 = name_bytes.len() as u64;

    let mut buf = Vec::with_capacity(STREAM_SCOPE_FIXED_BYTES + name_bytes.len());
    buf.push(SCOPE_KIND_STREAM);
    buf.extend_from_slice(&name_len.to_be_bytes());
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(&stream_id.0.to_be_bytes());
    buf
}

pub fn encode_global_scope_key(projection_name: &str) -> Vec<u8> {
    let name_bytes = projection_name.as_bytes();
    let name_len: u64 = name_bytes.len() as u64;

    let mut buf = Vec::with_capacity(GLOBAL_SCOPE_FIXED_BYTES + name_bytes.len());
    buf.push(SCOPE_KIND_GLOBAL);
    buf.extend_from_slice(&name_len.to_be_bytes());
    buf.extend_from_slice(name_bytes);
    buf
}

pub fn encode_data_key(scope_key: &[u8], cursor: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(scope_key.len() + CURSOR_BYTES);
    buf.extend_from_slice(scope_key);
    buf.extend_from_slice(&cursor.to_be_bytes());
    buf
}

pub fn decode_cursor_from_data_key(scope_key: &[u8], full_key: &[u8]) -> Option<u64> {
    if full_key.len() != scope_key.len() + CURSOR_BYTES {
        return None;
    }
    if !full_key.starts_with(scope_key) {
        return None;
    }
    let tail = &full_key[full_key.len() - CURSOR_BYTES..];
    Some(u64::from_be_bytes(tail.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_scope_key_layout_is_stable() {
        let stream_name = "orders";
        let stream_id = StreamId(42);

        let key = encode_stream_scope_key(stream_name, stream_id);
        let name_bytes = stream_name.as_bytes();

        assert_eq!(key.len(), STREAM_SCOPE_FIXED_BYTES + name_bytes.len());
        assert_eq!(key[0], SCOPE_KIND_STREAM);

        let name_len_off = SCOPE_KIND_BYTES;
        let name_off = name_len_off + NAME_LEN_BYTES;
        let stream_id_off = name_off + name_bytes.len();

        let name_len = u64::from_be_bytes(
            key[name_len_off..name_len_off + NAME_LEN_BYTES]
                .try_into()
                .unwrap(),
        );
        assert_eq!(name_len, name_bytes.len() as u64);
        assert_eq!(&key[name_off..stream_id_off], name_bytes);

        let encoded_stream_id = u64::from_be_bytes(
            key[stream_id_off..stream_id_off + STREAM_ID_BYTES]
                .try_into()
                .unwrap(),
        );
        assert_eq!(encoded_stream_id, stream_id.0);
    }

    #[test]
    fn global_scope_key_layout_is_stable() {
        let projection = "users_projection";

        let key = encode_global_scope_key(projection);
        let name_bytes = projection.as_bytes();

        assert_eq!(key.len(), GLOBAL_SCOPE_FIXED_BYTES + name_bytes.len());
        assert_eq!(key[0], SCOPE_KIND_GLOBAL);

        let name_len_off = SCOPE_KIND_BYTES;
        let name_off = name_len_off + NAME_LEN_BYTES;

        let name_len = u64::from_be_bytes(
            key[name_len_off..name_len_off + NAME_LEN_BYTES]
                .try_into()
                .unwrap(),
        );
        assert_eq!(name_len, name_bytes.len() as u64);
        assert_eq!(&key[name_off..], name_bytes);
    }

    #[test]
    fn data_key_is_scope_prefix_plus_cursor_suffix() {
        let scope_key = encode_global_scope_key("proj");
        let cursor = 123u64;

        let data_key = encode_data_key(&scope_key, cursor);

        assert_eq!(data_key.len(), scope_key.len() + CURSOR_BYTES);
        assert!(data_key.starts_with(&scope_key));

        let decoded = decode_cursor_from_data_key(&scope_key, &data_key).unwrap();
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn decode_cursor_rejects_wrong_prefix_or_length() {
        let scope_key = encode_global_scope_key("proj");
        let other_scope_key = encode_global_scope_key("other");
        let cursor = 5u64;

        let data_key = encode_data_key(&scope_key, cursor);
        assert_eq!(
            decode_cursor_from_data_key(&other_scope_key, &data_key),
            None
        );

        // Wrong length
        assert_eq!(
            decode_cursor_from_data_key(&scope_key, &data_key[..data_key.len() - 1]),
            None
        );
    }

    #[test]
    fn data_keys_order_by_cursor_for_same_scope() {
        let scope_key = encode_global_scope_key("proj");

        let k1 = encode_data_key(&scope_key, 1);
        let k2 = encode_data_key(&scope_key, 2);
        let k100 = encode_data_key(&scope_key, 100);

        assert!(k1 < k2);
        assert!(k2 < k100);
    }
}
