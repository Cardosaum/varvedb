// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Custom heed codecs for the snapshot subsystem.
//!
//! `heed_types::Bytes` uses `EItem = [u8]` (unsized), which makes `Database::range`
//! unusable for prefix scans. We use a sized `Vec<u8>` key type instead.

use std::borrow::Cow;

use heed::{BoxedError, BytesDecode, BytesEncode};

/// Sized byte-vector key codec (encodes `Vec<u8>` keys, decodes as borrowed bytes).
pub enum SnapshotKeyCodec {}

impl<'a> BytesEncode<'a> for SnapshotKeyCodec {
    type EItem = Vec<u8>;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, BoxedError> {
        Ok(Cow::Borrowed(item.as_slice()))
    }
}

impl<'a> BytesDecode<'a> for SnapshotKeyCodec {
    type DItem = Cow<'a, [u8]>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
        Ok(Cow::Borrowed(bytes))
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use heed::types::Bytes;
    use heed::{BytesDecode, BytesEncode, Database, EnvOpenOptions};

    use super::SnapshotKeyCodec;

    #[test]
    fn codec_encodes_and_decodes_as_borrowed() {
        let key = vec![1u8, 2, 3, 4];

        let encoded = <SnapshotKeyCodec as BytesEncode<'_>>::bytes_encode(&key).unwrap();
        assert!(matches!(encoded, Cow::Borrowed(_)));
        assert_eq!(encoded.as_ref(), key.as_slice());

        let decoded = <SnapshotKeyCodec as BytesDecode<'_>>::bytes_decode(key.as_slice()).unwrap();
        assert!(matches!(decoded, Cow::Borrowed(_)));
        assert_eq!(decoded.as_ref(), key.as_slice());
    }

    #[test]
    fn codec_supports_range_iteration_with_vec_keys() {
        let dir = tempfile::tempdir().expect("tempdir");

        let env = unsafe {
            EnvOpenOptions::new()
                .read_txn_with_tls()
                .max_dbs(8)
                .open(dir.path())
                .expect("open env")
        };

        let db: Database<SnapshotKeyCodec, Bytes> = {
            let mut wtxn = env.write_txn().expect("wtxn");
            let db = env
                .create_database(&mut wtxn, Some("codec_test"))
                .expect("create db");
            wtxn.commit().expect("commit");
            db
        };

        {
            let mut wtxn = env.write_txn().expect("wtxn");
            db.put(&mut wtxn, &vec![1, 2, 0], b"a").expect("put");
            db.put(&mut wtxn, &vec![1, 2, 5], b"b").expect("put");
            db.put(&mut wtxn, &vec![2, 0, 0], b"c").expect("put");
            wtxn.commit().expect("commit");
        }

        let rtxn = env.read_txn().expect("rtxn");
        let mut iter = db.range(&rtxn, &(vec![1, 2, 0]..)).expect("range");

        let (k1, v1) = iter.next().unwrap().unwrap();
        assert_eq!(k1.as_ref(), &[1, 2, 0]);
        assert_eq!(v1, b"a");

        let (k2, v2) = iter.next().unwrap().unwrap();
        assert_eq!(k2.as_ref(), &[1, 2, 5]);
        assert_eq!(v2, b"b");
    }
}
