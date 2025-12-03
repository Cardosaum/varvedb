#![no_main]

// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
use libfuzzer_sys::fuzz_target;
use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::Reader;
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(check_bytes)]
struct FuzzEvent {
    id: u32,
    data: Vec<u8>,
}

fuzz_target!(|data: &[u8]| {
    // Setup temporary storage
    let dir = match tempdir() {
        Ok(d) => d,
        Err(_) => return,
    };
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        map_size: 10 * 1024 * 1024, // 10MB
        ..Default::default()
    };

    let storage = match Storage::open(config) {
        Ok(s) => s,
        Err(_) => return,
    };

    // Inject random bytes into the event log
    let mut txn = match storage.env.write_txn() {
        Ok(t) => t,
        Err(_) => return,
    };
    let seq = 1u64;
    // We ignore the error here because the data might be too large for the map size or other LMDB constraints,
    // but we are interested in Reader crashes, not put failures.
    if let Ok(_) = storage.events_log.put(&mut txn, &seq, data) {
        if txn.commit().is_err() {
            return;
        }

        // Attempt to read and deserialize
        let reader = Reader::<FuzzEvent>::new(storage.clone());
        let txn = match storage.env.read_txn() {
            Ok(t) => t,
            Err(_) => return,
        };

        // This MUST NOT panic/crash
        let _ = reader.get(&txn, seq);
    }
});
