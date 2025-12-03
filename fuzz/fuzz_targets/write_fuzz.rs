#![no_main]

// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::Writer;
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, Arbitrary, Clone)]
#[archive(check_bytes)]
struct FuzzEvent {
    id: u32,
    data: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    stream_id: u128,
    version: u32,
    event: FuzzEvent,
}

fuzz_target!(|input: FuzzInput| {
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
    let mut writer = Writer::new(storage);

    // This MUST NOT panic/crash
    let _ = writer.append(input.stream_id, input.version, input.event);
});
