// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
struct MyEvent {
    pub data: u32,
}

#[test]
fn test_validation_error_on_corrupted_data() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().join("test_corrupt.mdb"),
        ..Default::default()
    };

    let storage = Storage::open(config)?;

    // 1. Write a valid event first to set up the stream
    let mut writer = Writer::<MyEvent>::new(storage.clone());
    writer.append(1, 1, MyEvent { data: 42 })?;

    // 2. Corrupt the data directly using heed
    // The writer appends: [StreamID (16)][Data] (if no encryption)
    // We want to overwrite the data part with garbage.
    let mut txn = storage.env.write_txn()?;
    let seq = 1u64;

    // Write definitely invalid data (too short)
    let garbage = vec![0u8; 1];

    storage.events_log.put(&mut txn, &seq, &garbage)?;
    txn.commit()?;

    // 3. Try to read it back
    let reader = Reader::<MyEvent>::new(storage.clone());
    let txn = storage.env.read_txn()?;

    let result = reader.get(&txn, 1);

    // 4. Assert that we get a Validation error
    match result {
        Err(varvedb::error::Error::EventValidation(_)) => {
            // Success! We caught the corruption.
        }
        Ok(_) => {
            panic!("Expected validation error, got Ok");
        }
        Err(e) => {
            panic!("Expected validation error, got {:?}", e);
        }
    }

    Ok(())
}
