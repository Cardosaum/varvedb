// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::Writer;
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
#[repr(C)]
pub struct ErrorEvent {
    pub id: u64,
}

#[test]
fn test_concurrency_conflict() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().join("error_test.mdb"),
        map_size: 10 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true,
        encryption_enabled: false,
        master_key: None,
    };
    let storage = Storage::open(config)?;
    let mut writer = Writer::<ErrorEvent>::new(storage.clone());

    let event = ErrorEvent { id: 1 };

    // First write should succeed
    writer.append(1, 1, event)?;

    // Second write with SAME stream_id and version should fail
    let event2 = ErrorEvent { id: 2 };
    let result = writer.append(1, 1, event2);

    assert!(result.is_err());
    let err = result.unwrap_err();
    match err {
        varvedb::error::Error::Validation(msg) => {
            assert!(msg.contains("Concurrency conflict"));
        }
        _ => panic!("Expected Validation error, got {:?}", err),
    }

    Ok(())
}
