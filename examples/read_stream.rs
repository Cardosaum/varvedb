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

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
#[repr(C)]
pub struct TestEvent {
    pub id: u32,
    pub data: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().join("read_stream_example.mdb"),
        ..Default::default()
    };
    let storage = Storage::open(config)?;

    let mut writer = Writer::<TestEvent>::new(storage.clone());
    let reader = Reader::<TestEvent>::new(storage.clone());

    // Append events to Stream 1
    writer.append(
        1,
        1,
        TestEvent {
            id: 1,
            data: "Stream 1 - Event 1".to_string(),
        },
    )?;
    writer.append(
        1,
        2,
        TestEvent {
            id: 2,
            data: "Stream 1 - Event 2".to_string(),
        },
    )?;

    // Append events to Stream 2
    writer.append(
        2,
        1,
        TestEvent {
            id: 3,
            data: "Stream 2 - Event 1".to_string(),
        },
    )?;

    println!("Appended events.");

    let txn = storage.env.read_txn()?;

    // Verify Stream 1, Version 1
    let event1_v1 = reader
        .get_by_stream(&txn, 1, 1)?
        .expect("Should find Stream 1 Version 1");
    println!("Read Stream 1 Version 1: {:?}", event1_v1);
    assert_eq!(event1_v1.data, "Stream 1 - Event 1");

    // Verify Stream 1, Version 2
    let event1_v2 = reader
        .get_by_stream(&txn, 1, 2)?
        .expect("Should find Stream 1 Version 2");
    println!("Read Stream 1 Version 2: {:?}", event1_v2);
    assert_eq!(event1_v2.data, "Stream 1 - Event 2");

    // Verify Stream 2, Version 1
    let event2_v1 = reader
        .get_by_stream(&txn, 2, 1)?
        .expect("Should find Stream 2 Version 1");
    println!("Read Stream 2 Version 1: {:?}", event2_v1);
    assert_eq!(event2_v1.data, "Stream 2 - Event 1");

    // Verify Non-existent Stream
    let none_event = reader.get_by_stream(&txn, 3, 1)?;
    assert!(none_event.is_none(), "Should not find Stream 3 Version 1");
    println!("Correctly returned None for non-existent stream.");

    // Verify Non-existent Version
    let none_version = reader.get_by_stream(&txn, 1, 3)?;
    assert!(none_version.is_none(), "Should not find Stream 1 Version 3");
    println!("Correctly returned None for non-existent version.");

    println!("All verifications passed!");

    Ok(())
}
