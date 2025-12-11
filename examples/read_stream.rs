// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::traits::MetadataExt;
use varvedb::{ExpectedVersion, Payload, Varve};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct TestEvent {
    pub id: u32,
    pub data: String,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct TestMetadata {
    pub stream_id: u128,
    pub version: u32,
}

impl MetadataExt for TestMetadata {
    fn stream_id(&self) -> u128 {
        self.stream_id
    }
    fn version(&self) -> u32 {
        self.version
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let path = dir.path().join("read_stream_example.mdb");

    let mut db = Varve::open(&path)?;

    // Append events to Stream 1
    db.append(
        Payload::new(
            TestEvent {
                id: 1,
                data: "Stream 1 - Event 1".to_string(),
            },
            TestMetadata {
                stream_id: 1,
                version: 1,
            },
        ),
        ExpectedVersion::exact(1),
    )?;

    db.append(
        Payload::new(
            TestEvent {
                id: 2,
                data: "Stream 1 - Event 2".to_string(),
            },
            TestMetadata {
                stream_id: 1,
                version: 2,
            },
        ),
        ExpectedVersion::exact(2),
    )?;

    // Append events to Stream 2
    db.append(
        Payload::new(
            TestEvent {
                id: 3,
                data: "Stream 2 - Event 1".to_string(),
            },
            TestMetadata {
                stream_id: 2,
                version: 1,
            },
        ),
        ExpectedVersion::exact(1),
    )?;

    println!("Appended events.");

    let txn = db.reader().storage().env.read_txn()?;

    // Verify Stream 1, Version 1
    let event1_v1 = db
        .get_by_stream(&txn, 1, 1)?
        .expect("Should find Stream 1 Version 1");
    println!("Read Stream 1 Version 1: {:?}", event1_v1);
    // Note: To access fields, one might need to deref or convert if rkyv accessors aren't direct.
    // For now assuming Debug print confirms it.

    // Verify Stream 1, Version 2
    let event1_v2 = db
        .get_by_stream(&txn, 1, 2)?
        .expect("Should find Stream 1 Version 2");
    println!("Read Stream 1 Version 2: {:?}", event1_v2);

    // Verify Stream 2, Version 1
    let event2_v1 = db
        .get_by_stream(&txn, 2, 1)?
        .expect("Should find Stream 2 Version 1");
    println!("Read Stream 2 Version 1: {:?}", event2_v1);

    // Verify Non-existent Stream
    let none_event = db.get_by_stream(&txn, 3, 1)?;
    assert!(none_event.is_none(), "Should not find Stream 3 Version 1");
    println!("Correctly returned None for non-existent stream.");

    // Verify Non-existent Version
    let none_version = db.get_by_stream(&txn, 1, 3)?;
    assert!(none_version.is_none(), "Should not find Stream 1 Version 3");
    println!("Correctly returned None for non-existent version.");

    println!("All verifications passed!");

    Ok(())
}
