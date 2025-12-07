// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use rkyv::{Archive, Deserialize, Serialize};
use varvedb::traits::MetadataExt;
use varvedb::{ExpectedVersion, Payload, Varve};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct BasicEvent {
    pub message: String,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct BasicMetadata {
    pub stream_id: u128,
    pub version: u32,
    pub timestamp: u64,
}

impl MetadataExt for BasicMetadata {
    fn stream_id(&self) -> u128 {
        self.stream_id
    }

    fn version(&self) -> u32 {
        self.version
    }
}

use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let path = dir.path().join("basic_example.mdb");
    // let _ = std::fs::remove_dir_all(path); // Clean up unnecessary with tempfile

    let mut db = Varve::open(&path)?;

    let stream_id = 1;

    // Append an event
    let event = BasicEvent {
        message: "Hello, Varve!".to_string(),
    };
    let metadata = BasicMetadata {
        stream_id,
        version: 1,
        timestamp: 123456789,
    };

    let seq = db.append(Payload::new(event, metadata), ExpectedVersion::Auto)?;
    println!("Appended event with sequence number: {}", seq);

    // Read events
    for result in db.iter()? {
        let view = result?;
        // view is EventView. It implements Deref to E::Archived.
        // We can just print it.
        println!("Read event: {:?}", view);
    }

    // Read exact event
    if let Some(view) = db.get_by_stream(&db.reader().storage().env.read_txn()?, stream_id, 1)? {
        println!("Found exact event: {:?}", view);
    }

    Ok(())
}
