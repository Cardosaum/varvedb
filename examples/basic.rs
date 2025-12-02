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
#[repr(C)]
pub struct BasicEvent {
    pub message: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().join("basic_example.mdb"),
        ..Default::default()
    };
    let storage = Storage::open(config)?;

    let mut writer = Writer::<BasicEvent>::new(storage.clone());
    let reader = Reader::<BasicEvent>::new(storage.clone());

    writer.append(
        1,
        1,
        BasicEvent {
            message: "Hello VarveDB!".to_string(),
        },
    )?;
    println!("Appended event.");

    let txn = storage.env.read_txn()?;
    if let Some(event) = reader.get(&txn, 1)? {
        println!("Read event: {}", event.message);
    }

    Ok(())
}
