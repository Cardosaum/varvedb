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
#[archive_attr(derive(Debug))]
#[repr(C)]
pub struct SecretEvent {
    pub content: String,
    pub secret_code: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("encrypted_example.mdb");

    // 1. Define a Master Key (32 bytes)
    // In a real app, load this from a secure location (env var, vault, etc.)
    let master_key = [0x42u8; 32];

    let config = StorageConfig {
        path: db_path.clone(),
        map_size: 10 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true,
        encryption_enabled: true, // Enable encryption
        master_key: Some(zeroize::Zeroizing::new(master_key)), // Provide the master key
    };

    println!("Opening encrypted storage at {:?}", db_path);
    let storage = Storage::open(config)?;

    let mut writer = Writer::<SecretEvent>::new(storage.clone());
    let reader = Reader::<SecretEvent>::new(storage.clone());

    // 2. Write an event
    println!("Writing secret event...");
    writer.append(
        1,
        1,
        SecretEvent {
            content: "The eagle flies at midnight".to_string(),
            secret_code: 12345,
        },
    )?;

    // 3. Read it back transparently
    println!("Reading secret event...");
    let txn = storage.env.read_txn()?;
    if let Some(event) = reader.get(&txn, 1)? {
        println!("Decrypted event: {:?}", event);
        assert_eq!(event.content, "The eagle flies at midnight");
    }

    // 4. Demonstrate security
    println!("Simulating unauthorized access (wrong master key)...");

    // Close the previous storage instance to release locks (if any, though LMDB handles multiple readers)
    drop(txn);
    drop(reader);
    drop(writer);
    drop(storage);

    let wrong_key = [0x00u8; 32];
    let attack_config = StorageConfig {
        path: db_path.clone(),
        map_size: 10 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true,
        encryption_enabled: true,
        master_key: Some(zeroize::Zeroizing::new(wrong_key)),
    };

    let attack_storage = Storage::open(attack_config)?;
    let attack_reader = Reader::<SecretEvent>::new(attack_storage.clone());
    let attack_txn = attack_storage.env.read_txn()?;

    // Try to read the event
    let result = attack_reader.get(&attack_txn, 1);

    match result {
        Ok(_) => {
            println!("CRITICAL: Managed to read event with wrong key! (This should not happen)")
        }
        Err(e) => println!("Security verified! Access denied: {}", e),
    }

    println!("Encryption example complete.");
    Ok(())
}
