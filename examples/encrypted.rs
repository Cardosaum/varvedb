// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::storage::StorageConfig;
use varvedb::traits::MetadataExt;
use varvedb::{ExpectedVersion, Payload, Varve};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct SecretEvent {
    pub content: String,
    pub secret_code: u32,
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub struct SecretMetadata {
    pub stream_id: u128,
    pub version: u32,
}

impl MetadataExt for SecretMetadata {
    fn stream_id(&self) -> u128 {
        self.stream_id
    }

    fn version(&self) -> u32 {
        self.version
    }
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

    // Verify authorized access in a scope
    {
        println!("Opening encrypted storage at {:?}", db_path);
        // Use the new Varve facade with custom config
        let mut db = Varve::open_with_config(config)?;

        // 2. Write an event
        println!("Writing secret event...");
        let event = SecretEvent {
            content: "The eagle flies at midnight".to_string(),
            secret_code: 12345,
        };
        let metadata = SecretMetadata {
            stream_id: 1,
            version: 1,
        };

        db.append(Payload::new(event, metadata), ExpectedVersion::exact(1))?;

        // 3. Read it back transparently
        println!("Reading secret event...");
        let txn = db.reader().storage().env.read_txn()?;
        if let Some(view) = db.get_by_stream(&txn, 1, 1)? {
            println!("Decrypted event: {:?}", view);
            // assert_eq!(view.content, "The eagle flies at midnight"); // Need Deref or AsRef
        }
    } // db and txn dropped here automatically

    // 4. Demonstrate security
    println!("Simulating unauthorized access (wrong master key)...");

    let wrong_key = [0x00u8; 32];
    let attack_config = StorageConfig {
        path: db_path.clone(),
        map_size: 10 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true,
        encryption_enabled: true,
        master_key: Some(zeroize::Zeroizing::new(wrong_key)),
    };

    // Try to open with wrong key
    match Varve::<SecretEvent, SecretMetadata>::open_with_config(attack_config) {
        Ok(attack_db) => {
            // In some LMDB + encryption setups, opening might succeed but reading fails.
            // Or opening fails if header validation checks key using HMAC or similar.
            // VarveDB encryption bench doesn't show explicit header check separate from read.
            // Let's assume we need to try to read to fail.
            let txn = attack_db.reader().storage().env.read_txn()?;
            match attack_db.get_by_stream(&txn, 1, 1) {
                Ok(_) => println!("CRITICAL: Managed to read event with wrong key!"),
                Err(e) => println!("Security verified! Access denied: {}", e),
            }
        }
        Err(e) => println!("Security verified! Failed to open with wrong key: {}", e),
    }

    println!("Encryption example complete.");
    Ok(())
}
