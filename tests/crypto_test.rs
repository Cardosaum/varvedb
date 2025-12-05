// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::crypto::{decrypt, encrypt, KeyManager};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[repr(C)]
pub struct SecretEvent {
    pub secret_data: Vec<u8>,
}

#[test]
fn test_crypto_shredding() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().join("test_crypto.mdb"),
        map_size: 10 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true,
        encryption_enabled: true,
        master_key: Some(zeroize::Zeroizing::new([1u8; 32])), // Use a dummy master key for crypto test
    };

    let storage = Storage::open(config)?;
    let key_manager = KeyManager::new(storage.clone());

    // 1. Get Key
    let stream_id = 123;
    let key = key_manager.get_or_create_key(stream_id)?;

    // 2. Encrypt Event
    let event = SecretEvent {
        secret_data: vec![0xDE, 0xAD, 0xBE, 0xEF],
    };
    let event_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&event)?;
    let encrypted_payload = encrypt(&key, &event_bytes, &[])?;

    // 3. Store (Simulating storing EncryptedEvent wrapper)
    // In a real scenario, the user would define an EncryptedEvent struct
    // that holds the Vec<u8> ciphertext.
    // For this test, we just verify we can decrypt it.

    // 4. Decrypt
    let decrypted_bytes = decrypt(&key, &encrypted_payload, &[])?;
    let decrypted_event =
        rkyv::access::<<SecretEvent as Archive>::Archived, rkyv::rancor::Error>(&decrypted_bytes)
            .unwrap();

    assert_eq!(
        decrypted_event.secret_data.as_slice(),
        &[0xDE, 0xAD, 0xBE, 0xEF]
    );

    // 5. Shred Key
    key_manager.delete_key(stream_id)?;

    // 6. Verify Unrecoverable
    // We can't get the key back (it would generate a new random one)
    // But if we try to decrypt with a wrong key (simulating lost key), it fails.
    let new_key = key_manager.get_or_create_key(stream_id)?; // New random key
    assert_ne!(key, new_key);

    let result = decrypt(&new_key, &encrypted_payload, &[]);
    assert!(result.is_err());

    Ok(())
}
