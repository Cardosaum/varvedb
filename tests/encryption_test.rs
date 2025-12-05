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
#[rkyv(derive(Debug, PartialEq))]
struct SecretEvent {
    pub secret_data: String,
}

#[test]
fn test_encryption_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        encryption_enabled: true,
        master_key: Some(zeroize::Zeroizing::new([1u8; 32])),
        ..Default::default()
    };
    let storage = Storage::open(config.clone())?;

    let mut writer = Writer::new(storage.clone());
    let event = SecretEvent {
        secret_data: "Top Secret".to_string(),
    };
    writer.append(1, 1, event)?;

    let reader = Reader::<SecretEvent>::new(storage.clone());
    let txn = storage.env.read_txn()?;

    let fetched = reader.get(&txn, 1)?;
    assert!(fetched.is_some());
    let event_view = fetched.unwrap();

    assert_eq!(event_view.secret_data, "Top Secret");

    Ok(())
}

#[test]
fn test_aad_integrity() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        encryption_enabled: true,
        master_key: Some(zeroize::Zeroizing::new([1u8; 32])),
        ..Default::default()
    };
    let storage = Storage::open(config.clone())?;

    let mut writer = Writer::new(storage.clone());
    let event = SecretEvent {
        secret_data: "Sensitive".to_string(),
    };
    writer.append(1, 1, event)?;

    // Tamper with the data on disk
    let mut txn = storage.env.write_txn()?;
    let mut bytes = storage.events_log.get(&txn, &1)?.unwrap().to_vec();

    // Flip a bit in the ciphertext (after StreamID 16 + Nonce 12)
    if bytes.len() > 28 {
        bytes[28] ^= 0xFF;
    }

    storage.events_log.put(&mut txn, &1, &bytes)?;
    txn.commit()?;

    // Try to read
    let reader = Reader::<SecretEvent>::new(storage.clone());
    let txn = storage.env.read_txn()?;
    let result = reader.get(&txn, 1);

    assert!(result.is_err()); // Should fail decryption/validation

    Ok(())
}

#[test]
fn test_disk_inspection() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        encryption_enabled: true,
        master_key: Some(zeroize::Zeroizing::new([1u8; 32])),
        ..Default::default()
    };
    let storage = Storage::open(config.clone())?;

    let secret_string = "MySuperSecretString";
    let mut writer = Writer::new(storage.clone());
    let event = SecretEvent {
        secret_data: secret_string.to_string(),
    };
    writer.append(1, 1, event)?;

    // Inspect raw bytes in events_log
    let txn = storage.env.read_txn()?;
    let bytes = storage.events_log.get(&txn, &1)?.unwrap();

    // Convert to string (lossy) and check if secret is present
    let raw_string = String::from_utf8_lossy(bytes);
    assert!(
        !raw_string.contains(secret_string),
        "Secret string found in raw bytes!"
    );

    Ok(())
}
