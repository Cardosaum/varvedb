use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(check_bytes)]
struct SecEvent {
    pub data: String,
}

#[test]
fn test_master_key_encryption() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let master_key = [42u8; 32];

    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        encryption_enabled: true,
        master_key: Some(master_key),
        ..Default::default()
    };

    let storage = Storage::open(config.clone())?;
    let mut writer = Writer::<SecEvent>::new(storage.clone());

    let event = SecEvent {
        data: "Secret Data".to_string(),
    };
    writer.append(1, 1, event)?;

    // 1. Verify we can read it back with the correct key
    let reader = Reader::<SecEvent>::new(storage.clone());
    let txn = storage.env.read_txn()?;
    let read_event = reader.get(&txn, 1)?.expect("Event should exist");
    assert_eq!(read_event.data, "Secret Data");
    drop(txn);
    drop(reader);
    drop(writer);
    drop(storage); // Close storage

    // 2. Attack Simulation: Open as if we are an attacker (no master key, or ignoring encryption)
    let attack_config = StorageConfig {
        path: dir.path().to_path_buf(),
        encryption_enabled: false, // Attacker ignores encryption flag to access raw DB
        master_key: None,
        ..Default::default()
    };

    let attack_storage = Storage::open(attack_config)?;
    let txn = attack_storage.env.read_txn()?;

    let stream_id = 1u128;
    let encrypted_key_bytes = attack_storage
        .keystore
        .get(&txn, &stream_id)?
        .expect("Key should exist");

    // The key should be 32 bytes + 12 bytes nonce + 16 bytes tag = 60 bytes
    assert_eq!(encrypted_key_bytes.len(), 60);

    // It should NOT be the plaintext key.
    // We don't know the plaintext key (it was random), but we can try to decrypt with WRONG master key.

    let wrong_master_key = [0u8; 32];
    let aad = stream_id.to_be_bytes();
    let result = varvedb::crypto::decrypt(&wrong_master_key, encrypted_key_bytes, &aad);
    assert!(
        result.is_err(),
        "Decryption with wrong master key should fail"
    );

    // 3. Decrypt with CORRECT master key manually
    let result = varvedb::crypto::decrypt(&master_key, encrypted_key_bytes, &aad);
    assert!(
        result.is_ok(),
        "Decryption with correct master key should succeed"
    );

    Ok(())
}

#[test]
fn test_wrong_master_key_access() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let master_key = [0xAAu8; 32];
    let wrong_key = [0xBBu8; 32];

    // 1. Write with correct key
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        encryption_enabled: true,
        master_key: Some(master_key),
        ..Default::default()
    };
    let storage = Storage::open(config.clone())?;
    let mut writer = Writer::<SecEvent>::new(storage.clone());
    writer.append(
        1,
        1,
        SecEvent {
            data: "Sensitive".to_string(),
        },
    )?;

    drop(writer);
    drop(storage);

    // 2. Try to read with WRONG key using high-level API
    let attack_config = StorageConfig {
        path: dir.path().to_path_buf(),
        encryption_enabled: true,
        master_key: Some(wrong_key),
        ..Default::default()
    };
    let attack_storage = Storage::open(attack_config)?;
    let reader = Reader::<SecEvent>::new(attack_storage.clone());
    let txn = attack_storage.env.read_txn()?;

    let result = reader.get(&txn, 1);

    assert!(
        result.is_err(),
        "Reader should fail when using wrong master key"
    );
    // Optionally check error message matches "Decryption failed" or similar

    Ok(())
}
