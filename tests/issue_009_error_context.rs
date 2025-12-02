use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::Writer;
use varvedb::error::Error;
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
struct MyEvent {
    pub data: u32,
}

#[tokio::test]
async fn test_error_context() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        ..Default::default()
    };
    let storage = Storage::open(config)?;
    let mut writer = Writer::<MyEvent>::new(storage.clone());

    // 1. Test VersionMismatch
    // Append event with version 1
    writer.append(1, 1, MyEvent { data: 1 })?;

    // Try to append version 3 (skipping 2) - this should fail if logic enforces sequential versions,
    // but currently VarveDB might not enforce strict sequential versions in `append` unless we implement optimistic concurrency control.
    // Wait, let's check `Writer::append` logic.
    // It takes `expected_version`? No, it takes `version`.
    // Actually, `Writer::append` signature is `append(&mut self, stream_id: u128, expected_version: u32, event: E)`.
    // So if we pass wrong expected version, it should fail.

    let result = writer.append(1, 1, MyEvent { data: 2 }); // Expected 1, but current is 1 (so next is 2). Wait.
                                                           // The logic in `append` is:
                                                           // The logic in `append` checks if the version already exists.
                                                           // We appended version 1.
                                                           // If we try to append version 1 again (with different data), it should fail with ConcurrencyConflict.

    let result = writer.append(1, 1, MyEvent { data: 2 });
    match result {
        Err(Error::ConcurrencyConflict { stream_id, version }) => {
            assert_eq!(stream_id, 1);
            assert_eq!(version, 1);
        }
        _ => panic!("Expected ConcurrencyConflict error, got {:?}", result),
    }

    // 2. Test KeyNotFound (requires encryption enabled)
    // We need a new storage with encryption enabled but no key for a specific stream.
    // Actually, `KeyManager` generates keys on the fly if they don't exist in `get_or_create_key`.
    // `KeyNotFound` is returned by `Reader::get` if decryption fails because key is missing (which shouldn't happen if written correctly).
    // Or if `get_master_key` fails (which returns KeyNotFound(0)).

    let dir_enc = tempdir()?;
    let config_enc = StorageConfig {
        path: dir_enc.path().to_path_buf(),
        encryption_enabled: true,
        // No master key provided!
        master_key: None,
        ..Default::default()
    };

    // Storage::open doesn't check master key presence immediately, but Writer/Reader creation might or usage might.
    // Actually `Storage::open` doesn't check. `Writer::new` creates `KeyManager`.
    // `KeyManager::get_or_create_key` calls `get_master_key`.

    let storage_enc = Storage::open(config_enc)?;
    let mut writer_enc = Writer::<MyEvent>::new(storage_enc.clone());

    let result = writer_enc.append(2, 0, MyEvent { data: 1 });
    match result {
        Err(Error::KeyNotFound(id)) => {
            assert_eq!(id, 0); // Master key ID
        }
        _ => panic!(
            "Expected KeyNotFound error for master key, got {:?}",
            result
        ),
    }

    Ok(())
}
