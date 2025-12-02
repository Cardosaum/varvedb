use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(check_bytes)]
#[repr(C)]
pub struct PersistEvent {
    pub id: u64,
    pub data: String,
}

#[test]
fn test_persistence_after_close() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("persist.mdb");

    let config = StorageConfig {
        path: db_path.clone(),
        map_size: 10 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true, encryption_enabled: false,
    };

    // 1. Open, Write, Close
    {
        let storage = Storage::open(config.clone())?;
        let mut writer = Writer::<PersistEvent>::new(storage.clone());
        writer.append(
            1,
            1,
            PersistEvent {
                id: 1,
                data: "Part 1".to_string(),
            },
        )?;
        // Storage is dropped here, closing the env
    }

    // 2. Re-open, Verify, Write More
    {
        let storage = Storage::open(config.clone())?;
        let mut writer = Writer::<PersistEvent>::new(storage.clone());
        let reader = Reader::<PersistEvent>::new(storage.clone());

        let txn = storage.env.read_txn()?;
        let event1 = reader.get(&txn, 1)?.expect("Event 1 should exist");
        assert_eq!(event1.data, "Part 1");
        drop(txn); // Release read txn before writing

        writer.append(
            1,
            2,
            PersistEvent {
                id: 2,
                data: "Part 2".to_string(),
            },
        )?;
    }

    // 3. Re-open again, Verify All
    {
        let storage = Storage::open(config)?;
        let reader = Reader::<PersistEvent>::new(storage.clone());
        let txn = storage.env.read_txn()?;

        let event1 = reader.get(&txn, 1)?.expect("Event 1 should exist");
        assert_eq!(event1.data, "Part 1");

        let event2 = reader.get(&txn, 2)?.expect("Event 2 should exist");
        assert_eq!(event2.data, "Part 2");
    }

    Ok(())
}
