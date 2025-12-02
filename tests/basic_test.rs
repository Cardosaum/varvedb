use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
#[repr(C)]
pub enum SystemEvent {
    V1(EventV1),
}

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
#[repr(C)]
pub struct EventV1 {
    pub stream_id: u128,
    pub kind: u16,
    pub timestamp: u64,
    pub payload: Vec<u8>,
}

#[test]
fn test_basic_write_read() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().join("test.mdb"),
        map_size: 10 * 1024 * 1024, // 10MB
        max_dbs: 10,
        create_dir: true,
        encryption_enabled: false,
        master_key: None,
    };

    let storage = Storage::open(config)?;
    let mut writer = Writer::<SystemEvent>::new(storage.clone());

    let event = SystemEvent::V1(EventV1 {
        stream_id: 1,
        kind: 100,
        timestamp: 123456789,
        payload: vec![1, 2, 3, 4],
    });

    writer.append(1, 1, event)?;

    let reader = Reader::<SystemEvent>::new(storage.clone());
    let txn = storage.env.read_txn()?;

    // Read back
    let fetched = reader.get(&txn, 1)?;
    assert!(fetched.is_some());

    let event = fetched.unwrap();
    match &*event {
        rkyv::Archived::<SystemEvent>::V1(v1) => {
            assert_eq!(v1.stream_id, 1);
            assert_eq!(v1.kind, 100);
        }
    }

    Ok(())
}
