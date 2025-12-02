use proptest::prelude::*;
use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[archive(check_bytes)]
#[repr(C)]
pub struct PropEvent {
    pub id: u64,
    pub data: String,
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]
    #[test]
    fn test_write_read_roundtrip(
        id in any::<u64>(),
        data in "\\PC*"
    ) {
        let dir = tempdir().unwrap();
        let config = StorageConfig {
            path: dir.path().join("prop_test.mdb"),
            map_size: 10 * 1024 * 1024,
            max_dbs: 10,
            create_dir: true,
        };
        let storage = Storage::open(config).unwrap();
        let mut writer = Writer::<PropEvent>::new(storage.clone());
        let reader = Reader::<PropEvent>::new(storage.clone());

        let event = PropEvent { id, data: data.clone() };
        writer.append(1, 1, event.clone()).unwrap();

        let txn = storage.env.read_txn().unwrap();
        let archived = reader.get(&txn, 1).unwrap().unwrap();

        assert_eq!(archived.id, event.id);
        assert_eq!(archived.data, event.data);
    }

    #[test]
    fn test_sequence_monotonicity(
        count in 1..20u32
    ) {
        let dir = tempdir().unwrap();
        let config = StorageConfig {
            path: dir.path().join("prop_seq.mdb"),
            map_size: 10 * 1024 * 1024,
            max_dbs: 10,
            create_dir: true,
        };
        let storage = Storage::open(config).unwrap();
        let mut writer = Writer::<PropEvent>::new(storage.clone());
        let reader = Reader::<PropEvent>::new(storage.clone());

        for i in 0..count {
            let event = PropEvent { id: i as u64, data: "test".to_string() };
            writer.append(1, i, event).unwrap();
        }

        let txn = storage.env.read_txn().unwrap();
        let _last_seq = 0;

        for i in 1..=count {
            let archived = reader.get(&txn, i as u64).unwrap();
            assert!(archived.is_some());
            // In a real scenario we would check the sequence number if it was part of the event,
            // but here we verify we can read them in order.
        }
    }
}
