// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
struct MyEvent {
    pub data: u32,
    pub payload: Vec<u8>,
}

#[tokio::test]
async fn test_fuzz_simulation_corruption() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        ..Default::default()
    };
    let storage = Storage::open(config)?;
    let mut writer = Writer::<MyEvent>::new(storage.clone());
    let reader = Reader::<MyEvent>::new(storage.clone());

    // 1. Write a valid event
    let event = MyEvent {
        data: 42,
        payload: vec![1, 2, 3, 4],
    };
    writer.append(1, 1, event.clone())?;

    // Verify read works
    // Reader::get takes (txn, seq). We need to open a read txn.
    {
        let txn = storage.env.read_txn()?;
        let read_event = reader.get(&txn, 1)?.unwrap();
        assert_eq!(read_event.data, 42);
    }

    // 2. Corrupt the data in storage
    {
        let env = &storage.env;
        let mut wtxn = env.write_txn()?;
        let db = storage.events_log;

        // Find the key for (stream 1, version 1)
        // Key format: StreamID (16 bytes) + Version (4 bytes)
        // Wait, StreamKey is defined in storage/mod.rs.
        // It's StreamID (128 bit) + Version (32 bit).
        // Let's manually construct it.
        let stream_id: u128 = 1;
        let version: u32 = 1;
        let mut key_bytes = Vec::new();
        key_bytes.extend_from_slice(&stream_id.to_be_bytes());
        key_bytes.extend_from_slice(&version.to_be_bytes());

        // Actually, events_log keys are GlobalSeq (u64).
        // The stream_index maps (StreamID, Version) -> GlobalSeq.
        // So we need to find the GlobalSeq first.

        let global_seq = storage.stream_index.get(&wtxn, &key_bytes)?.unwrap();

        // Now get the data from events_log
        let mut data = db.get(&wtxn, &global_seq)?.unwrap().to_vec();

        // Corrupt a byte in the middle (likely payload or rkyv structure)
        if data.len() > 10 {
            data[10] = data[10].wrapping_add(1);
        }

        // Write back corrupted data
        db.put(&mut wtxn, &global_seq, &data)?;
        wtxn.commit()?;
    }

    // 3. Try to read corrupted event
    // Should return Error::Validation or Error::Serialization, NOT panic
    {
        let txn = storage.env.read_txn()?;
        let result = reader.get(&txn, 1);
        match result {
            Ok(Some(_)) => panic!("Should have failed validation"),
            Ok(None) => panic!("Should have found something (even if invalid)"),
            Err(_) => {
                // Success: caught the corruption
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_fuzz_random_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        ..Default::default()
    };
    let storage = Storage::open(config)?;
    let mut writer = Writer::<MyEvent>::new(storage.clone());

    let mut rng = rand::thread_rng();

    // Write 100 random events
    for _ in 1..=100 {
        let stream_id: u128 = rng.gen();
        let version: u32 = 1; // Keep version simple for now
        let data: u32 = rng.gen();
        let payload_len = rng.gen_range(0..1024);
        let mut payload = vec![0u8; payload_len];
        rng.fill(&mut payload[..]);

        let event = MyEvent { data, payload };

        // We might get collisions on stream_id, so handle error
        match writer.append(stream_id, version, event) {
            Ok(_) => {}
            Err(varvedb::error::Error::ConcurrencyConflict { .. }) => {
                // Ignore conflicts, that's valid behavior
            }
            Err(e) => return Err(Box::new(e) as Box<dyn std::error::Error>),
        }
    }

    Ok(())
}
