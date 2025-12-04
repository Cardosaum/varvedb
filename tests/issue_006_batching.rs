// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use rkyv::{Archive, Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use varvedb::engine::{EventHandler, Processor, ProcessorConfig, Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
struct MyEvent {
    pub data: u32,
}

struct MyHandler {
    count: Arc<Mutex<u32>>,
}

impl EventHandler<MyEvent> for MyHandler {
    fn handle(&mut self, event: &ArchivedMyEvent) -> varvedb::error::Result<()> {
        let mut count = self.count.lock().unwrap();
        *count += event.data;
        Ok(())
    }
}

#[tokio::test]
async fn test_processor_batching() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        ..Default::default()
    };
    let storage = Storage::open(config)?;

    // 1. Write many events
    let mut writer = Writer::new(storage.clone());
    let event_count = 50;
    for i in 1..=event_count {
        writer.append(1, i, MyEvent { data: 1 })?;
    }

    // 2. Configure Processor with batch size
    let count = Arc::new(Mutex::new(0));
    let handler = MyHandler {
        count: count.clone(),
    };

    let reader = Reader::<MyEvent>::new(storage.clone());
    let rx = writer.subscribe();

    let config = ProcessorConfig {
        batch_size: 10,
        batch_timeout: Duration::from_secs(1),
    };

    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    "batch_consumer".hash(&mut hasher);
    let consumer_id = hasher.finish();

    let mut processor = Processor::new(reader, handler, consumer_id, rx).with_config(config);

    // 3. Run processor in background
    let handle = tokio::spawn(async move { processor.run().await });

    // 4. Wait for processing
    // Since run() loops forever, we need to check the count and then abort
    let start = std::time::Instant::now();
    loop {
        {
            let c = count.lock().unwrap();
            if *c >= event_count {
                break;
            }
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Timed out waiting for events");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    handle.abort();

    // 5. Verify count
    let final_count = *count.lock().unwrap();
    assert_eq!(final_count, event_count);

    Ok(())
}
