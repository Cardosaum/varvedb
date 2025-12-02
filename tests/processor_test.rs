use rkyv::{Archive, Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use varvedb::engine::{EventHandler, Processor, Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[archive(check_bytes)]
#[repr(C)]
pub enum TestEvent {
    Ping(u64),
}

struct TestHandler {
    received: Arc<Mutex<Vec<u64>>>,
}

impl EventHandler<TestEvent> for TestHandler {
    fn handle(&mut self, event: &<TestEvent as Archive>::Archived) -> varvedb::error::Result<()> {
        match event {
            rkyv::Archived::<TestEvent>::Ping(val) => {
                self.received.lock().unwrap().push(*val);
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_processor_reactive() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().join("test_reactive.mdb"),
        map_size: 10 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true,
        encryption_enabled: false,
        master_key: None,
    };

    let storage = Storage::open(config)?;
    let mut writer = Writer::<TestEvent>::new(storage.clone());
    let reader = Reader::<TestEvent>::new(storage.clone());
    let rx = writer.subscribe();

    let received = Arc::new(Mutex::new(Vec::new()));
    let handler = TestHandler {
        received: received.clone(),
    };

    let mut processor = Processor::new(reader, handler, "test_consumer", rx);

    // Spawn processor in background
    tokio::spawn(async move {
        processor.run().await.unwrap();
    });

    // Write events
    writer.append(1, 1, TestEvent::Ping(100))?;
    writer.append(1, 2, TestEvent::Ping(200))?;

    // Wait for processing (simple sleep for test)
    tokio::time::sleep(Duration::from_millis(100)).await;

    let results = received.lock().unwrap().clone();
    assert_eq!(results, vec![100, 200]);

    Ok(())
}
