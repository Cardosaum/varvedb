use rkyv::{Archive, Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use varvedb::processor::{EventHandler, Processor, ProcessorConfig};
use varvedb::traits::MetadataExt;
use varvedb::{ExpectedVersion, Payload, Varve};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(C)]
pub struct MyEvent {
    pub id: u32,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(C)]
pub struct MyMetadata {
    pub stream_id: u128,
    pub version: u32,
}

impl MetadataExt for MyMetadata {
    fn stream_id(&self) -> u128 {
        self.stream_id
    }
    fn version(&self) -> u32 {
        self.version
    }
}

struct BatchHandler {
    pub processed_count: Arc<Mutex<usize>>,
}

impl EventHandler<MyEvent> for BatchHandler {
    fn handle(&mut self, _event: &ArchivedMyEvent) -> varvedb::error::Result<()> {
        let mut count = self.processed_count.lock().unwrap();
        *count += 1;
        Ok(())
    }
}

#[tokio::test]
async fn test_batch_processing() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("batching_test.mdb");
    let mut db = Varve::open(&db_path)?;

    // Populate events
    let event_count = 100;
    for i in 0..event_count {
        let event = MyEvent { id: i };
        let metadata = MyMetadata {
            stream_id: 1,
            version: i + 1,
        };
        db.append(Payload::new(event, metadata), ExpectedVersion::Auto)?;
    }

    let processed_count = Arc::new(Mutex::new(0));
    let handler = BatchHandler {
        processed_count: processed_count.clone(),
    };

    let consumer_id = 12345u64;
    let mut processor = Processor::new(&db, handler, consumer_id).with_config(ProcessorConfig {
        batch_size: 10,
        batch_timeout: Duration::from_millis(10),
    });

    let handle = tokio::spawn(async move {
        processor.run().await.unwrap();
    });

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check if processed
    {
        let count = processed_count.lock().unwrap();
        assert_eq!(*count, event_count as usize);
    }

    // Abort processor (since it runs forever)
    handle.abort();

    Ok(())
}
