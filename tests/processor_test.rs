use rkyv::{Archive, Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use varvedb::processor::{EventHandler, Processor};
use varvedb::traits::MetadataExt;
use varvedb::{ExpectedVersion, Payload, Varve};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(C)]
pub struct TestEvent {
    pub content: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(C)]
pub struct TestMetadata {
    pub stream_id: u128,
    pub version: u32,
}

impl MetadataExt for TestMetadata {
    fn stream_id(&self) -> u128 {
        self.stream_id
    }
    fn version(&self) -> u32 {
        self.version
    }
}

struct TestHandler {
    received: Arc<Mutex<Vec<String>>>,
}

impl EventHandler<TestEvent> for TestHandler {
    fn handle(&mut self, event: &ArchivedTestEvent) -> varvedb::error::Result<()> {
        let mut received = self.received.lock().unwrap();
        received.push(event.content.to_string());
        Ok(())
    }
}

#[tokio::test]
async fn test_processor_basic_flow() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("processor_test.mdb");
    let mut db = Varve::open(&db_path)?;

    let received = Arc::new(Mutex::new(Vec::new()));
    let handler = TestHandler {
        received: received.clone(),
    };

    let consumer_id = 101u64;
    let mut processor = Processor::new(&db, handler, consumer_id);

    let handle = tokio::spawn(async move {
        processor.run().await.unwrap();
    });

    // Produce events
    let events = vec!["Event 1", "Event 2", "Event 3"];
    for (i, content) in events.iter().enumerate() {
        let event = TestEvent {
            content: content.to_string(),
        };
        let metadata = TestMetadata {
            stream_id: 1,
            version: (i + 1) as u32,
        };
        db.append(Payload::new(event, metadata), ExpectedVersion::Auto)?;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let rec = received.lock().unwrap();
        assert_eq!(rec.len(), 3);
        assert_eq!(rec[0], "Event 1");
    }

    handle.abort();
    Ok(())
}
