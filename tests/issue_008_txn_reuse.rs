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

struct SimpleHandler {
    count: Arc<Mutex<u32>>,
}

impl EventHandler<MyEvent> for SimpleHandler {
    fn handle(&mut self, _event: &ArchivedMyEvent) -> varvedb::error::Result<()> {
        let mut c = self.count.lock().unwrap();
        *c += 1;
        Ok(())
    }
}

#[tokio::test]
async fn test_txn_reuse() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("txn_reuse_test.mdb");
    let mut db = Varve::open(&db_path)?;

    let count = Arc::new(Mutex::new(0));
    let handler = SimpleHandler {
        count: count.clone(),
    };

    let consumer_id = 999u64;
    let mut processor = Processor::new(&db, handler, consumer_id).with_config(ProcessorConfig {
        batch_size: 5,
        batch_timeout: Duration::from_millis(10),
    });

    let handle = tokio::spawn(async move {
        processor.run().await.unwrap();
    });

    // Append events slowly to trigger multiple polls/txns
    for i in 0..15 {
        let event = MyEvent { id: i };
        let metadata = MyMetadata {
            stream_id: 1,
            version: i + 1,
        };
        db.append(Payload::new(event, metadata), ExpectedVersion::Auto)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    {
        let c = count.lock().unwrap();
        assert_eq!(*c, 15);
    }

    handle.abort();

    Ok(())
}
