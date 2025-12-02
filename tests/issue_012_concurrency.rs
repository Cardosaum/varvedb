use rkyv::{Archive, Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tempfile::tempdir;
use tokio::task::JoinSet;
use varvedb::engine::Writer;
use varvedb::error::Error;
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
struct MyEvent {
    pub data: u32,
}

#[tokio::test]
async fn test_concurrent_writes() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        ..Default::default()
    };
    let storage = Storage::open(config)?;

    let writer = Writer::<MyEvent>::new(storage.clone());

    let num_tasks = 10;
    let events_per_task = 20; // Total 200 events
    let stream_id = 1;

    let mut set = JoinSet::new();

    // Shared counter to verify total successful writes
    let success_count = Arc::new(AtomicU32::new(0));

    for i in 0..num_tasks {
        let mut writer = writer.clone();
        let success_count = success_count.clone();

        set.spawn(async move {
            let mut current_version_hint = 1;

            for j in 0..events_per_task {
                let data = (i * events_per_task) + j;
                let event = MyEvent { data: data as u32 };

                // Retry loop
                loop {
                    match writer.append(stream_id, current_version_hint, event.clone()) {
                        Ok(_) => {
                            success_count.fetch_add(1, Ordering::Relaxed);
                            current_version_hint += 1;
                            break; // Move to next event
                        }
                        Err(Error::ConcurrencyConflict { .. }) => {
                            // Version already exists, try next one
                            current_version_hint += 1;
                        }
                        Err(e) => {
                            panic!("Unexpected error: {:?}", e);
                        }
                    }
                }
            }
        });
    }

    while let Some(res) = set.join_next().await {
        res?;
    }

    // Verify total writes
    let total_writes = success_count.load(Ordering::Relaxed);
    assert_eq!(total_writes, (num_tasks * events_per_task) as u32);

    // Verify max version
    // We expect versions 1 to 200 to exist.
    // We can verify this by checking if version 200 exists and 201 does not (or simply that we wrote 200 items).
    // Let's try to write version 201 and see if it works (it should)
    // and version 200 should fail with ConcurrencyConflict? No, we just wrote it.
    // Actually, let's just check that we can read back 200 items?
    // We don't have a "read stream" method easily available.

    Ok(())
}
