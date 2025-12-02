use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::Writer;
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
struct MyEvent {
    pub data: u32,
}

#[tokio::test]
async fn test_shared_writer_channel() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        ..Default::default()
    };
    let storage = Storage::open(config)?;

    // 1. Create Writer 1
    let writer1 = Writer::<MyEvent>::new(storage.clone());

    // 2. Subscribe to Writer 1
    let mut rx = writer1.subscribe();
    let initial_seq = *rx.borrow();
    assert_eq!(initial_seq, 0);

    // 3. Create Writer 2 (sharing same storage)
    let mut writer2 = Writer::<MyEvent>::new(storage.clone());

    // 4. Write event using Writer 2
    writer2.append(1, 1, MyEvent { data: 42 })?;

    // 5. Verify Subscriber (from Writer 1) receives update
    rx.changed().await?;
    let new_seq = *rx.borrow();
    assert_eq!(new_seq, 1);

    Ok(())
}
