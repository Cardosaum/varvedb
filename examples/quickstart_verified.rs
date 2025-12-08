use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

// 1. Define your Event (Schema)
// We use rkyv for zero-copy deserialization.
#[derive(Archive, Serialize, Deserialize, Debug)]
#[rkyv(derive(Debug))] // verify allows safe zero-copy
#[repr(C)]
struct InventoryEvent {
    item_id: u32,
    action: String, // "StockAdded", "StockReserved"
    quantity: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 2. Setup (In production, use a persistent path)
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().join("inventory.mdb"),
        ..Default::default()
    };

    // 3. Open the Database
    // The Storage handles the LMDB environment and background threads.
    let storage = Storage::open(config)?;
    let mut writer = Writer::new(storage.clone());

    // 4. Append an Event
    // Stream ID: 1 (e.g., Product ID)
    // Version: 1   (First event in this stream)
    let event = InventoryEvent {
        item_id: 1,
        action: "StockAdded".to_string(),
        quantity: 100,
    };

    // We use append(stream_id, expected_version, event)
    let seq_num = writer.append(1, 1, event)?;
    println!("Appended event with Global Sequence Number: {}", seq_num);

    // 5. Read it back
    let reader = Reader::<InventoryEvent>::new(storage.clone());
    let txn = storage.env.read_txn()?;

    // Fetch by Global Sequence Number
    if let Some(view) = reader.get(&txn, seq_num)? {
        // 'view' is an EventView. It implements Deref to your archived event.
        println!("Read Event: {:?}", view);
        assert_eq!(view.quantity, 100);
    }

    Ok(())
}
