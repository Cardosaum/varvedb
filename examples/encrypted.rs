use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug))]
#[repr(C)]
pub struct SecretEvent {
    pub content: String,
    pub secret_code: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("encrypted_example.mdb");

    // 1. Define a Master Key (32 bytes)
    // In a real app, load this from a secure location (env var, vault, etc.)
    let master_key = [0x42u8; 32];

    let config = StorageConfig {
        path: db_path.clone(),
        map_size: 10 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true,
        encryption_enabled: true,     // Enable encryption
        master_key: Some(master_key), // Provide the master key
    };

    println!("Opening encrypted storage at {:?}", db_path);
    let storage = Storage::open(config)?;

    let mut writer = Writer::<SecretEvent>::new(storage.clone());
    let reader = Reader::<SecretEvent>::new(storage.clone());

    // 2. Write an event
    println!("Writing secret event...");
    writer.append(
        1,
        1,
        SecretEvent {
            content: "The eagle flies at midnight".to_string(),
            secret_code: 12345,
        },
    )?;

    // 3. Read it back transparently
    println!("Reading secret event...");
    let txn = storage.env.read_txn()?;
    if let Some(event) = reader.get(&txn, 1)? {
        println!("Decrypted event: {:?}", event);
        assert_eq!(event.content, "The eagle flies at midnight");
    }

    // 4. Demonstrate security (optional simulation)
    // If we tried to open this DB without the master key, we wouldn't be able to decrypt the stream keys.

    println!("Encryption example complete.");
    Ok(())
}
