use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use tokio::time::Duration;
use varvedb::engine::{EventHandler, Processor, Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[archive(check_bytes)]
#[repr(C)]
pub enum ChatEvent {
    Join { user: String },
    Message { user: String, content: String },
    Leave { user: String },
}

struct ChatHandler {
    name: String,
}

impl EventHandler<ChatEvent> for ChatHandler {
    fn handle(&mut self, event: &ArchivedChatEvent) -> varvedb::error::Result<()> {
        match event {
            ArchivedChatEvent::Join { user } => {
                println!("[{}] User joined: {}", self.name, user);
            }
            ArchivedChatEvent::Message { user, content } => {
                println!("[{}] {}: {}", self.name, user, content);
            }
            ArchivedChatEvent::Leave { user } => {
                println!("[{}] User left: {}", self.name, user);
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().join("chat_example.mdb"),
        map_size: 10 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true, encryption_enabled: false,
    };
    let storage = Storage::open(config)?;

    let mut writer = Writer::<ChatEvent>::new(storage.clone());
    let reader = Reader::<ChatEvent>::new(storage.clone());
    let rx = writer.subscribe();

    // Start Processor in background
    let mut processor = Processor::new(
        reader,
        ChatHandler {
            name: "ClientView".to_string(),
        },
        "chat_client",
        rx,
    );

    tokio::spawn(async move {
        if let Err(e) = processor.run().await {
            eprintln!("Processor error: {}", e);
        }
    });

    // Simulate Chat Activity
    println!("Simulating chat activity...");
    writer.append(
        1,
        1,
        ChatEvent::Join {
            user: "Alice".to_string(),
        },
    )?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    writer.append(
        1,
        2,
        ChatEvent::Message {
            user: "Alice".to_string(),
            content: "Hello everyone!".to_string(),
        },
    )?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    writer.append(
        1,
        3,
        ChatEvent::Join {
            user: "Bob".to_string(),
        },
    )?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    writer.append(
        1,
        4,
        ChatEvent::Message {
            user: "Bob".to_string(),
            content: "Hi Alice!".to_string(),
        },
    )?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    writer.append(
        1,
        5,
        ChatEvent::Leave {
            user: "Alice".to_string(),
        },
    )?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("Simulation complete.");
    Ok(())
}
