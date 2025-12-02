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
        ..Default::default()
    };
    let storage = Storage::open(config)?;

    let mut writer = Writer::<ChatEvent>::new(storage.clone());
    let reader = Reader::<ChatEvent>::new(storage.clone());
    let rx = writer.subscribe();

    // Start Processor
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

    // Simulate Activity
    let events = vec![
        ChatEvent::Join {
            user: "Alice".to_string(),
        },
        ChatEvent::Message {
            user: "Alice".to_string(),
            content: "Hello everyone!".to_string(),
        },
        ChatEvent::Join {
            user: "Bob".to_string(),
        },
        ChatEvent::Message {
            user: "Bob".to_string(),
            content: "Hi Alice!".to_string(),
        },
        ChatEvent::Leave {
            user: "Alice".to_string(),
        },
    ];

    for (i, event) in events.into_iter().enumerate() {
        writer.append(1, (i + 1) as u32, event)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!("Simulation complete.");
    Ok(())
}
