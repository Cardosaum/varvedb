// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use tokio::time::Duration;
use varvedb::processor::{EventHandler, Processor};
use varvedb::traits::MetadataExt;
use varvedb::{ExpectedVersion, Payload, Varve};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(C)]
pub enum ChatEvent {
    Join { user: String },
    Message { user: String, content: String },
    Leave { user: String },
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[repr(C)]
pub struct ChatMetadata {
    pub stream_id: u128,
    pub version: u32,
}

impl MetadataExt for ChatMetadata {
    fn stream_id(&self) -> u128 {
        self.stream_id
    }
    fn version(&self) -> u32 {
        self.version
    }
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
    let db_path = dir.path().join("chat_example.mdb");
    let mut db = Varve::open(&db_path)?;

    // Start Processor
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    "chat_client".hash(&mut hasher);
    let consumer_id = hasher.finish();

    let mut processor = Processor::new(
        &db,
        ChatHandler {
            name: "ClientView".to_string(),
        },
        consumer_id,
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

    let stream_id = 1;
    for (i, event) in events.into_iter().enumerate() {
        let metadata = ChatMetadata {
            stream_id,
            version: (i + 1) as u32,
        };
        db.append(Payload::new(event, metadata), ExpectedVersion::Auto)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!("Simulation complete.");
    Ok(())
}
