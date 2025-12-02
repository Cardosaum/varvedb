// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

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
async fn test_writer_clone() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().to_path_buf(),
        ..Default::default()
    };
    let storage = Storage::open(config)?;

    // 1. Create Writer
    let mut writer1 = Writer::<MyEvent>::new(storage.clone());

    // 2. Clone Writer
    let mut writer2 = writer1.clone();

    // 3. Subscribe
    let mut rx = writer1.subscribe();

    // 4. Write with original
    writer1.append(1, 1, MyEvent { data: 1 })?;

    // 5. Write with clone
    writer2.append(1, 2, MyEvent { data: 2 })?;

    // 6. Verify subscriber sees both
    // Note: watch channel only guarantees latest value.
    // Since we wrote 1 and 2, and didn't await in between, we might miss 1.
    // But we subscribed BEFORE writing.

    // However, `rx.borrow()` gets the *current* value.
    // If the writer thread (or this thread) has already updated it to 2, we see 2.

    let current = *rx.borrow();
    assert!(current >= 1); // We should see at least 1 or 2.

    if current < 2 {
        rx.changed().await?;
        assert_eq!(*rx.borrow(), 2);
    }

    Ok(())
}
