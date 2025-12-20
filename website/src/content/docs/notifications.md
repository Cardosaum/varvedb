---
title: "Async Notifications"
description: "Runtime-agnostic write notifications so async readers can await new commits without polling."
---

VarveDB is intentionally **runtime-agnostic**. If your readers run in an async context and you want an efficient way to wait for new writes (without polling), enable the optional `notify` feature.

## Enable the feature

```toml
[dependencies]
varvedb = { version = "0.4", features = ["notify"] }
```

## What you get: `WriteWatcher`

With `notify` enabled, VarveDB exposes a `WriteWatcher` handle that tracks a **committed watermark** (`committed_next_global_seq`) and can be awaited:

- `Varve::watcher() -> WriteWatcher`
- `GlobalReader::watcher() -> WriteWatcher`
- `StreamReader<T>::watcher() -> WriteWatcher`

The watcher is **cloneable** and can be shared across tasks.

## Typical pattern: tail the global log

```rust
use varvedb::{GlobalSequence, Varve};

async fn tail(mut varve: Varve) -> varvedb::Result<()> {
    let watcher = varve.watcher();
    let mut cursor = GlobalSequence(0);

    loop {
        // Read what’s currently available.
        let reader = varve.global_reader();
        let iter = reader.iter_from(cursor)?;
        let events = iter.collect_all()?;

        if events.is_empty() {
            // Nothing new: wait until the committed watermark advances.
            cursor = watcher.wait_for_global_seq(cursor).await;
            continue;
        }

        // Process events...
        cursor = GlobalSequence(events.last().unwrap().global_seq.0 + 1);
    }
}
```

## Important semantics

- **Signal, not data**: `wait_for_global_seq()` only tells you that *something was committed* after `from`. You still need to query LMDB to fetch events.
- **Gaps are possible**: global sequences are monotonically increasing, but if a write reserves a sequence and fails before commit, some sequence numbers may be missing. Using `iter_from(cursor)` naturally skips gaps.
- **In-process only**: this is designed for embedded usage where readers and writers share the same process.


