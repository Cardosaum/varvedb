---
title: "[Core] Implement \"Sidecar\" Storage for Large Payloads to Prevent Cache Thrashing"
tags: ["performance", "architecture", "storage-engine"]
priority: "High"
status: "Closed"
---

### 1. The Problem
VarveDB relies on LMDB (Lightning Memory-Mapped Database). LMDB is incredibly fast because it relies on the Operating System's **Page Cache** (RAM) to keep frequently accessed data ready for the CPU.

However, LMDB has a weakness: **Large Payloads.**
If we store large items (e.g., a 5MB PDF or a huge JSON blob) directly in the main B-Tree:
1.  **Overflow Pages:** LMDB has to allocate special "overflow pages" which fragment the database file.
2.  **Cache Thrashing:** When a user reads this 5MB blob, the OS must load 5MB of data into RAM. To make space, the OS might **evict** (throw out) 5MB of "hot" event history (the `events_log`) that other users are actively reading.

This degrades the performance of the entire system for a single read.

### 2. The Proposed Solution
We need a **Hybrid Storage Strategy**. We will enforce a size threshold. Small items go in the fast lane; big items go in the slow lane.

*   **Primary Store (`events.mdb`):** Only stores payloads `< 2KB`.
*   **Sidecar Store (`blobs.mdb`):** Stores payloads `> 2KB`.

We need to implement an "Indirection Layer" in our Event serialization logic.

### 3. Technical Implementation

#### A. Schema Evolution
We need to modify our `Event` enum (in `src/model.rs`) to support remote references.

```rust
// Concept Code
pub enum Payload {
    Inline(Vec<u8>),      // For small data (Fast)
    BlobRef(BlobHash),    // For large data (The pointer)
}
```

#### B. The Write Path (Logic)
In the `Writer` struct, implement the split logic:
1.  Calculate size of `event.payload`.
2.  **IF size < 2KB:** Serialize as `Payload::Inline`. Write to `events.mdb`.
3.  **IF size >= 2KB:**
    *   Calculate Hash (SHA-256) of payload.
    *   Write payload to `blobs.mdb` (Key: Hash, Value: Data).
    *   Serialize as `Payload::BlobRef(Hash)`.
    *   Write the event (with the reference) to `events.mdb`.

#### C. The Read Path (OS Cache Management) - **CRITICAL**
When we retrieve a large blob from the Sidecar Store, we must instruct the Operating System **NOT** to cache this data permanently. We want to read it, send it to the user, and immediately free that RAM.

We must use **Memory Advice** flags. Since we are using Memory Mapping (`mmap`), we need to use `madvise`.

**Requirements:**
1.  Open a read transaction on `blobs.mdb`.
2.  Get the reference slice to the data.
3.  Copy the data to the outgoing buffer (network socket).
4.  **Immediately trigger `madvise` with `MADV_DONTNEED` on that memory range.**

> **Note on Flags:**
> *   `MADV_DONTNEED`: Tells the kernel "I am done with this range. You can free these pages immediately." This protects our precious Page Cache from being filled with cold blob data.
> *   If we were using standard file I/O (not LMDB) for the blobs, we would use `posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED)`.

### 4. Acceptance Criteria
*   [ ] A constant `MAX_INLINE_SIZE = 2048` is defined.
*   [ ] Payloads larger than this constant are automatically routed to the secondary store.
*   [ ] Reading a large blob does not permanently increase the "Resident Set Size" (RAM usage) of the process (validated via `top` or `htop` during a load test).
*   [ ] The `Event` struct can transparently handle both Inline and BlobRef variants.

### 5. Resources
*   [Man Page: madvise(2)](https://man7.org/linux/man-pages/man2/madvise.2.html)
*   [Heed Crate Documentation (LMDB Wrapper)](https://docs.rs/heed/) - Look for `unsafe` access to underlying pointers if `heed` doesn't expose `madvise` directly.
