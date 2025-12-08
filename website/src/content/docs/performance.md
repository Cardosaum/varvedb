# Performance

VarveDB is designed to be as fast as possible while maintaining strong consistency guarantees.

## Benchmarks

*Hardware: MacBook Pro M2, NVMe SSD*

| Operation | Throughput | Latency (p99) |
| :--- | :--- | :--- |
| **Append** (1KB Event) | ~60,000 ops/sec | ~1.2 ms |
| **Read** (Random Access) | ~800,000 ops/sec | < 0.1 ms |
| **Sequential Scan** | ~1.5 M ops/sec | < 0.05 ms |

> Note: These numbers are indicative and depend heavily on hardware, specifically disk IOPS for writes and RAM speed for reads.

## Design Trade-offs

### Memory Usage
Since VarveDB uses memory-mapped files, the operating system manages memory usage. Your process might appear to use a lot of virtual memory, but physical RAM usage is dynamic. The OS will evict pages from the cache as needed.
*   **Tip**: Ensure your dataset fits reasonably within available RAM for optimal read performance.

### Write Amplification
LMDB uses a copy-on-write B-tree. This provides safety but can cause write amplification.
*   **Tip**: Use batching if you need to ingest millions of events quickly (batch API coming soon).

### Storage Space
`rkyv` is a binary format and is generally compact, but it prioritizes alignment for zero-copy access over compression.
*   **Tip**: If you store large strings or blobs, consider compressing them *before* putting them into your event struct if storage space is a primary concern.

## Hardware Recommendations

1.  **Storage**: **NVMe SSD** is highly recommended. Appending to the log requires frequent `fsync` calls (unless `Async` durability is chosen). HDD performance will be significantly lower.
2.  **Memory**: More RAM = more OS page cache = faster reads.
3.  **CPU**: Serialization is rarely the bottleneck due to `rkyv`. CPU is mostly used for hashing (indexes) and encryption (if enabled).
