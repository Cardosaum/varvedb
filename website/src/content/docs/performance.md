---
title: "Performance"
description: "Benchmark results, performance characteristics, and optimization tips for VarveDB."
---

# Performance

VarveDB delivers exceptional performance through its zero-copy architecture and memory-mapped storage. Real-world benchmarks demonstrate sub-microsecond reads and over 1 million events/sec write throughput.

## Benchmark Results

> **Hardware:** MacBook Pro M2, NVMe SSD  
> **Dataset:** 1,000,000 events (multi-threaded stress test)

### Write Performance

| Operation | Throughput | Avg Latency |
|:---|---:|---:|
| **Batch Write** (1M events) | **1.02M events/sec** | **978 ns/event** |
| Total append time | - | 978.14 ms |
| Event generation time | - | 87.74 ms |

**Key Insights:**

- **Million+ events per second** throughput achieved with large batch writes
- Sub-microsecond average latency per event when batching
- **Batching amortizes the commit cost** — Writing in batches achieves orders of magnitude higher throughput than individual appends
- Event generation overhead is minimal compared to append time
- Database size has minimal impact on write performance

### Read Performance

| Operation | Throughput | Avg Latency |
|:---|---:|---:|
| **Sequential Read** (1M events by global sequence) | **2.28M events/sec** | **438 ns/event** |
| Single event read | - | **543.67 µs** |
| Total read time (1M events) | - | 438.63 ms |

**Key Insights:**

- **2.28 million events per second** sequential read throughput
- **Sub-microsecond average latency** per event thanks to memory mapping and zero-copy deserialization
- Consistent performance at scale — Reading 1M events maintains excellent throughput
- Zero-copy architecture eliminates deserialization overhead
- Memory-mapped access provides near-RAM speed for hot data

### Streaming & Iteration

| Operation | Throughput | Avg Latency |
|:---|---:|---:|
| **Stream Iterator** (8M events) | **2.92M events/sec** | **342 ns/event** |
| Total iteration time (8M events) | - | 2.74 s |

**Key Insights:**

- **Nearly 3 million events per second** iteration throughput
- Scales to millions of events with consistent sub-microsecond per-event latency
- Iteration is extremely fast due to sequential disk access patterns and page cache efficiency
- Zero-copy architecture provides direct memory-mapped access without deserialization overhead

### Performance Summary

| Operation | Throughput | Avg Latency |
|:---|---:|---:|
| **Batch Write** | 1.02M events/sec | 978 ns/event |
| **Sequential Read** | 2.28M events/sec | 438 ns/event |
| **Stream Iterator** | 2.92M events/sec | 342 ns/event |

> All operations demonstrate **sub-microsecond per-event latency** and million+ events/sec throughput, showcasing VarveDB's exceptional performance characteristics.

## Performance Characteristics

### Memory Usage

VarveDB uses memory-mapped files via LMDB. The operating system manages physical memory usage dynamically:

- **Virtual Memory** — Your process may show high virtual memory usage (the entire DB file is mapped)
- **Physical Memory** — Only actively accessed pages reside in RAM. The OS evicts cold pages automatically
- **Tip** — For optimal read performance, ensure your working set (frequently accessed events) fits in available RAM

### Write Amplification

LMDB uses a copy-on-write B-tree, which provides crash safety but introduces write amplification:

- Each transaction modifies B-tree nodes, causing multiple disk writes per logical append
- **Mitigation** — Use `append_batch` to amortize the overhead across multiple events

### Storage Space

`rkyv` prioritizes zero-copy access over compression, resulting in aligned binary layouts:

- Typical overhead: 5-15% compared to serialization formats like MessagePack
- **Tip** — If storage is critical, compress large fields (strings, blobs) *before* storing them in your event struct

## Hardware Recommendations

### 1. Storage: NVMe SSD Strongly Recommended

- Synchronous writes require frequent `fsync` calls for durability
- **NVMe SSD** — ~4-5ms write latency (as shown in benchmarks)
- **SATA SSD** — ~8-12ms write latency
- **HDD** — 20-50ms write latency (not recommended)

### 2. Memory: More is Better

- More RAM = larger page cache = fewer disk reads
- For a 1GB database with random access patterns, aim for at least 2GB of free RAM

### 3. CPU: Rarely the Bottleneck

- `rkyv` serialization is extremely fast (sub-microsecond for small events)
- CPU usage is dominated by B-tree traversal (indexing), which is already optimized by LMDB

## Optimization Tips

1. **Batch Your Writes** — Use `append_batch` to achieve 100-1000× throughput gains
2. **Preallocate Buffers** — Reuse `Stream` handles to avoid repeated buffer allocations
3. **Use Sequential Iteration** — When rebuilding projections, iterate globally or by stream rather than random access
4. **Monitor Page Cache** — Use tools like `vmstat` or `iotop` to ensure your working set is cached
5. **Tune LMDB Map Size** — Set `VarveConfig::map_size` to be larger than your expected database size to avoid resizing overhead

## Benchmarking Methodology

All benchmarks are available in the `benches/` directory and can be reproduced with:

```bash
cargo bench --bench varvedb_benchmarks
```

Results include:
- **HTML Report**: `target/criterion/report/index.html`
- **Percentile Data**: `target/criterion/percentiles.json` and `percentiles.csv`

We use Criterion.rs for statistical analysis and hdrhistogram for accurate percentile measurement.
