---
title: "Performance"
description: "Benchmark results, performance characteristics, and optimization tips for VarveDB."
---

# Performance

VarveDB delivers exceptional performance through its zero-copy architecture and memory-mapped storage. Real-world benchmarks demonstrate sub-microsecond reads and consistent write latency.

## Benchmark Results

> **Hardware:** MacBook Pro M2, NVMe SSD  
> **Framework:** Criterion.rs + hdrhistogram for percentile tracking

### Write Performance

| Operation | Throughput | Latency (p50) | Latency (p99) |
|:---|---:|---:|---:|
| Single Append (24B) | ~220 ops/sec | 4.8 ms | 7.9 ms |
| Single Append (1KB) | ~235 ops/sec | 4.2 ms | 6.1 ms |
| Batch Append (10 × 24B) | ~2,200 ops/sec | 4.5 ms | 6.0 ms |
| Batch Append (100 × 24B) | ~20,000 ops/sec | 5.0 ms | 7.1 ms |
| Batch Append (1000 × 24B) | **~165,000 ops/sec** | 6.0 ms | 8.0 ms |

**Key Insights:**

- Single writes are limited by fsync overhead (~4-5ms per transaction), which is expected for durable writes
- **Batching amortizes the commit cost** — A batch of 1000 events achieves **700× higher throughput** than individual appends
- Write latency is consistent regardless of payload size (24B to 1KB) since LMDB handles small writes efficiently
- Database size has minimal impact on write performance (empty vs 100K pre-existing events show <5% variance)

### Read Performance

| Operation | Throughput | Latency (p50) | Latency (p99) |
|:---|---:|---:|---:|
| Sequential Read (1K DB) | ~1.4M ops/sec | **460 ns** | 750 ns |
| Random Read (1K DB) | ~1.4M ops/sec | 500 ns | 790 ns |
| Sequential Read (100K DB) | ~850K ops/sec | 790 ns | 2.0 µs |
| Random Read (100K DB) | ~600K ops/sec | 1.0 µs | 6.3 µs |

**Key Insights:**

- **Sub-microsecond latency** for small databases thanks to memory mapping and zero-copy deserialization
- Read performance scales well — Even with 100K events, p50 latency remains under 1 microsecond
- Random access shows only marginal degradation vs sequential access due to efficient B-tree indexing
- The p99 tail latency is excellent (<10µs even for 100K random reads), indicating stable performance

### Streaming & Iteration

| Operation | Throughput | Latency |
|:---|---:|---:|
| Full Stream Scan (1K events) | ~5.7M events/sec | 175 µs total |
| Full Stream Scan (100K events) | ~3.6M events/sec | 27.6 ms total |
| Global Iteration (100K events) | **~7.0M events/sec** | 14.4 ms total |

**Key Insights:**

- Iteration is extremely fast due to sequential disk access patterns and page cache efficiency
- Global iteration (across all streams) is faster than stream-specific iteration for large datasets

### Latency Distribution

All operations show excellent tail latency behavior:

| Percentile | Write (Batch 1K) | Read (Random 100K) | Stream Scan (1K) |
|:---|---:|---:|---:|
| **p50** | 6.0 ms | 1.0 µs | 146 µs |
| **p75** | 6.2 ms | 1.5 µs | 152 µs |
| **p90** | 6.9 ms | 2.3 µs | 160 µs |
| **p95** | 7.2 ms | 3.1 µs | 168 µs |
| **p99** | 8.8 ms | 6.3 µs | 187 µs |

> The narrow spread between p50 and p99 demonstrates **predictable, consistent performance**.

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
