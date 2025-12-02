use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
#[repr(C)]
pub struct BenchEvent {
    pub id: u64,
    pub payload: [u8; 256],
}

fn concurrent_benchmark(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let config = StorageConfig {
        path: dir.path().join("bench_concurrent.mdb"),
        map_size: 10 * 1024 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true,
    };
    let storage = Storage::open(config).unwrap();

    // Pre-populate
    let mut writer = Writer::<BenchEvent>::new(storage.clone());
    let count = 10_000;
    for i in 0..count {
        let event = BenchEvent {
            id: i,
            payload: [0u8; 256],
        };
        writer.append(1, i as u32, event).unwrap();
    }

    let mut group = c.benchmark_group("concurrent_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("concurrent_reads_4_threads", |b| {
        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            let mut handles = vec![];
            let barrier = Arc::new(Barrier::new(5)); // 4 readers + 1 main

            for _ in 0..4 {
                let storage = storage.clone();
                let barrier = barrier.clone();
                handles.push(thread::spawn(move || {
                    let reader = Reader::<BenchEvent>::new(storage.clone());
                    let txn = storage.env.read_txn().unwrap();
                    barrier.wait();
                    for i in 0..iters {
                        criterion::black_box(reader.get(&txn, (i % count) + 1).unwrap());
                    }
                }));
            }

            barrier.wait(); // Start all threads
            for h in handles {
                h.join().unwrap();
            }
            start.elapsed()
        })
    });
    group.finish();
}

criterion_group!(benches, concurrent_benchmark);
criterion_main!(benches);
