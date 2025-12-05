// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug)]
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
        encryption_enabled: false,
        master_key: None,
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
