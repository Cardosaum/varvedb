// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::Writer;
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[archive(check_bytes)]
#[repr(C)]
pub struct PayloadEvent {
    pub payload: Vec<u8>,
}

fn payload_size_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("payload_size");

    for size in [128, 1024, 1024 * 10, 1024 * 100].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let dir = tempdir().unwrap();
            let config = StorageConfig {
                path: dir.path().join(format!("bench_payload_{}.mdb", size)),
                map_size: 10 * 1024 * 1024 * 1024,
                max_dbs: 10,
                create_dir: true,
                encryption_enabled: false,
                master_key: None,
            };
            let storage = Storage::open(config).unwrap();
            let mut writer = Writer::<PayloadEvent>::new(storage.clone());

            let payload = vec![0u8; size];
            let event = PayloadEvent { payload };
            let mut i = 0;

            b.iter(|| {
                // We clone the event here which adds overhead, but we want to measure write throughput
                // including serialization of large payloads.
                // Actually, rkyv serialization is what we want to test + LMDB write.
                writer.append(1, i, event.clone()).unwrap(); // Clone is necessary as append takes ownership
                i += 1;
            });
        });
    }
    group.finish();
}

criterion_group!(benches, payload_size_benchmark);
criterion_main!(benches);
