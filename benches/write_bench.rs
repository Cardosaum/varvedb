// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::Writer;
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug)]
#[archive(check_bytes)]
#[repr(C)]
pub struct BenchEvent {
    pub id: u64,
    pub payload: [u8; 256], // 256 bytes payload
}

fn write_benchmark(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let config = StorageConfig {
        path: dir.path().join("bench.mdb"),
        ..Default::default()
    };
    let storage = Storage::open(config).unwrap();
    let mut writer = Writer::<BenchEvent>::new(storage.clone());

    let mut group = c.benchmark_group("write_throughput");
    group.throughput(Throughput::Elements(1));

    let mut i = 0;
    group.bench_function("append_event", |b| {
        b.iter(|| {
            let event = BenchEvent {
                id: i,
                payload: [0u8; 256],
            };
            writer.append(1, i as u32, event).unwrap();
            i += 1;
        })
    });
    group.finish();
}

criterion_group!(benches, write_benchmark);
criterion_main!(benches);
