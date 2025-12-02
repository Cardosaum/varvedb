use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rkyv::{Archive, Deserialize, Serialize};
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

fn read_benchmark(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let config = StorageConfig {
        path: dir.path().join("bench_read.mdb"),
        map_size: 10 * 1024 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true, encryption_enabled: false,
    };
    let storage = Storage::open(config).unwrap();
    let mut writer = Writer::<BenchEvent>::new(storage.clone());
    let reader = Reader::<BenchEvent>::new(storage.clone());

    // Pre-populate
    let count = 10_000;
    for i in 0..count {
        let event = BenchEvent {
            id: i,
            payload: [0u8; 256],
        };
        writer.append(1, i as u32, event).unwrap();
    }

    let txn = storage.env.read_txn().unwrap();
    let mut group = c.benchmark_group("read_throughput");
    group.throughput(Throughput::Elements(1));

    let mut i = 1;
    group.bench_function("read_event_sequential", |b| {
        b.iter(|| {
            criterion::black_box(reader.get(&txn, i).unwrap());
            i = (i % count) + 1;
        })
    });
    group.finish();
}

criterion_group!(benches, read_benchmark);
criterion_main!(benches);
