use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq, Clone)]
#[archive(check_bytes)]
#[archive_attr(derive(Debug, PartialEq))]
struct BenchEvent {
    pub data: [u8; 1024], // 1KB payload
}

fn bench_encryption_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("encryption_overhead");

    for encryption_enabled in [false, true] {
        group.bench_with_input(
            BenchmarkId::new(
                "append_1kb",
                if encryption_enabled {
                    "encrypted"
                } else {
                    "plain"
                },
            ),
            &encryption_enabled,
            |b, &enabled| {
                let dir = tempdir().unwrap();
                let config = StorageConfig {
                    path: dir.path().to_path_buf(),
                    encryption_enabled: enabled,
                    ..Default::default()
                };
                let storage = Storage::open(config.clone()).unwrap();
                let mut writer = Writer::new(storage.clone());
                let event = BenchEvent { data: [0u8; 1024] };

                let mut seq = 0;
                b.iter(|| {
                    seq += 1;
                    writer.append(1, seq, event.clone()).unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new(
                "read_1kb",
                if encryption_enabled {
                    "encrypted"
                } else {
                    "plain"
                },
            ),
            &encryption_enabled,
            |b, &enabled| {
                let dir = tempdir().unwrap();
                let config = StorageConfig {
                    path: dir.path().to_path_buf(),
                    encryption_enabled: enabled,
                    ..Default::default()
                };
                let storage = Storage::open(config.clone()).unwrap();
                let mut writer = Writer::new(storage.clone());
                let event = BenchEvent { data: [0u8; 1024] };

                // Pre-fill
                for i in 1..=100 {
                    writer.append(1, i, event.clone()).unwrap();
                }

                let reader = Reader::<BenchEvent>::new(storage.clone());
                let txn = storage.env.read_txn().unwrap();

                let mut seq = 0;
                b.iter(|| {
                    seq = (seq % 100) + 1;
                    reader.get(&txn, seq).unwrap().unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_encryption_overhead);
criterion_main!(benches);
