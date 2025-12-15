// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Read operation benchmarks: get_bytes, get_archived, reader operations

use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use rand::prelude::*;
use varvedb::{GlobalSequence, StreamId, StreamSequence};

use super::common::{BenchFixture, DatabaseSize, PercentileRecorder, SmallEvent, TimedOperation};

// =============================================================================
// Single Read Benchmarks
// =============================================================================

/// Benchmark single get_bytes (stream-based read) - this returns owned Vec<u8>
pub fn bench_get_bytes(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("read/get_bytes");
    group.throughput(Throughput::Elements(1));

    for db_size in [DatabaseSize::Events1K, DatabaseSize::Events100K] {
        group.bench_with_input(
            BenchmarkId::new("sequential", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(size);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let max_seq = size.count() as u64;
                let mut seq = 0u64;

                b.iter(|| {
                    let bench_name = format!("get_bytes/sequential/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = stream.get_bytes(StreamId(1), StreamSequence(seq % max_seq));
                    seq += 1;
                    black_box(result)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("random", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(size);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let max_seq = size.count() as u64;
                let mut rng = rand::thread_rng();

                b.iter(|| {
                    let seq = rng.gen_range(0..max_seq);
                    let bench_name = format!("get_bytes/random/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = stream.get_bytes(StreamId(1), StreamSequence(seq));
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark StreamReader.get_bytes - measures whether result is Some/None to avoid lifetime issues
pub fn bench_reader_get_bytes(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("read/reader_get_bytes");
    group.throughput(Throughput::Elements(1));

    for db_size in [DatabaseSize::Events1K, DatabaseSize::Events100K] {
        group.bench_with_input(
            BenchmarkId::new("sequential", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(size);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let mut reader = stream.reader();
                let max_seq = size.count() as u64;
                let mut seq = 0u64;

                b.iter(|| {
                    let bench_name = format!("reader_get_bytes/sequential/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = reader.get_bytes(StreamId(1), StreamSequence(seq % max_seq));
                    // Use is_some() to avoid lifetime escape issues while still benchmarking the read
                    let found = result.as_ref().map(|r| r.is_some()).unwrap_or(false);
                    seq += 1;
                    black_box(found)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("random", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(size);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let mut reader = stream.reader();
                let max_seq = size.count() as u64;
                let mut rng = rand::thread_rng();

                b.iter(|| {
                    let seq = rng.gen_range(0..max_seq);
                    let bench_name = format!("reader_get_bytes/random/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = reader.get_bytes(StreamId(1), StreamSequence(seq));
                    let found = result.as_ref().map(|r| r.is_some()).unwrap_or(false);
                    black_box(found)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark StreamReader.get_archived (with validation)
pub fn bench_reader_get_archived(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("read/reader_get_archived");
    group.throughput(Throughput::Elements(1));

    for db_size in [DatabaseSize::Events1K, DatabaseSize::Events100K] {
        group.bench_with_input(
            BenchmarkId::new("sequential", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(size);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let mut reader = stream.reader();
                let max_seq = size.count() as u64;
                let mut seq = 0u64;

                b.iter(|| {
                    let bench_name = format!("reader_get_archived/sequential/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = reader.get_archived(StreamId(1), StreamSequence(seq % max_seq));
                    let found = result.as_ref().map(|r| r.is_some()).unwrap_or(false);
                    seq += 1;
                    black_box(found)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("random", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(size);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let mut reader = stream.reader();
                let max_seq = size.count() as u64;
                let mut rng = rand::thread_rng();

                b.iter(|| {
                    let seq = rng.gen_range(0..max_seq);
                    let bench_name = format!("reader_get_archived/random/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = reader.get_archived(StreamId(1), StreamSequence(seq));
                    let found = result.as_ref().map(|r| r.is_some()).unwrap_or(false);
                    black_box(found)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark StreamReader.get_archived_unchecked (without validation)
pub fn bench_reader_get_archived_unchecked(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("read/reader_get_archived_unchecked");
    group.throughput(Throughput::Elements(1));

    for db_size in [DatabaseSize::Events1K, DatabaseSize::Events100K] {
        group.bench_with_input(
            BenchmarkId::new("sequential", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(size);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let mut reader = stream.reader();
                let max_seq = size.count() as u64;
                let mut seq = 0u64;

                b.iter(|| {
                    let bench_name =
                        format!("reader_get_archived_unchecked/sequential/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = unsafe {
                        reader.get_archived_unchecked(StreamId(1), StreamSequence(seq % max_seq))
                    };
                    let found = result.as_ref().map(|r| r.is_some()).unwrap_or(false);
                    seq += 1;
                    black_box(found)
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Global Reader Benchmarks
// =============================================================================

/// Benchmark GlobalReader.get (single event by global sequence) - returns owned GlobalEvent
pub fn bench_global_reader_get(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("read/global_reader_get");
    group.throughput(Throughput::Elements(1));

    for db_size in [DatabaseSize::Events1K, DatabaseSize::Events100K] {
        group.bench_with_input(
            BenchmarkId::new("sequential", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, varve) = BenchFixture::with_events(size);
                let mut reader = varve.global_reader();
                let max_seq = size.count() as u64;
                let mut seq = 0u64;

                b.iter(|| {
                    let bench_name = format!("global_reader_get/sequential/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = reader.get(GlobalSequence(seq % max_seq));
                    seq += 1;
                    black_box(result)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("random", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, varve) = BenchFixture::with_events(size);
                let mut reader = varve.global_reader();
                let max_seq = size.count() as u64;
                let mut rng = rand::thread_rng();

                b.iter(|| {
                    let seq = rng.gen_range(0..max_seq);
                    let bench_name = format!("global_reader_get/random/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = reader.get(GlobalSequence(seq));
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark GlobalReader.get_bytes
pub fn bench_global_reader_get_bytes(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("read/global_reader_get_bytes");
    group.throughput(Throughput::Elements(1));

    for db_size in [DatabaseSize::Events1K, DatabaseSize::Events100K] {
        group.bench_with_input(
            BenchmarkId::new("sequential", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, varve) = BenchFixture::with_events(size);
                let mut reader = varve.global_reader();
                let max_seq = size.count() as u64;
                let mut seq = 0u64;

                b.iter(|| {
                    let bench_name = format!("global_reader_get_bytes/sequential/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = reader.get_bytes(GlobalSequence(seq % max_seq));
                    let found = result.as_ref().map(|r| r.is_some()).unwrap_or(false);
                    seq += 1;
                    black_box(found)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark multiple readers accessing the same data
pub fn bench_multi_reader(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("read/multi_reader");
    group.throughput(Throughput::Elements(1));

    // Create multiple readers from the same stream and rotate through them
    group.bench_function("cloned_readers", |b| {
        let (_fixture, mut varve) = BenchFixture::with_events(DatabaseSize::Events1K);
        let stream = varve
            .stream::<SmallEvent, 256>("bench_stream")
            .expect("stream");

        // Create multiple reader clones - we'll use iter_batched to avoid lifetime issues
        let reader = stream.reader();
        let mut seq = 0u64;

        b.iter(|| {
            let mut local_reader = reader.clone();
            let _op = TimedOperation::start(recorder, "multi_reader/cloned_readers");
            let result = local_reader.get_bytes(StreamId(1), StreamSequence(seq % 1000));
            let found = result.as_ref().map(|r| r.is_some()).unwrap_or(false);
            seq += 1;
            black_box(found)
        });
    });

    group.finish();
}

// =============================================================================
// Public API
// =============================================================================

pub fn register_read_benchmarks(c: &mut Criterion, recorder: &PercentileRecorder) {
    bench_get_bytes(c, recorder);
    bench_reader_get_bytes(c, recorder);
    bench_reader_get_archived(c, recorder);
    bench_reader_get_archived_unchecked(c, recorder);
    bench_global_reader_get(c, recorder);
    bench_global_reader_get_bytes(c, recorder);
    bench_multi_reader(c, recorder);
}
