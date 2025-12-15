// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Stream iteration benchmarks: iter_stream, global iteration

use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use varvedb::{GlobalSequence, StreamId, StreamSequence};

use super::common::{BenchFixture, DatabaseSize, PercentileRecorder, SmallEvent, TimedOperation};

// =============================================================================
// Stream Iteration Benchmarks
// =============================================================================

/// Benchmark iter_stream().collect_bytes()
pub fn bench_iter_stream_collect(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("stream/iter_collect_bytes");

    for db_size in [DatabaseSize::Events1K, DatabaseSize::Events100K] {
        let count = db_size.count() as u64;
        group.throughput(Throughput::Elements(count));

        group.bench_with_input(
            BenchmarkId::new("full_stream", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(size);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let reader = stream.reader();

                b.iter(|| {
                    let bench_name = format!("iter_collect_bytes/full_stream/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let iter = reader.iter_stream(StreamId(1), None).expect("iter");
                    let result = iter.collect_bytes();
                    black_box(result)
                });
            },
        );

        // Iterate from middle of stream
        group.bench_with_input(
            BenchmarkId::new("half_stream", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(size);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let reader = stream.reader();
                let start_seq = StreamSequence((size.count() / 2) as u64);

                b.iter(|| {
                    let bench_name = format!("iter_collect_bytes/half_stream/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let iter = reader
                        .iter_stream(StreamId(1), Some(start_seq))
                        .expect("iter");
                    let result = iter.collect_bytes();
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark iter_stream().for_each()
pub fn bench_iter_stream_for_each(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("stream/iter_for_each");

    for db_size in [DatabaseSize::Events1K, DatabaseSize::Events100K] {
        let count = db_size.count() as u64;
        group.throughput(Throughput::Elements(count));

        group.bench_with_input(
            BenchmarkId::new("full_stream", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(size);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let reader = stream.reader();

                b.iter(|| {
                    let bench_name = format!("iter_for_each/full_stream/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let iter = reader.iter_stream(StreamId(1), None).expect("iter");
                    let mut count = 0u64;
                    let result = iter.for_each(|_seq, _bytes| {
                        count += 1;
                    });
                    black_box((result, count))
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Global Iteration Benchmarks
// =============================================================================

/// Benchmark global_reader.iter_from().collect_all()
pub fn bench_global_iter_collect(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("stream/global_iter_collect");

    for db_size in [DatabaseSize::Events1K, DatabaseSize::Events100K] {
        let count = db_size.count() as u64;
        group.throughput(Throughput::Elements(count));

        group.bench_with_input(
            BenchmarkId::new("full", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, varve) = BenchFixture::with_events(size);
                let reader = varve.global_reader();

                b.iter(|| {
                    let bench_name = format!("global_iter_collect/full/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let iter = reader.iter_from(GlobalSequence(0)).expect("iter");
                    let result = iter.collect_all();
                    black_box(result)
                });
            },
        );

        // Iterate from middle
        group.bench_with_input(
            BenchmarkId::new("half", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, varve) = BenchFixture::with_events(size);
                let reader = varve.global_reader();
                let start_seq = GlobalSequence((size.count() / 2) as u64);

                b.iter(|| {
                    let bench_name = format!("global_iter_collect/half/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let iter = reader.iter_from(start_seq).expect("iter");
                    let result = iter.collect_all();
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark global_reader.iter_from().for_each()
pub fn bench_global_iter_for_each(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("stream/global_iter_for_each");

    for db_size in [DatabaseSize::Events1K, DatabaseSize::Events100K] {
        let count = db_size.count() as u64;
        group.throughput(Throughput::Elements(count));

        group.bench_with_input(
            BenchmarkId::new("full", db_size.label()),
            &db_size,
            |b, &size| {
                let (_fixture, varve) = BenchFixture::with_events(size);
                let reader = varve.global_reader();

                b.iter(|| {
                    let bench_name = format!("global_iter_for_each/full/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let iter = reader.iter_from(GlobalSequence(0)).expect("iter");
                    let mut count = 0u64;
                    let result = iter.for_each(|_event| {
                        count += 1;
                    });
                    black_box((result, count))
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Pagination / Chunked Iteration Benchmarks
// =============================================================================

/// Benchmark iterating in chunks (simulating pagination)
pub fn bench_chunked_iteration(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("stream/chunked_iteration");

    let chunk_sizes = [100usize, 500, 1000];

    for chunk_size in chunk_sizes {
        group.throughput(Throughput::Elements(chunk_size as u64));

        group.bench_with_input(
            BenchmarkId::new("stream_chunk", chunk_size),
            &chunk_size,
            |b, &size| {
                let (_fixture, mut varve) = BenchFixture::with_events(DatabaseSize::Events100K);
                let stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let reader = stream.reader();
                let mut start = 0u64;

                b.iter(|| {
                    let bench_name = format!("chunked_iteration/stream_chunk/{}", size);
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let iter = reader
                        .iter_stream(StreamId(1), Some(StreamSequence(start)))
                        .expect("iter");

                    // Simulate pagination by collecting only chunk_size events
                    let mut count = 0;
                    let result = iter.for_each(|_seq, _bytes| {
                        count += 1;
                        if count >= size {
                            return; // Early exit isn't possible with for_each, but we track count
                        }
                    });

                    start = (start + size as u64) % 100_000;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Latency Distribution Benchmarks (for percentile focus)
// =============================================================================

/// Run dedicated percentile-focused iteration to capture latency distribution
pub fn bench_iteration_latency_distribution(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("stream/latency_distribution");

    // Single event iteration for latency measurement
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_event_iter", |b| {
        let (_fixture, mut varve) = BenchFixture::with_events(DatabaseSize::Events1K);
        let stream = varve
            .stream::<SmallEvent, 256>("bench_stream")
            .expect("stream");
        let reader = stream.reader();
        let mut seq = 0u64;

        b.iter(|| {
            let _op = TimedOperation::start(recorder, "latency_distribution/single_event_iter");
            let iter = reader
                .iter_stream(StreamId(1), Some(StreamSequence(seq % 1000)))
                .expect("iter");

            // Just get the first event
            let mut first_bytes = None;
            let _ = iter.for_each(|stream_seq, bytes| {
                if first_bytes.is_none() {
                    first_bytes = Some((stream_seq, bytes.to_vec()));
                }
            });
            seq += 1;
            black_box(first_bytes)
        });
    });

    group.finish();
}

// =============================================================================
// Public API
// =============================================================================

pub fn register_stream_benchmarks(c: &mut Criterion, recorder: &PercentileRecorder) {
    bench_iter_stream_collect(c, recorder);
    bench_iter_stream_for_each(c, recorder);
    bench_global_iter_collect(c, recorder);
    bench_global_iter_for_each(c, recorder);
    bench_chunked_iteration(c, recorder);
    bench_iteration_latency_distribution(c, recorder);
}
