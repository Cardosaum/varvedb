// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Write operation benchmarks: append, append_alloc, append_batch, append_batch_alloc

use criterion::{black_box, BenchmarkId, Criterion, Throughput};
use varvedb::StreamId;

use super::common::{
    generate_large_events, generate_medium_events, generate_small_events, generate_string_events,
    BatchSize, BenchFixture, DatabaseSize, LargeEvent, MediumEvent, PayloadSize,
    PercentileRecorder, SmallEvent, StringEvent, TimedOperation,
};

// =============================================================================
// Single Append Benchmarks
// =============================================================================

/// Benchmark single append with fixed-size events (non-allocating serializer)
pub fn bench_append_single(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("write/append_single");

    // Small events (24 bytes)
    group.throughput(Throughput::Elements(1));
    group.bench_function("small_24B", |b| {
        let fixture = BenchFixture::new();
        let mut varve = fixture.with_varve();
        let mut stream = varve.stream::<SmallEvent, 256>("bench").expect("stream");
        let mut id = 0u64;

        b.iter(|| {
            let event = SmallEvent::new(id);
            let _op = TimedOperation::start(recorder, "append_single/small_24B");
            let result = stream.append(StreamId(1), black_box(&event));
            id += 1;
            black_box(result)
        });
    });

    // Medium events (~256 bytes)
    group.bench_function("medium_256B", |b| {
        let fixture = BenchFixture::new();
        let mut varve = fixture.with_varve();
        let mut stream = varve.stream::<MediumEvent, 512>("bench").expect("stream");
        let mut id = 0u64;

        b.iter(|| {
            let event = MediumEvent::new(id);
            let _op = TimedOperation::start(recorder, "append_single/medium_256B");
            let result = stream.append(StreamId(1), black_box(&event));
            id += 1;
            black_box(result)
        });
    });

    // Large events (~1KB)
    group.bench_function("large_1KB", |b| {
        let fixture = BenchFixture::new();
        let mut varve = fixture.with_varve();
        let mut stream = varve.stream::<LargeEvent, 2048>("bench").expect("stream");
        let mut id = 0u64;

        b.iter(|| {
            let event = LargeEvent::new(id);
            let _op = TimedOperation::start(recorder, "append_single/large_1KB");
            let result = stream.append(StreamId(1), black_box(&event));
            id += 1;
            black_box(result)
        });
    });

    group.finish();
}

/// Benchmark single append with variable-size events (allocating serializer)
pub fn bench_append_alloc_single(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("write/append_alloc_single");
    group.throughput(Throughput::Elements(1));

    for payload_size in [
        PayloadSize::Bytes64,
        PayloadSize::Bytes256,
        PayloadSize::Bytes1K,
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(payload_size.label()),
            &payload_size,
            |b, &size| {
                let fixture = BenchFixture::new();
                let mut varve = fixture.with_varve();
                let mut stream = varve.stream::<StringEvent, 32768>("bench").expect("stream");
                let mut id = 0u64;

                b.iter(|| {
                    let event = StringEvent::new(id, size);
                    let bench_name = format!("append_alloc_single/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = stream.append_alloc(StreamId(1), black_box(&event));
                    id += 1;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Batch Append Benchmarks
// =============================================================================

/// Benchmark batch append with fixed-size events
pub fn bench_append_batch(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("write/append_batch");

    for batch_size in [BatchSize::Ten, BatchSize::Hundred, BatchSize::Thousand] {
        let count = batch_size.count();
        group.throughput(Throughput::Elements(count as u64));

        // Small events batch
        group.bench_with_input(
            BenchmarkId::new("small_24B", batch_size.label()),
            &batch_size,
            |b, &bs| {
                let fixture = BenchFixture::new();
                let mut varve = fixture.with_varve();
                let mut stream = varve.stream::<SmallEvent, 256>("bench").expect("stream");
                let events = generate_small_events(bs.count());

                b.iter(|| {
                    let bench_name = format!("append_batch/small_24B/{}", bs.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = stream.append_batch(StreamId(1), black_box(&events));
                    black_box(result)
                });
            },
        );

        // Medium events batch
        group.bench_with_input(
            BenchmarkId::new("medium_256B", batch_size.label()),
            &batch_size,
            |b, &bs| {
                let fixture = BenchFixture::new();
                let mut varve = fixture.with_varve();
                let mut stream = varve.stream::<MediumEvent, 512>("bench").expect("stream");
                let events = generate_medium_events(bs.count());

                b.iter(|| {
                    let bench_name = format!("append_batch/medium_256B/{}", bs.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = stream.append_batch(StreamId(1), black_box(&events));
                    black_box(result)
                });
            },
        );

        // Large events batch (1KB each)
        group.bench_with_input(
            BenchmarkId::new("large_1KB", batch_size.label()),
            &batch_size,
            |b, &bs| {
                let fixture = BenchFixture::new();
                let mut varve = fixture.with_varve();
                let mut stream = varve.stream::<LargeEvent, 2048>("bench").expect("stream");
                let events = generate_large_events(bs.count());

                b.iter(|| {
                    let bench_name = format!("append_batch/large_1KB/{}", bs.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = stream.append_batch(StreamId(1), black_box(&events));
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark batch append with variable-size events (allocating serializer)
pub fn bench_append_batch_alloc(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("write/append_batch_alloc");

    for batch_size in [BatchSize::Ten, BatchSize::Hundred] {
        let count = batch_size.count();
        group.throughput(Throughput::Elements(count as u64));

        for payload_size in [PayloadSize::Bytes64, PayloadSize::Bytes256] {
            let bench_name = format!("{}/{}", batch_size.label(), payload_size.label());

            group.bench_with_input(
                BenchmarkId::from_parameter(&bench_name),
                &(batch_size, payload_size),
                |b, &(bs, ps)| {
                    let fixture = BenchFixture::new();
                    let mut varve = fixture.with_varve();
                    let mut stream = varve.stream::<StringEvent, 65536>("bench").expect("stream");
                    let events = generate_string_events(bs.count(), ps);

                    b.iter(|| {
                        let bench_name =
                            format!("append_batch_alloc/{}/{}", bs.label(), ps.label());
                        let _op = TimedOperation::start(recorder, &bench_name);
                        let result = stream.append_batch_alloc(StreamId(1), black_box(&events));
                        black_box(result)
                    });
                },
            );
        }
    }

    group.finish();
}

// =============================================================================
// Write to Pre-populated Database
// =============================================================================

/// Benchmark appending to a database that already has events
pub fn bench_append_to_populated(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("write/append_to_populated");
    group.throughput(Throughput::Elements(1));

    // Include Empty as a baseline, plus populated databases
    for db_size in [
        DatabaseSize::Empty,
        DatabaseSize::Events1K,
        DatabaseSize::Events100K,
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(db_size.label()),
            &db_size,
            |b, &size| {
                let (fixture, mut varve) = BenchFixture::with_events(size);
                let _ = fixture; // Keep fixture alive
                let mut stream = varve
                    .stream::<SmallEvent, 256>("bench_stream")
                    .expect("stream");
                let mut id = size.count() as u64;

                b.iter(|| {
                    let event = SmallEvent::new(id);
                    let bench_name = format!("append_to_populated/{}", size.label());
                    let _op = TimedOperation::start(recorder, &bench_name);
                    let result = stream.append(StreamId(1), black_box(&event));
                    id += 1;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

// =============================================================================
// Multi-Stream Writes
// =============================================================================

/// Benchmark writes across multiple stream IDs
pub fn bench_multi_stream_writes(c: &mut Criterion, recorder: &PercentileRecorder) {
    let mut group = c.benchmark_group("write/multi_stream");
    group.throughput(Throughput::Elements(1));

    // Write to different stream IDs (same stream name)
    group.bench_function("different_stream_ids", |b| {
        let fixture = BenchFixture::new();
        let mut varve = fixture.with_varve();
        let mut stream = varve.stream::<SmallEvent, 256>("bench").expect("stream");
        let mut id = 0u64;

        b.iter(|| {
            let event = SmallEvent::new(id);
            let stream_id = StreamId((id % 100) + 1); // Rotate through 100 stream IDs
            let _op = TimedOperation::start(recorder, "multi_stream/different_stream_ids");
            let result = stream.append(stream_id, black_box(&event));
            id += 1;
            black_box(result)
        });
    });

    group.finish();
}

// =============================================================================
// Public API
// =============================================================================

pub fn register_write_benchmarks(c: &mut Criterion, recorder: &PercentileRecorder) {
    bench_append_single(c, recorder);
    bench_append_alloc_single(c, recorder);
    bench_append_batch(c, recorder);
    bench_append_batch_alloc(c, recorder);
    bench_append_to_populated(c, recorder);
    bench_multi_stream_writes(c, recorder);
}
