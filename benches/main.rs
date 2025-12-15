// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! VarveDB Benchmark Suite
//!
//! This is the main Criterion harness that orchestrates all benchmark groups.
//! Run with: `cargo bench --bench main`
//!
//! # Benchmark Groups
//!
//! - **write**: append, append_alloc, append_batch, append_batch_alloc
//! - **read**: get_bytes, get_archived, reader operations
//! - **stream**: iter_stream, global iteration
//!
//! # Percentile Tracking
//!
//! In addition to Criterion's statistical analysis, this suite uses hdrhistogram
//! to capture explicit percentile values (p50, p75, p90, p95, p99) which are
//! written to `target/criterion/percentiles.json` after each run.
//!
//! # Running Specific Groups
//!
//! ```bash
//! # Run all benchmarks
//! cargo bench --bench main
//!
//! # Run only write benchmarks
//! cargo bench --bench main -- write
//!
//! # Run only read benchmarks
//! cargo bench --bench main -- read
//!
//! # Run only stream benchmarks
//! cargo bench --bench main -- stream
//!
//! # Save baseline for comparison
//! cargo bench --bench main -- --save-baseline main
//!
//! # Compare against baseline
//! cargo bench --bench main -- --baseline main
//! ```

#[path = "common/mod.rs"]
mod common;
mod read_benchmarks;
mod stream_benchmarks;
mod write_benchmarks;

use criterion::{criterion_group, Criterion};
use std::sync::OnceLock;

use common::PercentileRecorder;

// Global percentile recorder (thread-safe)
static RECORDER: OnceLock<PercentileRecorder> = OnceLock::new();

fn get_recorder() -> &'static PercentileRecorder {
    RECORDER.get_or_init(PercentileRecorder::new)
}

// =============================================================================
// Benchmark Groups
// =============================================================================

fn write_benchmarks(c: &mut Criterion) {
    let recorder = get_recorder();
    write_benchmarks::register_write_benchmarks(c, recorder);
}

fn read_benchmarks(c: &mut Criterion) {
    let recorder = get_recorder();
    read_benchmarks::register_read_benchmarks(c, recorder);
}

fn stream_benchmarks(c: &mut Criterion) {
    let recorder = get_recorder();
    stream_benchmarks::register_stream_benchmarks(c, recorder);
}

// =============================================================================
// Criterion Configuration
// =============================================================================

criterion_group! {
    name = write_group;
    config = Criterion::default()
        .sample_size(100)
        .measurement_time(std::time::Duration::from_secs(5))
        .warm_up_time(std::time::Duration::from_secs(1));
    targets = write_benchmarks
}

criterion_group! {
    name = read_group;
    config = Criterion::default()
        .sample_size(100)
        .measurement_time(std::time::Duration::from_secs(5))
        .warm_up_time(std::time::Duration::from_secs(1));
    targets = read_benchmarks
}

criterion_group! {
    name = stream_group;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(std::time::Duration::from_secs(10))
        .warm_up_time(std::time::Duration::from_secs(2));
    targets = stream_benchmarks
}

// =============================================================================
// Main Entry Point
// =============================================================================

// Custom main to output percentiles at the end
fn main() {
    // Run all criterion benchmarks
    write_group();
    read_group();
    stream_group();

    // Output percentile results
    let recorder = get_recorder();

    // Print to console
    recorder.print_results();

    // Write to JSON file
    let json_path = "target/criterion/percentiles.json";
    if let Err(e) = recorder.write_json(json_path) {
        eprintln!("Warning: Failed to write percentiles JSON: {}", e);
    } else {
        println!("\nPercentile results written to: {}", json_path);
    }

    // Also write a summary CSV for easy import
    let csv_path = "target/criterion/percentiles.csv";
    if let Err(e) = write_percentiles_csv(recorder, csv_path) {
        eprintln!("Warning: Failed to write percentiles CSV: {}", e);
    } else {
        println!("Percentile results written to: {}", csv_path);
    }
}

fn write_percentiles_csv(recorder: &PercentileRecorder, path: &str) -> std::io::Result<()> {
    use std::fs::File;
    use std::io::Write;

    let results = recorder.results();
    let mut file = File::create(path)?;

    // Header
    writeln!(file, "benchmark,p50,p75,p90,p95,p99,min,max,mean,count")?;

    // Sort by name for consistent output
    let mut names: Vec<_> = results.keys().collect();
    names.sort();

    for name in names {
        let r = &results[name];
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{:.1},{}",
            name, r.p50, r.p75, r.p90, r.p95, r.p99, r.min, r.max, r.mean, r.count
        )?;
    }

    Ok(())
}
