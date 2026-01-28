// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Common benchmark fixtures, event types, and helpers.

use rkyv::{Archive, Deserialize, Serialize};
use std::path::PathBuf;
use tempfile::TempDir;
use varvedb::{StreamId, Varve, VarveConfig};

// =============================================================================
// Event Types for Benchmarking
// =============================================================================

/// Small fixed-size event (24 bytes payload)
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(attr(derive(Debug)))]
pub struct SmallEvent {
    pub id: u64,
    pub timestamp: u64,
    pub value: i64,
}

impl SmallEvent {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            timestamp: 1702400000 + id,
            value: (id as i64) * 42,
        }
    }
}

/// Medium fixed-size event (~256 bytes payload)
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(attr(derive(Debug)))]
pub struct MediumEvent {
    pub id: u64,
    pub timestamp: u64,
    pub values: [u64; 28], // 28 * 8 = 224 bytes + 16 bytes header = ~240 bytes
}

impl MediumEvent {
    pub fn new(id: u64) -> Self {
        let mut values = [0u64; 28];
        for (i, v) in values.iter_mut().enumerate() {
            *v = id.wrapping_mul(i as u64 + 1);
        }
        Self {
            id,
            timestamp: 1702400000 + id,
            values,
        }
    }
}

/// Large fixed-size event (~1KB payload)
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(attr(derive(Debug)))]
pub struct LargeEvent {
    pub id: u64,
    pub timestamp: u64,
    pub data: [u8; 1000],
}

impl LargeEvent {
    pub fn new(id: u64) -> Self {
        let mut data = [0u8; 1000];
        // Fill with deterministic pattern
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = ((id as usize + i) % 256) as u8;
        }
        Self {
            id,
            timestamp: 1702400000 + id,
            data,
        }
    }
}

/// Variable-size event with strings (for append_alloc benchmarks)
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(attr(derive(Debug)))]
pub struct StringEvent {
    pub id: u64,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
}

impl StringEvent {
    pub fn new(id: u64, payload_size: PayloadSize) -> Self {
        let (name_len, desc_len, tag_count) = match payload_size {
            PayloadSize::Bytes64 => (8, 20, 2),
            PayloadSize::Bytes256 => (16, 100, 5),
            PayloadSize::Bytes1K => (32, 500, 10),
            PayloadSize::Bytes4K => (64, 2000, 20),
            PayloadSize::Bytes16K => (128, 10000, 50),
        };

        Self {
            id,
            name: "x".repeat(name_len),
            description: "y".repeat(desc_len),
            tags: (0..tag_count).map(|i| format!("tag_{i}")).collect(),
        }
    }
}

// =============================================================================
// Payload Size Configuration
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadSize {
    Bytes64,
    Bytes256,
    Bytes1K,
    Bytes4K,
    Bytes16K,
}

impl PayloadSize {
    pub fn label(&self) -> &'static str {
        match self {
            PayloadSize::Bytes64 => "64B",
            PayloadSize::Bytes256 => "256B",
            PayloadSize::Bytes1K => "1KB",
            PayloadSize::Bytes4K => "4KB",
            PayloadSize::Bytes16K => "16KB",
        }
    }
}

// =============================================================================
// Database Size Configuration
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseSize {
    Empty,
    Events1K,
    Events100K,
}

impl DatabaseSize {
    pub fn count(&self) -> usize {
        match self {
            DatabaseSize::Empty => 0,
            DatabaseSize::Events1K => 1_000,
            DatabaseSize::Events100K => 100_000,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            DatabaseSize::Empty => "empty",
            DatabaseSize::Events1K => "1K",
            DatabaseSize::Events100K => "100K",
        }
    }
}

// =============================================================================
// Batch Size Configuration
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchSize {
    Ten,
    Hundred,
    Thousand,
}

impl BatchSize {
    pub fn count(&self) -> usize {
        match self {
            BatchSize::Ten => 10,
            BatchSize::Hundred => 100,
            BatchSize::Thousand => 1000,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            BatchSize::Ten => "10",
            BatchSize::Hundred => "100",
            BatchSize::Thousand => "1000",
        }
    }
}

// =============================================================================
// Benchmark Fixture
// =============================================================================

/// A benchmark fixture that manages temporary database setup
pub struct BenchFixture {
    _temp_dir: TempDir,
    pub path: PathBuf,
}

impl BenchFixture {
    /// Create a new empty fixture
    pub fn new() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let path = temp_dir.path().to_path_buf();
        Self {
            _temp_dir: temp_dir,
            path,
        }
    }

    /// Create a fixture with a Varve instance
    pub fn with_varve(&self) -> Varve {
        let config = VarveConfig {
            max_dbs: 64,
            map_size: 1024 * 1024 * 1024, // 1GB for benchmarks
            ..VarveConfig::default()
        };
        Varve::with_config(&self.path, config).expect("Failed to create Varve")
    }

    /// Create a fixture pre-populated with events
    pub fn with_events(db_size: DatabaseSize) -> (Self, Varve) {
        let fixture = Self::new();
        let mut varve = fixture.with_varve();

        let count = db_size.count();
        if count > 0 {
            let mut stream = varve
                .stream::<SmallEvent, 256>("bench_stream")
                .expect("Failed to create stream");

            // Use batch append for efficiency
            let batch_size = 1000.min(count);
            let batches = count / batch_size;
            let remainder = count % batch_size;

            for batch_idx in 0..batches {
                let events: Vec<SmallEvent> = (0..batch_size)
                    .map(|i| SmallEvent::new((batch_idx * batch_size + i) as u64))
                    .collect();
                stream
                    .append_batch(StreamId(1), &events)
                    .expect("Failed to batch append");
            }

            if remainder > 0 {
                let events: Vec<SmallEvent> = (0..remainder)
                    .map(|i| SmallEvent::new((batches * batch_size + i) as u64))
                    .collect();
                stream
                    .append_batch(StreamId(1), &events)
                    .expect("Failed to batch append remainder");
            }
        }

        (fixture, varve)
    }
}

impl Default for BenchFixture {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Percentile Recording
// =============================================================================

use hdrhistogram::Histogram;
use serde::Serialize as SerdeSerialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Mutex;
use std::time::Instant;

/// Percentile results for a single benchmark
#[derive(Debug, Clone, SerdeSerialize)]
pub struct PercentileResult {
    pub p50: u64,
    pub p75: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
    pub min: u64,
    pub max: u64,
    pub mean: f64,
    pub count: u64,
}

impl PercentileResult {
    pub fn from_histogram(hist: &Histogram<u64>) -> Self {
        Self {
            p50: hist.value_at_quantile(0.50),
            p75: hist.value_at_quantile(0.75),
            p90: hist.value_at_quantile(0.90),
            p95: hist.value_at_quantile(0.95),
            p99: hist.value_at_quantile(0.99),
            min: hist.min(),
            max: hist.max(),
            mean: hist.mean(),
            count: hist.len(),
        }
    }
}

/// Global percentile recorder for all benchmarks
pub struct PercentileRecorder {
    histograms: Mutex<HashMap<String, Histogram<u64>>>,
}

impl PercentileRecorder {
    pub fn new() -> Self {
        Self {
            histograms: Mutex::new(HashMap::new()),
        }
    }

    /// Record a single timing measurement (in nanoseconds)
    pub fn record(&self, name: &str, nanos: u64) {
        let mut histograms = self.histograms.lock().unwrap();
        let hist = histograms
            .entry(name.to_string())
            .or_insert_with(|| Histogram::new(3).expect("Failed to create histogram"));
        let _ = hist.record(nanos);
    }

    /// Get all results
    pub fn results(&self) -> HashMap<String, PercentileResult> {
        let histograms = self.histograms.lock().unwrap();
        histograms
            .iter()
            .map(|(name, hist)| (name.clone(), PercentileResult::from_histogram(hist)))
            .collect()
    }

    /// Write results to JSON file
    pub fn write_json(&self, path: &str) -> std::io::Result<()> {
        let results = self.results();
        let json = serde_json::to_string_pretty(&results)?;
        let mut file = File::create(path)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }

    /// Print results to console
    pub fn print_results(&self) {
        let results = self.results();
        println!("\n=== Percentile Results (nanoseconds) ===\n");

        let mut names: Vec<_> = results.keys().collect();
        names.sort();

        for name in names {
            let r = &results[name];
            println!(
                "{name}:\n  p50={:>8} p75={:>8} p90={:>8} p95={:>8} p99={:>8}\n  min={:>8} max={:>8} mean={:>8.1} count={}\n",
                r.p50, r.p75, r.p90, r.p95, r.p99, r.min, r.max, r.mean, r.count
            );
        }
    }
}

impl Default for PercentileRecorder {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to time a single operation and record to histogram
pub struct TimedOperation<'a> {
    recorder: &'a PercentileRecorder,
    name: String,
    start: Instant,
}

impl<'a> TimedOperation<'a> {
    pub fn start(recorder: &'a PercentileRecorder, name: impl Into<String>) -> Self {
        Self {
            recorder,
            name: name.into(),
            start: Instant::now(),
        }
    }
}

impl<'a> Drop for TimedOperation<'a> {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos() as u64;
        self.recorder.record(&self.name, elapsed);
    }
}

// =============================================================================
// Test Utilities
// =============================================================================

/// Generate a batch of small events
pub fn generate_small_events(count: usize) -> Vec<SmallEvent> {
    (0..count).map(|i| SmallEvent::new(i as u64)).collect()
}

/// Generate a batch of medium events
pub fn generate_medium_events(count: usize) -> Vec<MediumEvent> {
    (0..count).map(|i| MediumEvent::new(i as u64)).collect()
}

/// Generate a batch of large events
pub fn generate_large_events(count: usize) -> Vec<LargeEvent> {
    (0..count).map(|i| LargeEvent::new(i as u64)).collect()
}

/// Generate a batch of string events
pub fn generate_string_events(count: usize, size: PayloadSize) -> Vec<StringEvent> {
    (0..count)
        .map(|i| StringEvent::new(i as u64, size))
        .collect()
}
