// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use prometheus::{Histogram, IntCounter, Registry};

/// Prometheus metrics for VarveDB.
///
/// Tracks write latency, read latency, and event counts.
///
/// # Metrics
/// - `varvedb_write_duration_seconds`: Histogram of write latency.
/// - `varvedb_read_duration_seconds`: Histogram of read latency.
/// - `varvedb_events_written_total`: Counter of total events written.
#[derive(Debug, Clone)]
pub struct VarveMetrics {
    pub events_appended: IntCounter,
    pub bytes_written: IntCounter,
    pub append_latency: Histogram,
    pub events_read: IntCounter,
}

impl VarveMetrics {
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let events_appended = IntCounter::new(
            "varvedb_events_appended_total",
            "Total number of events appended",
        )?;
        let bytes_written = IntCounter::new(
            "varvedb_bytes_written_total",
            "Total bytes written to event log",
        )?;
        let append_latency = Histogram::with_opts(prometheus::HistogramOpts::new(
            "varvedb_append_duration_seconds",
            "Duration of append operations",
        ))?;
        let events_read =
            IntCounter::new("varvedb_events_read_total", "Total number of events read")?;

        registry.register(Box::new(events_appended.clone()))?;
        registry.register(Box::new(bytes_written.clone()))?;
        registry.register(Box::new(append_latency.clone()))?;
        registry.register(Box::new(events_read.clone()))?;

        Ok(Self {
            events_appended,
            bytes_written,
            append_latency,
            events_read,
        })
    }
}
