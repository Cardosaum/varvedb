use prometheus::{IntCounter, Histogram, register_int_counter, register_histogram, Registry};
use std::sync::Arc;

pub struct VarveMetrics {
    pub events_appended: IntCounter,
    pub bytes_written: IntCounter,
    pub append_latency: Histogram,
    pub events_read: IntCounter,
}

impl VarveMetrics {
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        let events_appended = IntCounter::new("varvedb_events_appended_total", "Total number of events appended")?;
        let bytes_written = IntCounter::new("varvedb_bytes_written_total", "Total bytes written to event log")?;
        let append_latency = Histogram::with_opts(prometheus::HistogramOpts::new("varvedb_append_duration_seconds", "Duration of append operations"))?;
        let events_read = IntCounter::new("varvedb_events_read_total", "Total number of events read")?;

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
