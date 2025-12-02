// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use prometheus::Registry;
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::Arc;
use tempfile::tempdir;
use varvedb::engine::{Reader, Writer};
use varvedb::metrics::VarveMetrics;
use varvedb::storage::{Storage, StorageConfig};

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(check_bytes)]
#[repr(C)]
pub struct MetricEvent {
    pub id: u64,
}

#[test]
fn test_metrics_collection() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = StorageConfig {
        path: dir.path().join("test_metrics.mdb"),
        map_size: 10 * 1024 * 1024,
        max_dbs: 10,
        create_dir: true,
        encryption_enabled: false,
        master_key: None,
    };

    let storage = Storage::open(config)?;

    // Setup Metrics
    let registry = Registry::new();
    let metrics = Arc::new(VarveMetrics::new(&registry)?);

    let mut writer = Writer::<MetricEvent>::new(storage.clone()).with_metrics(metrics.clone());
    let reader = Reader::<MetricEvent>::new(storage.clone()).with_metrics(metrics.clone());

    // Write Event
    let event = MetricEvent { id: 1 };
    writer.append(1, 1, event)?;

    // Read Event
    let txn = storage.env.read_txn()?;
    let _ = reader.get(&txn, 1)?;

    // Verify Metrics
    let metric_families = registry.gather();

    let events_appended = metric_families
        .iter()
        .find(|m| m.get_name() == "varvedb_events_appended_total")
        .expect("events_appended metric not found");
    assert_eq!(
        events_appended.get_metric()[0].get_counter().get_value(),
        1.0
    );

    let events_read = metric_families
        .iter()
        .find(|m| m.get_name() == "varvedb_events_read_total")
        .expect("events_read metric not found");
    assert_eq!(events_read.get_metric()[0].get_counter().get_value(), 1.0);

    Ok(())
}
