// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Shared snapshot “due” logic.

use crate::snapshot::{SnapshotAdvice, SnapshotPolicy};

pub(crate) fn compute_advice(
    last_snapshot_cursor: Option<u64>,
    applied_cursor: u64,
    policy: SnapshotPolicy,
) -> SnapshotAdvice {
    let events_since_last_snapshot = match last_snapshot_cursor {
        Some(last) if applied_cursor >= last => applied_cursor - last,
        Some(_) => 0,
        None => applied_cursor.saturating_add(1),
    };

    SnapshotAdvice {
        should_snapshot: events_since_last_snapshot >= policy.every_n_events.get(),
        events_since_last_snapshot,
    }
}
