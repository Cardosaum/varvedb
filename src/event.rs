// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Event types for VarveDB.

use crate::types::{GlobalSequence, StreamId, StreamSequence};

/// A global event with all metadata.
///
/// This represents a complete event with its position in both the global
/// sequence and within its specific stream.
#[derive(Debug, Clone)]
pub struct GlobalEvent {
    /// The global sequence number of this event.
    pub global_seq: GlobalSequence,
    /// The name of the stream this event belongs to.
    pub stream_name: String,
    /// The stream ID within the named stream.
    pub stream_id: StreamId,
    /// The sequence number within the specific stream_id.
    pub stream_seq: StreamSequence,
    /// The raw payload bytes of the event.
    pub payload: Vec<u8>,
}
