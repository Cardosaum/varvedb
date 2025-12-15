// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

pub mod constants;
pub mod types;
#[macro_use]
pub mod utils;
pub mod log;
pub mod stream;
pub mod varve;

// Re-export main types for convenience
pub use stream::{Stream, StreamReader};
pub use types::{
    EventMeta, GlobalEventRecord, GlobalSequence, StreamId, StreamKey, StreamSequence,
};
pub use varve::{Error, GlobalReader, HighSerializer, LowSerializer, Varve, VarveConfig};
