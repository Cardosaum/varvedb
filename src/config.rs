// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Configuration types for VarveDB.

use crate::constants;

/// Configuration for opening a VarveDB database.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VarveConfig {
    /// Maximum number of named databases (LMDB sub-databases).
    pub max_dbs: u32,
    /// Maximum size of the memory-mapped database file.
    pub map_size: usize,
}

impl Default for VarveConfig {
    fn default() -> Self {
        Self {
            max_dbs: constants::DEFAULT_MAX_DBS,
            map_size: constants::DEFAULT_MAP_SIZE,
        }
    }
}
