// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

// Stream-based event store database names
pub const GLOBAL_EVENTS_DB_NAME: &str = "global_events";

// Per-stream database naming
pub const STREAM_DB_PREFIX: &str = "stream:";
pub const STREAM_INDEX_SUFFIX: &str = ":index";
pub const STREAM_META_SUFFIX: &str = ":meta";

pub const DEFAULT_MAP_SIZE: usize = 10 * 1024 * 1024; // 10 MB
                                                      // Increased to support multiple stream databases (index + meta per stream) + global
pub const DEFAULT_MAX_DBS: u32 = 64;
