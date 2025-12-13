// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use heed3::byteorder::BigEndian;
use heed3::types::Bytes;
use heed3::types::U64;
use heed3::EncryptedDatabase;

pub type SequenceKey = U64<BigEndian>;
pub type EventsDb = EncryptedDatabase<SequenceKey, Bytes>;