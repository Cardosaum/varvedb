// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

/// Default buffer size for serialization.
pub const DEFAULT_SERIALIZATION_BUFFER_SIZE: usize = 1024;

/// Size of the encryption key in bytes (AES-256).
pub const KEY_SIZE: usize = 32;

/// Size of the nonce in bytes.
pub const NONCE_SIZE: usize = 12;

/// Size of the Stream ID in bytes.
pub const STREAM_ID_SIZE: usize = 16;

/// Size of the Sequence Number in bytes.
pub const SEQ_SIZE: usize = 8;

/// Capacity for Additional Authenticated Data (AAD) vector.
/// AAD consists of Stream ID + Sequence Number.
pub const AAD_CAPACITY: usize = STREAM_ID_SIZE + SEQ_SIZE;

/// Minimum size of an encrypted event.
/// Must contain at least Stream ID and Nonce.
pub const ENCRYPTED_EVENT_MIN_SIZE: usize = STREAM_ID_SIZE + NONCE_SIZE;

/// Default batch size for the processor.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Default batch timeout in milliseconds for the processor.
pub const DEFAULT_BATCH_TIMEOUT_MS: u64 = 100;
