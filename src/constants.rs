// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

/// The default buffer size for serialization.
pub const DEFAULT_SERIALIZATION_BUFFER_SIZE: usize = 4096;

/// The default batch size for the processor.
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// The default batch timeout in milliseconds.
pub const DEFAULT_BATCH_TIMEOUT_MS: u64 = 100;

/// The size of the Stream ID in bytes.
pub const STREAM_ID_SIZE: usize = 16;

/// The size of the encryption key in bytes (AES-256).
pub const KEY_SIZE: usize = 32;

/// The size of the nonce in bytes (AES-GCM).
pub const NONCE_SIZE: usize = 12;

/// The minimum size of an encrypted event (StreamID + Nonce + Tag).
/// StreamID (16) + Nonce (12) + Tag (16) = 44 bytes.
pub const ENCRYPTED_EVENT_MIN_SIZE: usize = 44;

/// The capacity of the AAD buffer (StreamID + Seq).
/// StreamID (16) + Seq (8) = 24 bytes.
pub const AAD_CAPACITY: usize = 24;

/// The maximum size of a payload to be stored inline (2KB).
pub const MAX_INLINE_SIZE: usize = 2048;
