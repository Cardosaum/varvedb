// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use rkyv::{Archive, Deserialize, Serialize};

/// Represents the payload of an event, which can be stored inline or as a reference to a blob.
#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[rkyv(derive(Debug))]
#[repr(C)]
pub enum StoragePayload {
    /// Small data stored directly in the event log.
    Inline(Vec<u8>),
    /// Large data stored in the blob store, referenced by its hash.
    /// The hash is a SHA-256 hash (32 bytes).
    BlobRef([u8; 32]),
}

/// A container for an event and its associated metadata.
///
/// This structure is used to pass data to `Varve::append`.
pub struct Payload<E, M> {
    pub event: E,
    pub metadata: M,
}

impl<E, M> Payload<E, M> {
    /// Creates a new `Payload` with the given event and metadata.
    pub fn new(event: E, metadata: M) -> Self {
        Self { event, metadata }
    }
}
