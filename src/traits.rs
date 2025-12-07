// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

/// Trait to extract metadata required for event storage.
///
/// This trait must be implemented by the metadata type used in `Varve`.
/// It allows the database engine to determine the stream to which an event belongs
/// and the expected version (for optimistic concurrency control).
pub trait MetadataExt {
    /// Returns the unique identifier of the stream this event belongs to.
    fn stream_id(&self) -> u128;

    /// Returns the version (sequence number) of the event within the stream.
    ///
    /// This should correspond to the expected version of the stream *after* this event is appended?
    /// No, usually this is the version OF this event.
    /// In VarveDB model:
    /// - `seq` is global.
    /// - `version` is per-stream.
    ///
    /// When appending, we check if `version` already exists.
    /// So this returns the version this event SHOULD have.
    fn version(&self) -> u32;
}
