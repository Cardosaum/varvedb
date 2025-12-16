// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Serializer type aliases for rkyv.

use rkyv::rancor::Strategy;

/// Zero-allocation serializer for fixed-size types.
///
/// Use this with `Stream::append` for POD types and other fixed-size data.
pub type LowSerializer<'a> =
    Strategy<rkyv::ser::Serializer<rkyv::ser::writer::Buffer<'a>, (), ()>, rkyv::rancor::Error>;

/// Allocating serializer for arbitrary types.
///
/// Use this with `Stream::append_alloc` for types containing Strings, Vecs, etc.
pub type HighSerializer<'a> = Strategy<
    rkyv::ser::Serializer<
        rkyv::ser::writer::Buffer<'a>,
        rkyv::ser::allocator::ArenaHandle<'a>,
        rkyv::ser::sharing::Share,
    >,
    rkyv::rancor::Error,
>;
