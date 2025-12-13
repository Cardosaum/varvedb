// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use std::path::Path;
use std::sync::Arc;

use heed::{Env, EnvOpenOptions, Error as HeedError, PutFlags, RoTxn, WithTls};
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::Arena;

use crate::{constants, timed_dbg, types::EventsDb};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Heed(#[from] HeedError),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Database not found: {0}")]
    DatabaseNotFound(String),
}

#[derive(Debug, Clone)]
pub struct VarveConfig {
    pub max_dbs: u32,
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

/// A single-open event store handle.
///
/// - **Writes** require `&mut self` (single-writer by construction; no locks).
/// - Use [`Varve::reader`] to get a cheap, cloneable reader view for concurrent reads on other threads.
pub struct Varve<const N: usize> {
    core: Arc<VarveCore>,
    next_sequence: u64,
    serializer_buffer: [u8; N],
}

struct VarveCore {
    env: Env,
    events_db: EventsDb,
}

/// Zero-allocation serializer for fixed-size types.
pub type LowSerializer<'a> =
    Strategy<rkyv::ser::Serializer<rkyv::ser::writer::Buffer<'a>, (), ()>, rkyv::rancor::Error>;

/// Allocating serializer for arbitrary types.
pub type HighSerializer<'a> = Strategy<
    rkyv::ser::Serializer<
        rkyv::ser::writer::Buffer<'a>,
        rkyv::ser::allocator::ArenaHandle<'a>,
        rkyv::ser::sharing::Share,
    >,
    rkyv::rancor::Error,
>;

impl<const N: usize> Varve<N> {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::with_config(path, VarveConfig::default())
    }

    pub fn with_config(path: impl AsRef<Path>, config: VarveConfig) -> Result<Self, Error> {
        let env = unsafe {
            EnvOpenOptions::new()
                .read_txn_with_tls()
                .max_dbs(config.max_dbs)
                .map_size(config.map_size)
                .open(path)?
        };

        let events_db: EventsDb = {
            let mut wtxn = env.write_txn()?;
            let db = env.create_database(&mut wtxn, Some(constants::EVENTS_DB_NAME))?;
            wtxn.commit()?;
            db
        };

        let next_sequence = {
            let rtxn = env.read_txn()?;
            let last = events_db.last(&rtxn)?;
            // NOTE: we intentionally abort the read txn on drop (cheaper than commit).
            match last {
                Some((last_key, _)) => last_key.saturating_add(1),
                None => 0,
            }
        };

        Ok(Self {
            core: Arc::new(VarveCore { env, events_db }),
            next_sequence,
            serializer_buffer: [0u8; N],
        })
    }

    /// Creates a cheap, cloneable reader view suitable for concurrent reads across threads.
    ///
    /// This does **not** open another LMDB environment (it reuses the same one).
    pub fn reader(&self) -> VarveReader {
        VarveReader {
            core: Arc::clone(&self.core),
            scratch: rkyv::util::AlignedVec::new(),
        }
    }

    // =========================================================================
    // Private serialization helpers
    // =========================================================================

    /// Serialize an event using the non-allocating (low) serializer.
    fn serialize_low<T>(&mut self, event: &T) -> Result<Vec<u8>, Error>
    where
        T: for<'a> rkyv::Serialize<LowSerializer<'a>>,
    {
        let writer = rkyv::ser::writer::Buffer::from(&mut self.serializer_buffer);
        let mut serializer = rkyv::ser::Serializer::new(writer, (), ());
        rkyv::api::serialize_using::<_, rkyv::rancor::Error>(event, &mut serializer)
            .map_err(|e| Error::Serialization(format!("{e:?}")))?;
        let pos = serializer.into_writer().len();
        Ok(self.serializer_buffer[..pos].to_vec())
    }

    /// Serialize an event using the allocating (high) serializer.
    fn serialize_high<T>(&mut self, event: &T) -> Result<Vec<u8>, Error>
    where
        T: for<'a> rkyv::Serialize<HighSerializer<'a>>,
    {
        let mut arena = Arena::new();
        let writer = rkyv::ser::writer::Buffer::from(&mut self.serializer_buffer);
        let sharing = rkyv::ser::sharing::Share::new();
        let mut serializer = rkyv::ser::Serializer::new(writer, arena.acquire(), sharing);
        rkyv::api::serialize_using::<_, rkyv::rancor::Error>(event, &mut serializer)
            .map_err(|e| Error::Serialization(format!("{e:?}")))?;
        let pos = serializer.into_writer().len();
        Ok(self.serializer_buffer[..pos].to_vec())
    }

    // =========================================================================
    // Private storage helpers
    // =========================================================================

    /// Store a single serialized event and commit immediately.
    fn store_single(&mut self, bytes: &[u8]) -> Result<u64, Error> {
        let seq = self.next_sequence;
        let mut wtxn = self.core.env.write_txn()?;

        timed_dbg!("put", {
            self.core
                .events_db
                .put_with_flags(&mut wtxn, PutFlags::NO_OVERWRITE, &seq, bytes)
        })?;

        timed_dbg!("commit", wtxn.commit())?;

        self.next_sequence = seq + 1;
        Ok(seq)
    }

    /// Store multiple serialized events in a single transaction.
    fn store_batch(&mut self, serialized_events: Vec<Vec<u8>>) -> Result<Vec<u64>, Error> {
        let count = serialized_events.len();
        let mut sequences = Vec::with_capacity(count);

        let mut wtxn = self.core.env.write_txn()?;

        timed_dbg!(format!("batch_put({count})"), {
            for bytes in serialized_events {
                let seq = self.next_sequence;
                self.core.events_db.put_with_flags(
                    &mut wtxn,
                    PutFlags::NO_OVERWRITE,
                    &seq,
                    &bytes,
                )?;
                sequences.push(seq);
                self.next_sequence = seq + 1;
            }
            Ok::<_, Error>(())
        })?;

        timed_dbg!("batch_commit", wtxn.commit())?;

        Ok(sequences)
    }

    // =========================================================================
    // Public append API
    // =========================================================================

    /// Append an event using a non-allocating serializer (best for POD / fixed-size types).
    ///
    /// Returns the assigned sequence number.
    pub fn append<T>(&mut self, event: &T) -> Result<u64, Error>
    where
        T: for<'a> rkyv::Serialize<LowSerializer<'a>>,
    {
        let bytes = timed_dbg!("serialize", self.serialize_low(event))?;
        self.store_single(&bytes)
    }

    /// Append an event using an allocating serializer (supports Strings, Vecs, etc).
    ///
    /// Returns the assigned sequence number.
    pub fn append_alloc<T>(&mut self, event: &T) -> Result<u64, Error>
    where
        T: for<'a> rkyv::Serialize<HighSerializer<'a>>,
    {
        let bytes = timed_dbg!("serialize", self.serialize_high(event))?;
        self.store_single(&bytes)
    }

    /// Append a batch of events using a non-allocating serializer in a single transaction.
    ///
    /// This is more efficient than calling [`append`](Self::append) multiple times, as it reduces
    /// transaction overhead by batching all writes into a single commit.
    ///
    /// Returns the sequence numbers assigned to each event, in order.
    pub fn append_batch<T>(&mut self, events: &[T]) -> Result<Vec<u64>, Error>
    where
        T: for<'a> rkyv::Serialize<LowSerializer<'a>>,
    {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let event_count = events.len();

        let serialized = timed_dbg!(format!("batch_serialize({event_count})"), {
            let mut serialized = Vec::with_capacity(event_count);
            for event in events {
                serialized.push(self.serialize_low(event)?);
            }
            Ok::<_, Error>(serialized)
        })?;

        timed_dbg!(
            format!("batch_total({event_count})"),
            self.store_batch(serialized)
        )
    }

    /// Append a batch of events using an allocating serializer in a single transaction.
    ///
    /// This is more efficient than calling [`append_alloc`](Self::append_alloc) multiple times,
    /// as it reduces transaction overhead by batching all writes into a single commit.
    /// Supports Strings, Vecs, etc.
    ///
    /// Returns the sequence numbers assigned to each event, in order.
    pub fn append_batch_alloc<T>(&mut self, events: &[T]) -> Result<Vec<u64>, Error>
    where
        T: for<'a> rkyv::Serialize<HighSerializer<'a>>,
    {
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let event_count = events.len();

        let serialized = timed_dbg!(format!("batch_serialize({event_count})"), {
            let mut serialized = Vec::with_capacity(event_count);
            for event in events {
                serialized.push(self.serialize_high(event)?);
            }
            Ok::<_, Error>(serialized)
        })?;

        timed_dbg!(
            format!("batch_total({event_count})"),
            self.store_batch(serialized)
        )
    }

    /// Read an event by sequence using the writer-thread handle (zero-copy bytes).
    pub fn get<'a>(&'a self, sequence: u64) -> Result<VarveGetResult<'a>, Error> {
        let rtxn = self.core.env.read_txn()?;
        let result = VarveGetResultTryBuilder {
            guard: rtxn,
            data_builder: |guard| self.core.events_db.get(guard, &sequence),
        };
        Ok(result.try_build()?)
    }
}

/// A cheap, cloneable reader view that can be sent to other threads.
///
/// Internally this is just another handle to the same LMDB environment; it does not reopen the env.
pub struct VarveReader {
    core: Arc<VarveCore>,
    /// Scratch buffer used to materialize stable bytes for encrypted environments.
    ///
    /// heed3 encrypted environments decrypt into a cycling buffer, so borrowed bytes are not
    /// stable under concurrent reads.
    scratch: rkyv::util::AlignedVec<16>,
}

#[ouroboros::self_referencing]
pub struct VarveGetResult<'a> {
    pub guard: RoTxn<'a, WithTls>,
    #[borrows(mut guard)]
    #[covariant]
    pub data: Option<&'this [u8]>,
}

impl VarveReader {
    /// Read an event by sequence (zero-copy bytes).
    ///
    /// This opens a short-lived read transaction and borrows bytes from it.
    pub fn get<'a>(&'a self, sequence: u64) -> Result<VarveGetResult<'a>, Error> {
        let result = VarveGetResultTryBuilder {
            guard: self.core.env.read_txn()?,
            data_builder: |guard| self.core.events_db.get(guard, &sequence),
        };
        Ok(result.try_build()?)
    }

    /// Read an event by sequence into an internal aligned scratch buffer and return stable bytes.
    ///
    /// This is the recommended API for **concurrent readers** with encrypted LMDB, because
    /// borrowed slices may be invalidated by other reads due to LMDB's decrypt cache design.
    pub fn get_bytes(&mut self, sequence: u64) -> Result<Option<&[u8]>, Error> {
        let rtxn = self.core.env.read_txn()?;
        let bytes = self.core.events_db.get(&rtxn, &sequence)?;
        match bytes {
            Some(b) => {
                self.scratch.clear();
                self.scratch.extend_from_slice(b);
                Ok(Some(&self.scratch))
            }
            None => Ok(None),
        }
    }

    /// Read an event into the internal scratch buffer and return an archived view.
    ///
    /// This validates the archived data using `bytecheck` (slower, but safe).
    pub fn get_archived<T>(&mut self, sequence: u64) -> Result<Option<&rkyv::Archived<T>>, Error>
    where
        T: rkyv::Archive,
        rkyv::Archived<T>: rkyv::Portable
            + for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            >,
    {
        let Some(bytes) = self.get_bytes(sequence)? else {
            return Ok(None);
        };

        let archived = rkyv::access::<rkyv::Archived<T>, rkyv::rancor::Error>(bytes)
            .map_err(|e| Error::Serialization(format!("{:?}", e)))?;
        Ok(Some(archived))
    }

    /// Read an event into the internal scratch buffer and return an archived view **without** validation.
    ///
    /// # Safety
    /// The bytes stored for `sequence` must be a valid archived `T` at rkyv's root position.
    /// This should only be used when the data is trusted (e.g. written by this same schema).
    pub unsafe fn get_archived_unchecked<T>(
        &mut self,
        sequence: u64,
    ) -> Result<Option<&rkyv::Archived<T>>, Error>
    where
        T: rkyv::Archive,
        rkyv::Archived<T>: rkyv::Portable,
    {
        let Some(bytes) = self.get_bytes(sequence)? else {
            return Ok(None);
        };
        // SAFETY: caller guarantees the bytes are a valid archived `T`.
        Ok(Some(unsafe {
            rkyv::access_unchecked::<rkyv::Archived<T>>(bytes)
        }))
    }
}

impl Clone for VarveReader {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            scratch: rkyv::util::AlignedVec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::{Archive, Deserialize, Serialize};
    use tempfile::tempdir;

    // ============================================
    // Event type definitions (ported from old integration tests)
    // ============================================

    pub mod events {
        use super::*;

        pub mod payment {
            use super::*;

            #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
            #[rkyv(attr(derive(Debug)))]
            #[non_exhaustive]
            pub enum Payment {
                Created(created::Created),
                Refunded(refunded::Refunded),
            }

            pub mod created {
                use super::*;

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub enum Created {
                    V1(V1),
                }

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub struct V1 {
                    pub payment_id: String,
                    pub amount: u64,
                    pub currency: String,
                    pub customer_id: String,
                }
            }

            pub mod refunded {
                use super::*;

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub enum Refunded {
                    V1(V1),
                }

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub struct V1 {
                    pub payment_id: String,
                    pub refund_amount: u64,
                    pub reason: String,
                }
            }
        }

        pub mod user {
            use super::*;

            #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
            #[rkyv(attr(derive(Debug)))]
            #[non_exhaustive]
            pub enum User {
                Registered(registered::Registered),
                ProfileUpdated(profile_updated::ProfileUpdated),
            }

            pub mod registered {
                use super::*;

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub enum Registered {
                    V1(V1),
                }

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub struct V1 {
                    pub user_id: String,
                    pub email: String,
                    pub name: String,
                    pub tags: Vec<String>,
                }
            }

            pub mod profile_updated {
                use super::*;

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub enum ProfileUpdated {
                    V1(V1),
                }

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub struct V1 {
                    pub user_id: String,
                    pub field: String,
                    pub old_value: String,
                    pub new_value: String,
                }
            }
        }

        pub mod order {
            use super::*;

            #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
            #[rkyv(attr(derive(Debug)))]
            #[non_exhaustive]
            pub enum Order {
                Placed(placed::Placed),
                Shipped(shipped::Shipped),
                Delivered(delivered::Delivered),
            }

            pub mod placed {
                use super::*;

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub enum Placed {
                    V1(V1),
                }

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub struct V1 {
                    pub order_id: String,
                    pub customer_id: String,
                    pub items: Vec<OrderItem>,
                    pub total: u64,
                }

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                pub struct OrderItem {
                    pub product_id: String,
                    pub quantity: u32,
                    pub price: u64,
                }
            }

            pub mod shipped {
                use super::*;

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub enum Shipped {
                    V1(V1),
                }

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub struct V1 {
                    pub order_id: String,
                    pub tracking_number: String,
                    pub carrier: String,
                }
            }

            pub mod delivered {
                use super::*;

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub enum Delivered {
                    V1(V1),
                }

                #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub struct V1 {
                    pub order_id: String,
                    pub signature: String,
                    pub delivered_at: u64,
                }
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    #[rkyv(attr(derive(Debug)))]
    #[non_exhaustive]
    pub enum DomainEvent {
        Payment(events::payment::Payment),
        User(events::user::User),
        Order(events::order::Order),
    }

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    #[rkyv(attr(derive(Debug)))]
    pub struct SimpleEvent {
        pub id: u64,
        pub timestamp: u64,
        pub value: i32,
    }

    // Helper: stable archived access via VarveReader scratch buffer
    fn get_archived_simple(
        reader: &mut VarveReader,
        sequence: u64,
    ) -> &rkyv::Archived<SimpleEvent> {
        // SAFETY: We just wrote the bytes with this same schema in the test.
        unsafe { reader.get_archived_unchecked::<SimpleEvent>(sequence) }
            .expect("get_archived_unchecked failed")
            .expect("missing event")
    }

    fn get_archived_domain(
        reader: &mut VarveReader,
        sequence: u64,
    ) -> &rkyv::Archived<DomainEvent> {
        // Use checked access for complex types (strings/vecs)
        reader
            .get_archived::<DomainEvent>(sequence)
            .expect("get_archived failed")
            .expect("missing event")
    }

    // ============================================
    // Unit Tests
    // ============================================

    #[test]
    fn test_append_and_read_simple_event() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut store = Varve::<1024>::new(dir.path()).expect("Failed to create Varve");

        let event = SimpleEvent {
            id: 1,
            timestamp: 1702400000,
            value: 42,
        };

        let seq = store.append(&event).expect("append failed");
        assert_eq!(seq, 0);

        let mut reader = store.reader();
        let archived = get_archived_simple(&mut reader, 0);
        assert_eq!(archived.id, 1);
        assert_eq!(archived.timestamp, 1702400000);
        assert_eq!(archived.value, 42);
    }

    #[test]
    fn test_append_and_read_multiple_simple_events() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut store = Varve::<1024>::new(dir.path()).expect("Failed to create Varve");

        for i in 0..100u64 {
            let event = SimpleEvent {
                id: i,
                timestamp: 1702400000 + i,
                value: (i * 10) as i32,
            };
            let seq = store.append(&event).expect("append failed");
            assert_eq!(seq, i);
        }

        let mut reader = store.reader();
        for i in 0..100u64 {
            let archived = get_archived_simple(&mut reader, i);
            assert_eq!(archived.id, i);
            assert_eq!(archived.timestamp, 1702400000 + i);
            assert_eq!(archived.value, (i * 10) as i32);
        }
    }

    #[test]
    fn test_append_alloc_and_read_payment_event() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut store = Varve::<4096>::new(dir.path()).expect("Failed to create Varve");

        let event = DomainEvent::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                payment_id: "pay_123abc".to_string(),
                amount: 9999,
                currency: "USD".to_string(),
                customer_id: "cust_456def".to_string(),
            }),
        ));

        store.append_alloc(&event).expect("append_alloc failed");

        let mut reader = store.reader();
        let archived = get_archived_domain(&mut reader, 0);

        match archived {
            ArchivedDomainEvent::Payment(payment) => match payment {
                events::payment::ArchivedPayment::Created(created) => match created {
                    events::payment::created::ArchivedCreated::V1(v1) => {
                        assert_eq!(v1.payment_id.as_str(), "pay_123abc");
                        assert_eq!(v1.amount, 9999);
                        assert_eq!(v1.currency.as_str(), "USD");
                        assert_eq!(v1.customer_id.as_str(), "cust_456def");
                    }
                },
                _ => panic!("Expected Created payment"),
            },
            _ => panic!("Expected Payment event"),
        }
    }

    #[test]
    fn test_append_alloc_and_read_user_event_with_tags() {
        let dir = tempdir().expect("Failed to create temp dir");

        let mut store = Varve::<4096>::new(dir.path()).expect("Failed to create Varve");

        let tags = vec![
            "premium".to_string(),
            "verified".to_string(),
            "early-adopter".to_string(),
        ];

        let event = DomainEvent::User(events::user::User::Registered(
            events::user::registered::Registered::V1(events::user::registered::V1 {
                user_id: "usr_789ghi".to_string(),
                email: "john.doe@example.com".to_string(),
                name: "John Doe".to_string(),
                tags: tags.clone(),
            }),
        ));

        store.append_alloc(&event).expect("append_alloc failed");

        let mut reader = store.reader();
        let archived = get_archived_domain(&mut reader, 0);

        match archived {
            ArchivedDomainEvent::User(user) => match user {
                events::user::ArchivedUser::Registered(registered) => match registered {
                    events::user::registered::ArchivedRegistered::V1(v1) => {
                        assert_eq!(v1.user_id.as_str(), "usr_789ghi");
                        assert_eq!(v1.email.as_str(), "john.doe@example.com");
                        assert_eq!(v1.name.as_str(), "John Doe");
                        assert_eq!(v1.tags.len(), 3);
                        assert_eq!(v1.tags[0].as_str(), "premium");
                        assert_eq!(v1.tags[1].as_str(), "verified");
                        assert_eq!(v1.tags[2].as_str(), "early-adopter");
                    }
                },
                _ => panic!("Expected Registered user"),
            },
            _ => panic!("Expected User event"),
        }
    }

    #[test]
    fn test_append_alloc_and_read_order_with_nested_items() {
        let dir = tempdir().expect("Failed to create temp dir");

        let mut store = Varve::<8192>::new(dir.path()).expect("Failed to create Varve");

        let items = vec![
            events::order::placed::OrderItem {
                product_id: "prod_001".to_string(),
                quantity: 2,
                price: 1999,
            },
            events::order::placed::OrderItem {
                product_id: "prod_002".to_string(),
                quantity: 1,
                price: 4999,
            },
            events::order::placed::OrderItem {
                product_id: "prod_003".to_string(),
                quantity: 5,
                price: 299,
            },
        ];

        let event = DomainEvent::Order(events::order::Order::Placed(
            events::order::placed::Placed::V1(events::order::placed::V1 {
                order_id: "ord_abc123".to_string(),
                customer_id: "cust_xyz789".to_string(),
                items,
                total: 2 * 1999 + 4999 + 5 * 299,
            }),
        ));

        store.append_alloc(&event).expect("append_alloc failed");

        let mut reader = store.reader();
        let archived = get_archived_domain(&mut reader, 0);

        match archived {
            ArchivedDomainEvent::Order(order) => match order {
                events::order::ArchivedOrder::Placed(placed) => match placed {
                    events::order::placed::ArchivedPlaced::V1(v1) => {
                        assert_eq!(v1.order_id.as_str(), "ord_abc123");
                        assert_eq!(v1.customer_id.as_str(), "cust_xyz789");
                        assert_eq!(v1.items.len(), 3);
                        assert_eq!(v1.items[0].product_id.as_str(), "prod_001");
                        assert_eq!(v1.items[0].quantity, 2);
                        assert_eq!(v1.items[0].price, 1999);
                        assert_eq!(v1.items[1].product_id.as_str(), "prod_002");
                        assert_eq!(v1.items[2].product_id.as_str(), "prod_003");
                        assert_eq!(v1.total, 2 * 1999 + 4999 + 5 * 299);
                    }
                },
                _ => panic!("Expected Placed order"),
            },
            _ => panic!("Expected Order event"),
        }
    }

    #[test]
    fn test_get_nonexistent_sequence_returns_none() {
        let dir = tempdir().expect("Failed to create temp dir");

        let store = Varve::<1024>::new(dir.path()).expect("Failed to create Varve");
        let mut reader = store.reader();

        assert!(reader.get_bytes(0).expect("get_bytes failed").is_none());
    }

    #[test]
    fn test_append_sequence_is_persistent_across_reopen() {
        let dir = tempdir().expect("Failed to create temp dir");

        {
            let mut store = Varve::<1024>::new(dir.path()).expect("Failed to create Varve");
            let e0 = SimpleEvent {
                id: 0,
                timestamp: 0,
                value: 0,
            };
            let e1 = SimpleEvent {
                id: 1,
                timestamp: 1,
                value: 2,
            };
            assert_eq!(store.append(&e0).unwrap(), 0);
            assert_eq!(store.append(&e1).unwrap(), 1);
        }

        // Reopen and ensure the next sequence continues.
        let mut store = Varve::<1024>::new(dir.path()).expect("Failed to reopen Varve");
        let e2 = SimpleEvent {
            id: 2,
            timestamp: 2,
            value: 4,
        };
        assert_eq!(store.append(&e2).unwrap(), 2);

        let mut reader = store.reader();
        let archived = get_archived_simple(&mut reader, 2);
        assert_eq!(archived.id, 2);
    }

    #[test]
    fn test_append_alloc_buffer_too_small_should_fail() {
        let dir = tempdir().expect("Failed to create temp dir");

        let mut store = Varve::<64>::new(dir.path()).expect("Failed to create Varve");

        let event = DomainEvent::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                payment_id: "this_is_a_fairly_long_payment_id_that_wont_fit".to_string(),
                amount: 999999,
                currency: "VERYLONGCURRENCYNAME".to_string(),
                customer_id: "customer_1234567890".to_string(),
            }),
        ));

        assert!(store.append_alloc(&event).is_err());
    }

    #[test]
    fn test_varve_concurrent_reader_and_writer() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::thread;
        use std::time::{Duration, Instant};

        let dir = tempdir().expect("Failed to create temp dir");

        let mut store = Varve::<{ std::mem::size_of::<SimpleEvent>() }>::new(dir.path())
            .inspect_err(|e| eprintln!("Error creating Varve: {e:?}"))
            .expect("Failed to create Varve");

        let r1 = store.reader();
        let r2 = r1.clone();
        let r3 = r1.clone();

        const TOTAL: u64 = 200;
        let written = AtomicU64::new(0);
        let start = Instant::now();
        let timeout = Duration::from_secs(5);

        timed_dbg!("append_batch", {
            thread::scope(|s| {
                let written = &written;

                s.spawn(move || {
                    let events = (0..TOTAL)
                        .map(|i| SimpleEvent {
                            id: i,
                            timestamp: 1000 + i,
                            value: (i as i32) * 2,
                        })
                        .collect::<Vec<_>>();
                    store
                        .append_batch(&events)
                        .inspect_err(|e| eprintln!("Error appending event: {e:?}"))
                        .expect("append failed");
                    written.store(TOTAL, Ordering::Release);
                });

                let reader_task = |mut reader: VarveReader| {
                    let mut next = 0u64;
                    while next < TOTAL && start.elapsed() < timeout {
                        let max_written = written.load(Ordering::Acquire);
                        while next < max_written {
                            let bytes = reader
                                .get_bytes(next)
                                .inspect_err(|e| eprintln!("Error getting bytes: {e:?}"))
                                .expect("get_bytes failed")
                                .or_else(|| {
                                    eprintln!("Missing data for sequence {next}");
                                    None
                                })
                                .expect("missing data");
                            let archived = rkyv::access::<
                                rkyv::Archived<SimpleEvent>,
                                rkyv::rancor::Error,
                            >(bytes)
                            .inspect_err(|e| eprintln!("Error accessing archived: {e:?}"))
                            .expect("access failed");
                            assert_eq!(archived.id, next);
                            assert_eq!(archived.value, (next as i32) * 2);
                            next += 1;
                        }
                        thread::yield_now();
                    }
                    assert_eq!(next, TOTAL, "reader did not observe all events in time");
                };

                s.spawn(move || reader_task(r1));
                s.spawn(move || reader_task(r2));
                s.spawn(move || reader_task(r3));
            });
        });
    }

    #[test]
    fn test_append_batch_simple_events() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut store = Varve::<1024>::new(dir.path()).expect("Failed to create Varve");

        let events = vec![
            SimpleEvent {
                id: 0,
                timestamp: 1702400000,
                value: 10,
            },
            SimpleEvent {
                id: 1,
                timestamp: 1702400001,
                value: 20,
            },
            SimpleEvent {
                id: 2,
                timestamp: 1702400002,
                value: 30,
            },
        ];

        let sequences = store.append_batch(&events).expect("append_batch failed");
        assert_eq!(sequences, vec![0, 1, 2]);

        let mut reader = store.reader();
        for (i, seq) in sequences.iter().enumerate() {
            let archived = get_archived_simple(&mut reader, *seq);
            assert_eq!(archived.id, i as u64);
            assert_eq!(archived.value, (i as i32 + 1) * 10);
        }
    }

    #[test]
    fn test_append_batch_alloc_events() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut store = Varve::<4096>::new(dir.path()).expect("Failed to create Varve");

        let events = vec![
            DomainEvent::Payment(events::payment::Payment::Created(
                events::payment::created::Created::V1(events::payment::created::V1 {
                    payment_id: "pay_001".to_string(),
                    amount: 1000,
                    currency: "USD".to_string(),
                    customer_id: "cust_001".to_string(),
                }),
            )),
            DomainEvent::Payment(events::payment::Payment::Created(
                events::payment::created::Created::V1(events::payment::created::V1 {
                    payment_id: "pay_002".to_string(),
                    amount: 2000,
                    currency: "EUR".to_string(),
                    customer_id: "cust_002".to_string(),
                }),
            )),
            DomainEvent::Payment(events::payment::Payment::Created(
                events::payment::created::Created::V1(events::payment::created::V1 {
                    payment_id: "pay_003".to_string(),
                    amount: 3000,
                    currency: "GBP".to_string(),
                    customer_id: "cust_003".to_string(),
                }),
            )),
        ];

        let sequences = store
            .append_batch_alloc(&events)
            .expect("append_batch_alloc failed");
        assert_eq!(sequences, vec![0, 1, 2]);

        let mut reader = store.reader();
        let ids = vec!["pay_001", "pay_002", "pay_003"];
        let amounts = vec![1000, 2000, 3000];
        let currencies = vec!["USD", "EUR", "GBP"];
        let customers = vec!["cust_001", "cust_002", "cust_003"];

        for (i, seq) in sequences.iter().enumerate() {
            let archived = get_archived_domain(&mut reader, *seq);
            match archived {
                ArchivedDomainEvent::Payment(payment) => match payment {
                    events::payment::ArchivedPayment::Created(created) => match created {
                        events::payment::created::ArchivedCreated::V1(v1) => {
                            assert_eq!(v1.payment_id.as_str(), ids[i]);
                            assert_eq!(v1.amount, amounts[i]);
                            assert_eq!(v1.currency.as_str(), currencies[i]);
                            assert_eq!(v1.customer_id.as_str(), customers[i]);
                        }
                    },
                    _ => panic!("Expected Created payment"),
                },
                _ => panic!("Expected Payment event"),
            }
        }
    }

    #[test]
    fn test_append_batch_empty_slice() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut store = Varve::<1024>::new(dir.path()).expect("Failed to create Varve");

        let events: Vec<SimpleEvent> = vec![];
        let sequences = store.append_batch(&events).expect("append_batch failed");
        assert_eq!(sequences, vec![] as Vec<u64>);
    }

    #[test]
    fn test_append_batch_alloc_empty_slice() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut store = Varve::<1024>::new(dir.path()).expect("Failed to create Varve");

        let events: Vec<DomainEvent> = vec![];
        let sequences = store
            .append_batch_alloc(&events)
            .expect("append_batch_alloc failed");
        assert_eq!(sequences, vec![] as Vec<u64>);
    }

    #[test]
    fn test_append_batch_sequence_continuation() {
        let dir = tempdir().expect("Failed to create temp dir");
        let mut store = Varve::<1024>::new(dir.path()).expect("Failed to create Varve");

        // Add a single event first
        let single_event = SimpleEvent {
            id: 0,
            timestamp: 1702400000,
            value: 10,
        };
        let seq1 = store.append(&single_event).expect("append failed");
        assert_eq!(seq1, 0);

        // Now append a batch
        let batch_events = vec![
            SimpleEvent {
                id: 1,
                timestamp: 1702400001,
                value: 20,
            },
            SimpleEvent {
                id: 2,
                timestamp: 1702400002,
                value: 30,
            },
        ];
        let sequences = store
            .append_batch(&batch_events)
            .expect("append_batch failed");
        assert_eq!(sequences, vec![1, 2]);

        let mut reader = store.reader();
        let archived = get_archived_simple(&mut reader, 0);
        assert_eq!(archived.id, 0);
        let archived = get_archived_simple(&mut reader, 1);
        assert_eq!(archived.id, 1);
        let archived = get_archived_simple(&mut reader, 2);
        assert_eq!(archived.id, 2);
    }
}
