// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use std::path::Path;

use chacha20poly1305::aead::AeadMutInPlace;
use chacha20poly1305::aead::Key;
use chacha20poly1305::KeyInit;
use heed3::EncryptedEnv;
use heed3::EnvOpenOptions;
use heed3::Error as HeedError;
use heed3::PutFlags;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::Arena;

use crate::constants;
use crate::types::EventsDb;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Heed(#[from] HeedError),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub max_dbs: u32,
    pub map_size: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            max_dbs: constants::DEFAULT_MAX_DBS,
            map_size: constants::DEFAULT_MAP_SIZE,
        }
    }
}

pub struct Writer<const N: usize> {
    env: EncryptedEnv,
    events_db: EventsDb,
    sequence: u64,
    serializer_buffer: [u8; N],
}

impl<const N: usize> Writer<N> {
    pub fn new<E: AeadMutInPlace + KeyInit>(
        key: Key<E>,
        path: impl AsRef<Path>,
    ) -> Result<Self, Error> {
        Self::with_config::<E>(key, path, WriterConfig::default())
    }

    pub fn with_config<E: AeadMutInPlace + KeyInit>(
        key: Key<E>,
        path: impl AsRef<Path>,
        config: WriterConfig,
    ) -> Result<Self, Error> {
        let env = unsafe {
            EnvOpenOptions::new()
                .read_txn_with_tls()
                .max_dbs(config.max_dbs)
                .map_size(config.map_size)
                .open_encrypted::<E, _>(key, path)?
        };

        let events_db: EventsDb = {
            let mut wtxn = env.write_txn()?;
            let db = env.create_database(&mut wtxn, Some(constants::EVENTS_DB_NAME))?;
            wtxn.commit()?;
            db
        };

        let sequence = 0;
        let serializer_buffer = [0u8; N];
        Ok(Self {
            env,
            events_db,
            sequence,
            serializer_buffer,
        })
    }
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

impl<const N: usize> Writer<N> {
    pub fn append<T>(&mut self, event: &T) -> Result<(), Error>
    where
        T: for<'a> rkyv::Serialize<LowSerializer<'a>>,
    {
        let writer = rkyv::ser::writer::Buffer::from(&mut self.serializer_buffer);
        let mut serializer = rkyv::ser::Serializer::new(writer, (), ());

        rkyv::api::serialize_using::<_, rkyv::rancor::Error>(event, &mut serializer)
            .map_err(|e| Error::Serialization(format!("{:?}", e)))?;

        let pos = serializer.into_writer().len();
        let serialized_bytes = &self.serializer_buffer[..pos];

        let mut wtxn = self.env.write_txn()?;
        self.events_db.put_with_flags(
            &mut wtxn,
            PutFlags::NO_OVERWRITE,
            &self.sequence,
            serialized_bytes,
        )?;
        self.sequence += 1;
        wtxn.commit()?;
        Ok(())
    }

    pub fn append_alloc<T>(&mut self, event: &T) -> Result<(), Error>
    where
        T: for<'a> rkyv::Serialize<HighSerializer<'a>>,
    {
        let mut arena = Arena::new();

        let writer = rkyv::ser::writer::Buffer::from(&mut self.serializer_buffer);
        let sharing = rkyv::ser::sharing::Share::new();
        let mut serializer = rkyv::ser::Serializer::new(writer, arena.acquire(), sharing);
        rkyv::api::serialize_using::<_, rkyv::rancor::Error>(event, &mut serializer)
            .map_err(|e| Error::Serialization(format!("{:?}", e)))?;

        let pos = serializer.into_writer().len();
        let serialized_bytes = &self.serializer_buffer[..pos];

        let mut wtxn = self.env.write_txn()?;
        self.events_db.put_with_flags(
            &mut wtxn,
            PutFlags::NO_OVERWRITE,
            &self.sequence,
            serialized_bytes,
        )?;
        self.sequence += 1;
        wtxn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chacha20poly1305::{aead::OsRng, ChaCha20Poly1305};
    use rkyv::{Archive, Deserialize, Serialize};
    use tempfile::tempdir;

    // ============================================
    // Event type definitions (nested structure)
    // ============================================

    pub mod events {
        use super::*;

        pub mod payment {
            use super::*;

            #[derive(Debug, Clone, Archive, Serialize, Deserialize)]
            #[rkyv(attr(derive(Debug)))]
            #[non_exhaustive]
            pub enum Payment {
                Created(created::Created),
            }

            pub mod created {
                use super::*;

                #[derive(Debug, Clone, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub enum Created {
                    V1(V1),
                }

                #[derive(Debug, Clone, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub struct V1 {
                    pub signature: String,
                    pub amount: u64,
                    pub currency: String,
                }
            }
        }

        pub mod user {
            use super::*;

            #[derive(Debug, Clone, Archive, Serialize, Deserialize)]
            #[rkyv(attr(derive(Debug)))]
            #[non_exhaustive]
            pub enum User {
                Registered(registered::Registered),
            }

            pub mod registered {
                use super::*;

                #[derive(Debug, Clone, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub enum Registered {
                    V1(V1),
                }

                #[derive(Debug, Clone, Archive, Serialize, Deserialize)]
                #[rkyv(attr(derive(Debug)))]
                #[non_exhaustive]
                pub struct V1 {
                    pub email: String,
                    pub name: String,
                    pub tags: Vec<String>,
                }
            }
        }
    }

    #[derive(Debug, Clone, Archive, Serialize, Deserialize)]
    #[rkyv(attr(derive(Debug)))]
    #[non_exhaustive]
    pub enum Events {
        Payment(events::payment::Payment),
        User(events::user::User),
    }

    // ============================================
    // Simple POD event (no strings/vecs)
    // ============================================

    #[derive(Debug, Clone, Archive, Serialize, Deserialize)]
    #[rkyv(attr(derive(Debug)))]
    pub struct SimpleEvent {
        pub id: u64,
        pub timestamp: u64,
        pub value: i32,
    }

    // ============================================
    // Helper to create a test writer
    // ============================================

    fn create_test_writer<const N: usize>() -> (Writer<N>, tempfile::TempDir) {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = ChaCha20Poly1305::generate_key(&mut OsRng);
        let writer =
            Writer::<N>::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create writer");
        (writer, dir)
    }

    // ============================================
    // Tests
    // ============================================

    #[test]
    fn test_append_simple_event() {
        let (mut writer, _dir) = create_test_writer::<1024>();

        let event = SimpleEvent {
            id: 1,
            timestamp: 1234567890,
            value: 42,
        };

        assert!(writer.append(&event).is_ok());
    }

    #[test]
    fn test_append_multiple_simple_events() {
        let (mut writer, _dir) = create_test_writer::<1024>();

        for i in 0..10 {
            let event = SimpleEvent {
                id: i,
                timestamp: 1234567890 + i,
                value: i as i32 * 10,
            };

            assert!(writer.append(&event).is_ok());
        }
    }

    #[test]
    fn test_append_alloc_nested_payment_event_with_strings() {
        let (mut writer, _dir) = create_test_writer::<4096>();

        let event = Events::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                signature: "sig_abc123xyz".to_string(),
                amount: 9999,
                currency: "USD".to_string(),
            }),
        ));

        assert!(writer.append_alloc(&event).is_ok());
    }

    #[test]
    fn test_append_alloc_nested_user_event_with_vec_of_strings() {
        let (mut writer, _dir) = create_test_writer::<4096>();

        let event = Events::User(events::user::User::Registered(
            events::user::registered::Registered::V1(events::user::registered::V1 {
                email: "user@example.com".to_string(),
                name: "John Doe".to_string(),
                tags: vec![
                    "premium".to_string(),
                    "early-adopter".to_string(),
                    "verified".to_string(),
                ],
            }),
        ));

        assert!(writer.append_alloc(&event).is_ok());
    }

    #[test]
    fn test_append_alloc_mixed_events() {
        let (mut writer, _dir) = create_test_writer::<4096>();

        let payment = Events::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                signature: "pay_001".to_string(),
                amount: 100,
                currency: "EUR".to_string(),
            }),
        ));
        assert!(writer.append_alloc(&payment).is_ok());

        let user = Events::User(events::user::User::Registered(
            events::user::registered::Registered::V1(events::user::registered::V1 {
                email: "alice@test.com".to_string(),
                name: "Alice".to_string(),
                tags: vec!["new".to_string()],
            }),
        ));
        assert!(writer.append_alloc(&user).is_ok());

        let payment2 = Events::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                signature: "pay_002".to_string(),
                amount: 250,
                currency: "GBP".to_string(),
            }),
        ));
        assert!(writer.append_alloc(&payment2).is_ok());
    }

    #[test]
    fn test_append_alloc_event_with_long_string() {
        let (mut writer, _dir) = create_test_writer::<8192>();

        let long_signature = "x".repeat(1000);

        let event = Events::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                signature: long_signature,
                amount: 1,
                currency: "BTC".to_string(),
            }),
        ));

        assert!(writer.append_alloc(&event).is_ok());
    }

    #[test]
    fn test_append_alloc_event_with_many_tags() {
        let (mut writer, _dir) = create_test_writer::<16384>();

        let many_tags: Vec<String> = (0..100).map(|i| format!("tag_{}", i)).collect();

        let event = Events::User(events::user::User::Registered(
            events::user::registered::Registered::V1(events::user::registered::V1 {
                email: "power_user@example.com".to_string(),
                name: "Power User".to_string(),
                tags: many_tags,
            }),
        ));

        assert!(writer.append_alloc(&event).is_ok());
    }

    #[test]
    fn test_append_alloc_buffer_too_small_should_fail() {
        let (mut writer, _dir) = create_test_writer::<32>();

        let event = Events::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                signature: "this_is_a_fairly_long_signature_that_wont_fit".to_string(),
                amount: 999999,
                currency: "VERYLONGCURRENCYNAME".to_string(),
            }),
        ));

        assert!(writer.append_alloc(&event).is_err());
    }
}
