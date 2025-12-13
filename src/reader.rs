// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use std::path::Path;

use chacha20poly1305::{
    aead::{AeadMutInPlace, Key},
    KeyInit,
};
use heed3::{EncryptedEnv, EnvFlags, EnvOpenOptions, RoTxn, WithoutTls};

use crate::{constants, types::EventsDb};

const DEFAULT_MAX_DBS: u32 = 1;
const DEFAULT_MAP_SIZE: usize = 10 * 1024 * 1024; // 10 MB

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Heed(#[from] heed3::Error),
    #[error("Database not found: {0}")]
    DatabaseNotFound(String),
}

pub struct ReaderConfig {
    pub max_dbs: u32,
    pub map_size: usize,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            max_dbs: DEFAULT_MAX_DBS,
            map_size: DEFAULT_MAP_SIZE,
        }
    }
}

pub struct Reader {
    env: EncryptedEnv<WithoutTls>,
    events_db: EventsDb,
}

impl Reader {
    pub fn new<E: AeadMutInPlace + KeyInit>(
        key: Key<E>,
        path: impl AsRef<Path>,
    ) -> Result<Self, Error> {
        Self::with_config::<E>(key, path, ReaderConfig::default())
    }

    pub fn with_config<E: AeadMutInPlace + KeyInit>(
        key: Key<E>,
        path: impl AsRef<Path>,
        config: ReaderConfig,
    ) -> Result<Self, Error> {
        let env = unsafe {
            EnvOpenOptions::new()
                .read_txn_without_tls()
                .flags(EnvFlags::READ_ONLY)
                .max_dbs(config.max_dbs)
                .map_size(config.map_size)
                .open_encrypted::<E, _>(key, path)?
        };

        let events_db: EventsDb = {
            let rtxn = env.read_txn()?;
            let db = env
                .open_database(&rtxn, Some(constants::EVENTS_DB_NAME))?
                .ok_or(Error::DatabaseNotFound(
                    constants::EVENTS_DB_NAME.to_string(),
                ))?;
            rtxn.commit()?;
            db
        };

        Ok(Self { env, events_db })
    }
}

#[ouroboros::self_referencing]
pub struct GetResult<'a> {
    pub guard: RoTxn<'a, WithoutTls>,
    #[borrows(mut guard)]
    #[covariant]
    pub data: Option<&'this [u8]>,
}

impl Reader {
    pub fn get<'a>(&'a self, sequence: u64) -> Result<GetResult<'a>, Error> {
        let rtxn = self.env.read_txn()?;
        let result = GetResultTryBuilder {
            guard: rtxn,
            data_builder: |guard: &mut RoTxn<'a, WithoutTls>| self.events_db.get(guard, &sequence),
        };
        Ok(result.try_build()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chacha20poly1305::{aead::OsRng, ChaCha20Poly1305};
    use rkyv::{Archive, Deserialize, Serialize};
    use tempfile::tempdir;

    use crate::writer::Writer;

    // ============================================
    // Event type definitions (matching writer tests)
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
                    pub signature: String,
                    pub amount: u64,
                    pub currency: String,
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
                    pub email: String,
                    pub name: String,
                    pub tags: Vec<String>,
                }
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    #[rkyv(attr(derive(Debug)))]
    #[non_exhaustive]
    pub enum Events {
        Payment(events::payment::Payment),
        User(events::user::User),
    }

    // ============================================
    // Simple POD event (no strings/vecs)
    // ============================================

    #[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
    #[rkyv(attr(derive(Debug)))]
    pub struct SimpleEvent {
        pub id: u64,
        pub timestamp: u64,
        pub value: i32,
    }

    // ============================================
    // Helpers
    // ============================================

    fn generate_key() -> Key<ChaCha20Poly1305> {
        ChaCha20Poly1305::generate_key(&mut OsRng)
    }

    fn create_writer<const N: usize>(
        key: Key<ChaCha20Poly1305>,
        dir: &tempfile::TempDir,
    ) -> Writer<N> {
        Writer::<N>::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create writer")
    }

    fn create_reader(key: Key<ChaCha20Poly1305>, dir: &tempfile::TempDir) -> Reader {
        Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader")
    }

    // ============================================
    // Unit Tests
    // ============================================

    #[test]
    fn test_reader_new_opens_existing_database() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        // Create writer first to initialize the database
        let _writer = create_writer::<1024>(key, &dir);
        drop(_writer);

        // Reader should be able to open the existing database
        let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path());
        assert!(reader.is_ok());
    }

    #[test]
    fn test_reader_with_config_opens_existing_database() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        // Create writer first
        let _writer = create_writer::<1024>(key, &dir);
        drop(_writer);

        let config = ReaderConfig {
            max_dbs: 2,
            map_size: 20 * 1024 * 1024,
        };

        let reader = Reader::with_config::<ChaCha20Poly1305>(key, dir.path(), config);
        assert!(reader.is_ok());
    }

    #[test]
    fn test_reader_fails_on_nonexistent_database() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        // Try to open reader without creating writer first
        let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path());
        assert!(reader.is_err());
    }

    #[test]
    fn test_get_simple_event() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        let event = SimpleEvent {
            id: 42,
            timestamp: 1234567890,
            value: 100,
        };

        // Write the event
        {
            let mut writer = create_writer::<1024>(key, &dir);
            writer.append(&event).expect("Failed to append event");
        }

        // Read the event
        let reader = create_reader(key, &dir);
        let result = reader.get(0).expect("Failed to get event");
        let data = result.borrow_data();

        assert!(data.is_some());
        let bytes = data.unwrap();

        // Deserialize and verify
        let archived = rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access archived data");

        assert_eq!(archived.id, event.id);
        assert_eq!(archived.timestamp, event.timestamp);
        assert_eq!(archived.value, event.value);
    }

    #[test]
    fn test_get_multiple_simple_events() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        let events: Vec<SimpleEvent> = (0..10)
            .map(|i| SimpleEvent {
                id: i,
                timestamp: 1000000 + i,
                value: (i * 10) as i32,
            })
            .collect();

        // Write all events
        {
            let mut writer = create_writer::<1024>(key, &dir);
            for event in &events {
                writer.append(event).expect("Failed to append event");
            }
        }

        // Read and verify each event
        let reader = create_reader(key, &dir);
        for (seq, expected) in events.iter().enumerate() {
            let result = reader.get(seq as u64).expect("Failed to get event");
            let data = result.borrow_data();
            assert!(data.is_some(), "Event at sequence {} should exist", seq);

            let bytes = data.unwrap();
            let archived = rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(bytes)
                .expect("Failed to access archived data");

            assert_eq!(archived.id, expected.id);
            assert_eq!(archived.timestamp, expected.timestamp);
            assert_eq!(archived.value, expected.value);
        }
    }

    #[test]
    fn test_get_nonexistent_sequence_returns_none() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        // Create writer to initialize db, but don't write anything
        {
            let _writer = create_writer::<1024>(key, &dir);
        }

        let reader = create_reader(key, &dir);
        let result = reader.get(0).expect("Failed to get");
        assert!(result.borrow_data().is_none());
    }

    #[test]
    fn test_get_out_of_range_sequence_returns_none() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        // Write one event
        {
            let mut writer = create_writer::<1024>(key, &dir);
            let event = SimpleEvent {
                id: 1,
                timestamp: 1,
                value: 1,
            };
            writer.append(&event).expect("Failed to append");
        }

        let reader = create_reader(key, &dir);

        // Sequence 0 should exist
        let result0 = reader.get(0).expect("Failed to get");
        assert!(result0.borrow_data().is_some());

        // Sequence 1 should not exist
        let result1 = reader.get(1).expect("Failed to get");
        assert!(result1.borrow_data().is_none());

        // High sequence should not exist
        let result_high = reader.get(999999).expect("Failed to get");
        assert!(result_high.borrow_data().is_none());
    }

    #[test]
    fn test_get_nested_payment_event_with_strings() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        let event = Events::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                signature: "sig_abc123xyz".to_string(),
                amount: 9999,
                currency: "USD".to_string(),
            }),
        ));

        // Write the event
        {
            let mut writer = create_writer::<4096>(key, &dir);
            writer.append_alloc(&event).expect("Failed to append event");
        }

        // Read the event
        let reader = create_reader(key, &dir);
        let result = reader.get(0).expect("Failed to get event");
        let data = result.borrow_data();

        assert!(data.is_some());
        let bytes = data.unwrap();

        let archived = rkyv::access::<rkyv::Archived<Events>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access archived data");

        match archived {
            ArchivedEvents::Payment(payment) => match payment {
                events::payment::ArchivedPayment::Created(created) => match created {
                    events::payment::created::ArchivedCreated::V1(v1) => {
                        assert_eq!(v1.signature.as_str(), "sig_abc123xyz");
                        assert_eq!(v1.amount, 9999);
                        assert_eq!(v1.currency.as_str(), "USD");
                    }
                },
            },
            _ => panic!("Expected Payment event"),
        }
    }

    #[test]
    fn test_get_nested_user_event_with_vec_of_strings() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        let tags = vec![
            "premium".to_string(),
            "early-adopter".to_string(),
            "verified".to_string(),
        ];

        let event = Events::User(events::user::User::Registered(
            events::user::registered::Registered::V1(events::user::registered::V1 {
                email: "user@example.com".to_string(),
                name: "John Doe".to_string(),
                tags: tags.clone(),
            }),
        ));

        // Write the event
        {
            let mut writer = create_writer::<4096>(key, &dir);
            writer.append_alloc(&event).expect("Failed to append event");
        }

        // Read the event
        let reader = create_reader(key, &dir);
        let result = reader.get(0).expect("Failed to get event");
        let data = result.borrow_data();

        assert!(data.is_some());
        let bytes = data.unwrap();

        let archived = rkyv::access::<rkyv::Archived<Events>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access archived data");

        match archived {
            ArchivedEvents::User(user) => match user {
                events::user::ArchivedUser::Registered(registered) => match registered {
                    events::user::registered::ArchivedRegistered::V1(v1) => {
                        assert_eq!(v1.email.as_str(), "user@example.com");
                        assert_eq!(v1.name.as_str(), "John Doe");
                        assert_eq!(v1.tags.len(), 3);
                        assert_eq!(v1.tags[0].as_str(), "premium");
                        assert_eq!(v1.tags[1].as_str(), "early-adopter");
                        assert_eq!(v1.tags[2].as_str(), "verified");
                    }
                },
            },
            _ => panic!("Expected User event"),
        }
    }

    #[test]
    fn test_get_mixed_events_in_sequence() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        // Write a mix of events
        {
            let mut writer = create_writer::<4096>(key, &dir);

            // Event 0: Payment
            let payment = Events::Payment(events::payment::Payment::Created(
                events::payment::created::Created::V1(events::payment::created::V1 {
                    signature: "pay_001".to_string(),
                    amount: 100,
                    currency: "EUR".to_string(),
                }),
            ));
            writer.append_alloc(&payment).expect("Failed to append");

            // Event 1: User
            let user = Events::User(events::user::User::Registered(
                events::user::registered::Registered::V1(events::user::registered::V1 {
                    email: "alice@test.com".to_string(),
                    name: "Alice".to_string(),
                    tags: vec!["new".to_string()],
                }),
            ));
            writer.append_alloc(&user).expect("Failed to append");

            // Event 2: Payment
            let payment2 = Events::Payment(events::payment::Payment::Created(
                events::payment::created::Created::V1(events::payment::created::V1 {
                    signature: "pay_002".to_string(),
                    amount: 250,
                    currency: "GBP".to_string(),
                }),
            ));
            writer.append_alloc(&payment2).expect("Failed to append");
        }

        // Read and verify each
        let reader = create_reader(key, &dir);

        // Check event 0 (Payment)
        {
            let result = reader.get(0).expect("Failed to get");
            let bytes = result.borrow_data().expect("Should have data");
            let archived = rkyv::access::<rkyv::Archived<Events>, rkyv::rancor::Error>(bytes)
                .expect("Failed to access");

            match archived {
                ArchivedEvents::Payment(payment) => match payment {
                    events::payment::ArchivedPayment::Created(created) => match created {
                        events::payment::created::ArchivedCreated::V1(v1) => {
                            assert_eq!(v1.signature.as_str(), "pay_001");
                            assert_eq!(v1.amount, 100);
                            assert_eq!(v1.currency.as_str(), "EUR");
                        }
                    },
                },
                _ => panic!("Expected Payment"),
            }
        }

        // Check event 1 (User)
        {
            let result = reader.get(1).expect("Failed to get");
            let bytes = result.borrow_data().expect("Should have data");
            let archived = rkyv::access::<rkyv::Archived<Events>, rkyv::rancor::Error>(bytes)
                .expect("Failed to access");

            match archived {
                ArchivedEvents::User(user) => match user {
                    events::user::ArchivedUser::Registered(registered) => match registered {
                        events::user::registered::ArchivedRegistered::V1(v1) => {
                            assert_eq!(v1.email.as_str(), "alice@test.com");
                            assert_eq!(v1.name.as_str(), "Alice");
                        }
                    },
                },
                _ => panic!("Expected User"),
            }
        }

        // Check event 2 (Payment)
        {
            let result = reader.get(2).expect("Failed to get");
            let bytes = result.borrow_data().expect("Should have data");
            let archived = rkyv::access::<rkyv::Archived<Events>, rkyv::rancor::Error>(bytes)
                .expect("Failed to access");

            match archived {
                ArchivedEvents::Payment(payment) => match payment {
                    events::payment::ArchivedPayment::Created(created) => match created {
                        events::payment::created::ArchivedCreated::V1(v1) => {
                            assert_eq!(v1.signature.as_str(), "pay_002");
                            assert_eq!(v1.amount, 250);
                            assert_eq!(v1.currency.as_str(), "GBP");
                        }
                    },
                },
                _ => panic!("Expected Payment"),
            }
        }
    }

    #[test]
    fn test_get_event_with_long_string() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        let long_signature = "x".repeat(1000);

        let event = Events::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                signature: long_signature.clone(),
                amount: 1,
                currency: "BTC".to_string(),
            }),
        ));

        // Write the event
        {
            let mut writer = create_writer::<8192>(key, &dir);
            writer.append_alloc(&event).expect("Failed to append event");
        }

        // Read the event
        let reader = create_reader(key, &dir);
        let result = reader.get(0).expect("Failed to get event");
        let bytes = result.borrow_data().expect("Should have data");

        let archived = rkyv::access::<rkyv::Archived<Events>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access archived data");

        match archived {
            ArchivedEvents::Payment(payment) => match payment {
                events::payment::ArchivedPayment::Created(created) => match created {
                    events::payment::created::ArchivedCreated::V1(v1) => {
                        assert_eq!(v1.signature.as_str(), long_signature);
                        assert_eq!(v1.signature.len(), 1000);
                    }
                },
            },
            _ => panic!("Expected Payment event"),
        }
    }

    #[test]
    fn test_get_event_with_many_tags() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        let many_tags: Vec<String> = (0..100).map(|i| format!("tag_{}", i)).collect();

        let event = Events::User(events::user::User::Registered(
            events::user::registered::Registered::V1(events::user::registered::V1 {
                email: "power_user@example.com".to_string(),
                name: "Power User".to_string(),
                tags: many_tags.clone(),
            }),
        ));

        // Write the event
        {
            let mut writer = create_writer::<16384>(key, &dir);
            writer.append_alloc(&event).expect("Failed to append event");
        }

        // Read the event
        let reader = create_reader(key, &dir);
        let result = reader.get(0).expect("Failed to get event");
        let bytes = result.borrow_data().expect("Should have data");

        let archived = rkyv::access::<rkyv::Archived<Events>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access archived data");

        match archived {
            ArchivedEvents::User(user) => match user {
                events::user::ArchivedUser::Registered(registered) => match registered {
                    events::user::registered::ArchivedRegistered::V1(v1) => {
                        assert_eq!(v1.tags.len(), 100);
                        for (i, tag) in v1.tags.iter().enumerate() {
                            assert_eq!(tag.as_str(), format!("tag_{}", i));
                        }
                    }
                },
            },
            _ => panic!("Expected User event"),
        }
    }

    #[test]
    fn test_reader_wrong_key_fails() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key1 = generate_key();
        let key2 = generate_key();

        // Write with key1
        {
            let mut writer = create_writer::<1024>(key1, &dir);
            let event = SimpleEvent {
                id: 1,
                timestamp: 1,
                value: 1,
            };
            writer.append(&event).expect("Failed to append");
        }

        // Try to read with different key - should fail
        let reader_result = Reader::new::<ChaCha20Poly1305>(key2, dir.path());
        // The reader might open, but reading should fail or return corrupted data
        // Depending on implementation, either opening or reading will fail
        if let Ok(reader) = reader_result {
            let result = reader.get(0);
            // Either get fails or data is corrupted (we expect some form of error)
            if let Ok(get_result) = result {
                if let Some(bytes) = get_result.borrow_data() {
                    // If we got data, it should fail to deserialize correctly
                    let access_result =
                        rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(bytes);
                    // With wrong key, the data should be corrupted
                    assert!(
                        access_result.is_err() || access_result.map(|a| a.id != 1).unwrap_or(true)
                    );
                }
            }
        }
        // Test passes if we got here - wrong key caused some form of failure
    }

    #[test]
    fn test_reader_config_default() {
        let config = ReaderConfig::default();
        assert_eq!(config.max_dbs, DEFAULT_MAX_DBS);
        assert_eq!(config.map_size, DEFAULT_MAP_SIZE);
    }

    #[test]
    fn test_multiple_sequential_reads_same_sequence() {
        let dir = tempdir().expect("Failed to create temp dir");
        let key = generate_key();

        let event = SimpleEvent {
            id: 42,
            timestamp: 1234567890,
            value: 100,
        };

        // Write the event
        {
            let mut writer = create_writer::<1024>(key, &dir);
            writer.append(&event).expect("Failed to append event");
        }

        // Read the same event multiple times
        let reader = create_reader(key, &dir);

        for _ in 0..10 {
            let result = reader.get(0).expect("Failed to get event");
            let data = result.borrow_data();
            assert!(data.is_some());

            let bytes = data.unwrap();
            let archived = rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(bytes)
                .expect("Failed to access archived data");

            assert_eq!(archived.id, event.id);
            assert_eq!(archived.timestamp, event.timestamp);
            assert_eq!(archived.value, event.value);
        }
    }
}
