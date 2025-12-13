// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Integration tests for varvedb writer and reader.
//!
//! These tests verify that events can be written and read back correctly,
//! testing the full round-trip through the event store.

use chacha20poly1305::{
    aead::{Key, OsRng},
    ChaCha20Poly1305, KeyInit,
};
use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;
use varvedb::{
    reader::{Reader, ReaderConfig},
    writer::{Writer, WriterConfig},
};

// ============================================
// Event type definitions
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

// ============================================
// Simple POD event
// ============================================

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(attr(derive(Debug)))]
pub struct SimpleEvent {
    pub id: u64,
    pub timestamp: u64,
    pub value: i32,
}

// ============================================
// Helper functions
// ============================================

fn generate_key() -> Key<ChaCha20Poly1305> {
    ChaCha20Poly1305::generate_key(&mut OsRng)
}

// ============================================
// Integration Tests
// ============================================

#[test]
fn test_write_and_read_simple_event() {
    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

    let event = SimpleEvent {
        id: 1,
        timestamp: 1702400000,
        value: 42,
    };

    // Write the event
    {
        let mut writer = Writer::<1024>::new::<ChaCha20Poly1305>(key, dir.path())
            .expect("Failed to create writer");
        writer.append(&event).expect("Failed to append event");
    }

    // Read the event
    let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader");
    let result = reader.get(0).expect("Failed to get event");
    let bytes = result.borrow_data().expect("Should have data");

    let archived = rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(bytes)
        .expect("Failed to access");

    assert_eq!(archived.id, 1);
    assert_eq!(archived.timestamp, 1702400000);
    assert_eq!(archived.value, 42);
}

#[test]
fn test_write_and_read_multiple_simple_events() {
    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

    const NUM_EVENTS: u64 = 100;

    // Write events
    {
        let mut writer = Writer::<1024>::new::<ChaCha20Poly1305>(key, dir.path())
            .expect("Failed to create writer");

        for i in 0..NUM_EVENTS {
            let event = SimpleEvent {
                id: i,
                timestamp: 1702400000 + i,
                value: (i * 10) as i32,
            };
            writer.append(&event).expect("Failed to append event");
        }
    }

    // Read and verify all events
    let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader");

    for i in 0..NUM_EVENTS {
        let result = reader.get(i).expect("Failed to get event");
        let bytes = result.borrow_data().expect("Should have data");

        let archived = rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access");

        assert_eq!(archived.id, i);
        assert_eq!(archived.timestamp, 1702400000 + i);
        assert_eq!(archived.value, (i * 10) as i32);
    }
}

#[test]
fn test_write_and_read_payment_event() {
    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

    let event = DomainEvent::Payment(events::payment::Payment::Created(
        events::payment::created::Created::V1(events::payment::created::V1 {
            payment_id: "pay_123abc".to_string(),
            amount: 9999,
            currency: "USD".to_string(),
            customer_id: "cust_456def".to_string(),
        }),
    ));

    // Write the event
    {
        let mut writer = Writer::<4096>::new::<ChaCha20Poly1305>(key, dir.path())
            .expect("Failed to create writer");
        writer.append_alloc(&event).expect("Failed to append event");
    }

    // Read the event
    let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader");
    let result = reader.get(0).expect("Failed to get event");
    let bytes = result.borrow_data().expect("Should have data");

    let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
        .expect("Failed to access");

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
fn test_write_and_read_user_event_with_tags() {
    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

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

    // Write the event
    {
        let mut writer = Writer::<4096>::new::<ChaCha20Poly1305>(key, dir.path())
            .expect("Failed to create writer");
        writer.append_alloc(&event).expect("Failed to append event");
    }

    // Read the event
    let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader");
    let result = reader.get(0).expect("Failed to get event");
    let bytes = result.borrow_data().expect("Should have data");

    let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
        .expect("Failed to access");

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
fn test_write_and_read_order_with_nested_items() {
    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

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

    // Write the event
    {
        let mut writer = Writer::<8192>::new::<ChaCha20Poly1305>(key, dir.path())
            .expect("Failed to create writer");
        writer.append_alloc(&event).expect("Failed to append event");
    }

    // Read the event
    let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader");
    let result = reader.get(0).expect("Failed to get event");
    let bytes = result.borrow_data().expect("Should have data");

    let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
        .expect("Failed to access");

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
fn test_write_and_read_mixed_event_stream() {
    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

    // Write a realistic sequence of domain events
    {
        let mut writer = Writer::<8192>::new::<ChaCha20Poly1305>(key, dir.path())
            .expect("Failed to create writer");

        // Event 0: User registered
        let user_registered = DomainEvent::User(events::user::User::Registered(
            events::user::registered::Registered::V1(events::user::registered::V1 {
                user_id: "usr_001".to_string(),
                email: "alice@example.com".to_string(),
                name: "Alice".to_string(),
                tags: vec!["new".to_string()],
            }),
        ));
        writer
            .append_alloc(&user_registered)
            .expect("Failed to append");

        // Event 1: Order placed
        let order_placed = DomainEvent::Order(events::order::Order::Placed(
            events::order::placed::Placed::V1(events::order::placed::V1 {
                order_id: "ord_001".to_string(),
                customer_id: "usr_001".to_string(),
                items: vec![events::order::placed::OrderItem {
                    product_id: "prod_abc".to_string(),
                    quantity: 1,
                    price: 2999,
                }],
                total: 2999,
            }),
        ));
        writer
            .append_alloc(&order_placed)
            .expect("Failed to append");

        // Event 2: Payment created
        let payment_created = DomainEvent::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                payment_id: "pay_001".to_string(),
                amount: 2999,
                currency: "USD".to_string(),
                customer_id: "usr_001".to_string(),
            }),
        ));
        writer
            .append_alloc(&payment_created)
            .expect("Failed to append");

        // Event 3: Order shipped
        let order_shipped = DomainEvent::Order(events::order::Order::Shipped(
            events::order::shipped::Shipped::V1(events::order::shipped::V1 {
                order_id: "ord_001".to_string(),
                tracking_number: "1Z999AA10123456784".to_string(),
                carrier: "UPS".to_string(),
            }),
        ));
        writer
            .append_alloc(&order_shipped)
            .expect("Failed to append");

        // Event 4: User profile updated
        let profile_updated = DomainEvent::User(events::user::User::ProfileUpdated(
            events::user::profile_updated::ProfileUpdated::V1(events::user::profile_updated::V1 {
                user_id: "usr_001".to_string(),
                field: "name".to_string(),
                old_value: "Alice".to_string(),
                new_value: "Alice Smith".to_string(),
            }),
        ));
        writer
            .append_alloc(&profile_updated)
            .expect("Failed to append");

        // Event 5: Order delivered
        let order_delivered = DomainEvent::Order(events::order::Order::Delivered(
            events::order::delivered::Delivered::V1(events::order::delivered::V1 {
                order_id: "ord_001".to_string(),
                signature: "Alice Smith".to_string(),
                delivered_at: 1702500000,
            }),
        ));
        writer
            .append_alloc(&order_delivered)
            .expect("Failed to append");
    }

    // Read and verify all events
    let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader");

    // Verify event 0: User registered
    {
        let result = reader.get(0).expect("Failed to get");
        let bytes = result.borrow_data().expect("Should have data");
        let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access");

        match archived {
            ArchivedDomainEvent::User(events::user::ArchivedUser::Registered(registered)) => {
                match registered {
                    events::user::registered::ArchivedRegistered::V1(v1) => {
                        assert_eq!(v1.user_id.as_str(), "usr_001");
                        assert_eq!(v1.email.as_str(), "alice@example.com");
                    }
                }
            }
            _ => panic!("Expected User::Registered at position 0"),
        }
    }

    // Verify event 1: Order placed
    {
        let result = reader.get(1).expect("Failed to get");
        let bytes = result.borrow_data().expect("Should have data");
        let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access");

        match archived {
            ArchivedDomainEvent::Order(events::order::ArchivedOrder::Placed(placed)) => {
                match placed {
                    events::order::placed::ArchivedPlaced::V1(v1) => {
                        assert_eq!(v1.order_id.as_str(), "ord_001");
                        assert_eq!(v1.total, 2999);
                    }
                }
            }
            _ => panic!("Expected Order::Placed at position 1"),
        }
    }

    // Verify event 2: Payment created
    {
        let result = reader.get(2).expect("Failed to get");
        let bytes = result.borrow_data().expect("Should have data");
        let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access");

        match archived {
            ArchivedDomainEvent::Payment(events::payment::ArchivedPayment::Created(created)) => {
                match created {
                    events::payment::created::ArchivedCreated::V1(v1) => {
                        assert_eq!(v1.payment_id.as_str(), "pay_001");
                        assert_eq!(v1.amount, 2999);
                    }
                }
            }
            _ => panic!("Expected Payment::Created at position 2"),
        }
    }

    // Verify event 3: Order shipped
    {
        let result = reader.get(3).expect("Failed to get");
        let bytes = result.borrow_data().expect("Should have data");
        let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access");

        match archived {
            ArchivedDomainEvent::Order(events::order::ArchivedOrder::Shipped(shipped)) => {
                match shipped {
                    events::order::shipped::ArchivedShipped::V1(v1) => {
                        assert_eq!(v1.order_id.as_str(), "ord_001");
                        assert_eq!(v1.tracking_number.as_str(), "1Z999AA10123456784");
                    }
                }
            }
            _ => panic!("Expected Order::Shipped at position 3"),
        }
    }

    // Verify event 4: Profile updated
    {
        let result = reader.get(4).expect("Failed to get");
        let bytes = result.borrow_data().expect("Should have data");
        let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access");

        match archived {
            ArchivedDomainEvent::User(events::user::ArchivedUser::ProfileUpdated(updated)) => {
                match updated {
                    events::user::profile_updated::ArchivedProfileUpdated::V1(v1) => {
                        assert_eq!(v1.user_id.as_str(), "usr_001");
                        assert_eq!(v1.new_value.as_str(), "Alice Smith");
                    }
                }
            }
            _ => panic!("Expected User::ProfileUpdated at position 4"),
        }
    }

    // Verify event 5: Order delivered
    {
        let result = reader.get(5).expect("Failed to get");
        let bytes = result.borrow_data().expect("Should have data");
        let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access");

        match archived {
            ArchivedDomainEvent::Order(events::order::ArchivedOrder::Delivered(delivered)) => {
                match delivered {
                    events::order::delivered::ArchivedDelivered::V1(v1) => {
                        assert_eq!(v1.order_id.as_str(), "ord_001");
                        assert_eq!(v1.signature.as_str(), "Alice Smith");
                        assert_eq!(v1.delivered_at, 1702500000);
                    }
                }
            }
            _ => panic!("Expected Order::Delivered at position 5"),
        }
    }

    // Verify no more events
    let result = reader.get(6).expect("Failed to get");
    assert!(result.borrow_data().is_none());
}

#[test]
fn test_write_with_custom_config_and_read() {
    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

    // Write with custom config
    {
        let config = WriterConfig {
            max_dbs: 2,
            map_size: 50 * 1024 * 1024, // 50 MB
        };

        let mut writer = Writer::<4096>::with_config::<ChaCha20Poly1305>(key, dir.path(), config)
            .expect("Failed to create writer");

        let event = SimpleEvent {
            id: 999,
            timestamp: 1234567890,
            value: -42,
        };
        writer.append(&event).expect("Failed to append");
    }

    // Read with custom config
    let config = ReaderConfig {
        max_dbs: 2,
        map_size: 50 * 1024 * 1024,
    };

    let reader = Reader::with_config::<ChaCha20Poly1305>(key, dir.path(), config)
        .expect("Failed to create reader");
    let result = reader.get(0).expect("Failed to get");
    let bytes = result.borrow_data().expect("Should have data");

    let archived = rkyv::access::<rkyv::Archived<SimpleEvent>, rkyv::rancor::Error>(bytes)
        .expect("Failed to access");

    assert_eq!(archived.id, 999);
    assert_eq!(archived.value, -42);
}

#[test]
fn test_large_event_write_and_read() {
    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

    // Create event with large strings and many items
    let long_description = "x".repeat(5000);
    let many_tags: Vec<String> = (0..500).map(|i| format!("tag_{:05}", i)).collect();

    let event = DomainEvent::User(events::user::User::Registered(
        events::user::registered::Registered::V1(events::user::registered::V1 {
            user_id: "usr_large".to_string(),
            email: long_description.clone(),
            name: "Large User".to_string(),
            tags: many_tags.clone(),
        }),
    ));

    // Write with large buffer
    {
        let mut writer = Writer::<65536>::new::<ChaCha20Poly1305>(key, dir.path())
            .expect("Failed to create writer");
        writer.append_alloc(&event).expect("Failed to append event");
    }

    // Read back
    let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader");
    let result = reader.get(0).expect("Failed to get event");
    let bytes = result.borrow_data().expect("Should have data");

    let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
        .expect("Failed to access");

    match archived {
        ArchivedDomainEvent::User(user) => match user {
            events::user::ArchivedUser::Registered(registered) => match registered {
                events::user::registered::ArchivedRegistered::V1(v1) => {
                    assert_eq!(v1.email.len(), 5000);
                    assert_eq!(v1.tags.len(), 500);
                    for (i, tag) in v1.tags.iter().enumerate() {
                        assert_eq!(tag.as_str(), format!("tag_{:05}", i));
                    }
                }
            },
            _ => panic!("Expected Registered"),
        },
        _ => panic!("Expected User"),
    }
}

#[test]
fn test_concurrent_readers() {
    use std::sync::Arc;
    use std::thread;

    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

    // Write some events
    {
        let mut writer = Writer::<1024>::new::<ChaCha20Poly1305>(key, dir.path())
            .expect("Failed to create writer");

        for i in 0..100 {
            let event = SimpleEvent {
                id: i,
                timestamp: 1000 + i,
                value: (i * 2) as i32,
            };
            writer.append(&event).expect("Failed to append");
        }
    }

    let reader = Arc::new(
        Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader"),
    );

    // Spawn multiple reader threads
    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let reader = Arc::clone(&reader);
            thread::spawn(move || {
                // Each thread reads a different portion
                for i in (thread_id * 25)..((thread_id + 1) * 25) {
                    let result = reader
                        .get(i as u64)
                        .inspect_err(|e| eprintln!("Error getting event: {:?}", e))
                        .expect("Failed to get");
                    let bytes = result.borrow_data().expect("Should have data");

                    // NOTE: The bytes returned from LMDB/heed are borrowed from the transaction.
                    // For concurrent reads we defensively copy them into an aligned, owned buffer
                    // before decoding.
                    let mut owned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
                    owned.extend_from_slice(bytes);

                    let event = rkyv::from_bytes::<SimpleEvent, rkyv::rancor::Error>(&owned)
                        .inspect_err(|e| eprintln!("Error deserializing event: {:?}", e))
                        .expect("Failed to deserialize");

                    assert_eq!(event.id, i as u64);
                    assert_eq!(event.value, i * 2);
                }
            })
        })
        .collect();

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

#[test]
fn test_read_nonexistent_sequence() {
    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

    // Write one event
    {
        let mut writer = Writer::<1024>::new::<ChaCha20Poly1305>(key, dir.path())
            .expect("Failed to create writer");
        let event = SimpleEvent {
            id: 0,
            timestamp: 0,
            value: 0,
        };
        writer.append(&event).expect("Failed to append");
    }

    let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader");

    // Sequence 0 should exist
    assert!(reader
        .get(0)
        .expect("Failed to get")
        .borrow_data()
        .is_some());

    // Sequence 1 should not exist
    assert!(reader
        .get(1)
        .expect("Failed to get")
        .borrow_data()
        .is_none());

    // High sequences should not exist
    assert!(reader
        .get(100)
        .expect("Failed to get")
        .borrow_data()
        .is_none());
    assert!(reader
        .get(u64::MAX)
        .expect("Failed to get")
        .borrow_data()
        .is_none());
}

#[test]
fn test_payment_refund_flow() {
    let dir = tempdir().expect("Failed to create temp dir");
    let key = generate_key();

    // Write payment flow
    {
        let mut writer = Writer::<4096>::new::<ChaCha20Poly1305>(key, dir.path())
            .expect("Failed to create writer");

        // Payment created
        let payment = DomainEvent::Payment(events::payment::Payment::Created(
            events::payment::created::Created::V1(events::payment::created::V1 {
                payment_id: "pay_refund_test".to_string(),
                amount: 5000,
                currency: "EUR".to_string(),
                customer_id: "cust_refund".to_string(),
            }),
        ));
        writer.append_alloc(&payment).expect("Failed to append");

        // Payment refunded
        let refund = DomainEvent::Payment(events::payment::Payment::Refunded(
            events::payment::refunded::Refunded::V1(events::payment::refunded::V1 {
                payment_id: "pay_refund_test".to_string(),
                refund_amount: 5000,
                reason: "Customer request".to_string(),
            }),
        ));
        writer.append_alloc(&refund).expect("Failed to append");
    }

    let reader = Reader::new::<ChaCha20Poly1305>(key, dir.path()).expect("Failed to create reader");

    // Verify payment
    {
        let result = reader.get(0).expect("Failed to get");
        let bytes = result.borrow_data().expect("Should have data");
        let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access");

        match archived {
            ArchivedDomainEvent::Payment(events::payment::ArchivedPayment::Created(created)) => {
                match created {
                    events::payment::created::ArchivedCreated::V1(v1) => {
                        assert_eq!(v1.amount, 5000);
                    }
                }
            }
            _ => panic!("Expected Payment::Created"),
        }
    }

    // Verify refund
    {
        let result = reader.get(1).expect("Failed to get");
        let bytes = result.borrow_data().expect("Should have data");
        let archived = rkyv::access::<rkyv::Archived<DomainEvent>, rkyv::rancor::Error>(bytes)
            .expect("Failed to access");

        match archived {
            ArchivedDomainEvent::Payment(events::payment::ArchivedPayment::Refunded(refunded)) => {
                match refunded {
                    events::payment::refunded::ArchivedRefunded::V1(v1) => {
                        assert_eq!(v1.refund_amount, 5000);
                        assert_eq!(v1.reason.as_str(), "Customer request");
                    }
                }
            }
            _ => panic!("Expected Payment::Refunded"),
        }
    }
}
