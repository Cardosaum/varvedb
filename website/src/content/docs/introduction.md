> [!WARNING]
> **UNDER DEVELOPMENT**: This project is currently in early development and is **NOT** production ready. APIs and storage formats are subject to change.

# Introduction

**VarveDB** is a high-performance, embedded, append-only event store for Rust. It is designed to provide a persistent, ACID-compliant event log optimized for high-throughput event sourcing applications.

By leveraging **LMDB** (Lightning Memory-Mapped Database) for reliable storage and **rkyv** for zero-copy deserialization, VarveDB ensures minimal overhead and maximum performance.

## Key Features

*   **Zero-Copy Access**: Events are mapped directly from disk to memory using `rkyv`, avoiding expensive deserialization steps.
*   **ACID Transactions**: Writes are Atomic, Consistent, Isolated, and Durable.
*   **Optimistic Concurrency**: Built-in stream versioning prevents race conditions and ensures data integrity.
*   **Reactive Interface**: Real-time event subscriptions via `tokio::watch` allow for building responsive, event-driven systems.
*   **Authenticated Encryption**: Optional AES-256-GCM encryption with Additional Authenticated Data (AAD) binding ensures data security at rest.
*   **GDPR Compliance**: Support for crypto-shredding via key deletion allows for compliant data removal.

## Why VarveDB?

VarveDB fills the gap for a lightweight, embedded event store in the Rust ecosystem. Unlike heavy external databases like Kafka or EventStoreDB, VarveDB runs directly within your application process. This makes it ideal for:

*   **Microservices**: Keep your services self-contained with their own event logs.
*   **IoT and Edge Devices**: Efficient storage with low resource footprint.
*   **High-Frequency Trading**: Ultra-low latency event persistence.
*   **Local-First Applications**: robust data storage for offline-capable apps.

## Core Concepts

*   **Stream**: An ordered sequence of events, identified by a `StreamID`.
*   **Event**: The fundamental unit of data, which can be any serializable Rust struct.
*   **Writer**: Appends events to the store.
*   **Reader**: Reads events from the store.
*   **Processor**: Subscribes to new events and processes them in real-time.
