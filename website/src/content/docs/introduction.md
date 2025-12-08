> [!WARNING]
> **UNDER DEVELOPMENT**: This project is currently in early development and is **NOT** production ready. APIs and storage formats are subject to change.

# Introduction

**VarveDB** is a high-performance, embedded, append-only event store for Rust. It provides a persistent, ACID-compliant event log optimized for high-throughput event sourcing applications.

Built on **LMDB** (Lightning Memory-Mapped Database) and **rkyv**, VarveDB is designed for scenarios where performance and data integrity are paramount, offering zero-copy access to data and strict concurrency controls.

## Key Features

*   **Zero-Copy Access**: Events are mapped directly from disk to memory using [rkyv](https://rkyv.org/), eliminating deserialization overhead for read operations.
*   **Optimistic Concurrency**: Writes are guarded by `ExpectedVersion`, allowing safe concurrent access without heavy locking.
*   **Embedded Architecture**: Runs in-process with your application, removing the latency and operational complexity of external database servers.
*   **Strongly Typed**: Enforce schema correctness at compile time with Rust types.
*   **Reactive**: subscribe to changes in real-time using efficient `tokio::watch` channels.
*   **Encryption**: Optional authenticated encryption (AES-256-GCM) ensures data is secure at rest without sacrificing the append-only nature.

## Why VarveDB?

Traditional databases often struggle with the specific requirements of event sourcing—immutable logs, global ordering, and replayability. External event stores like Kafka or EventStoreDB introduce network latency and operational overhead that may be unnecessary for reliable edge or single-node applications.

VarveDB bridges this gap by providing:
1.  **Speed**: By memory-mapping the database file, reading an event is as fast as reading a byte array from memory.
2.  **Simplicity**: No external clusters to manage. Just include it as a crate.
3.  **Safety**: ACID transactions ensure that your event log is never corrupted, even in the event of power failure.

## Use Cases

*   **Command Sourcing**: Store every state change in your application as an immutable sequence of events.
*   **Audit Logging**: cryptographically verifiable logs for compliance and security.
*   **Embedded Systems**: Efficient data storage for IoT devices where resources are constrained.
*   **High-Frequency Data**: Capture market data or sensor readings with minimal latency.
*   **Local-First Software**: Build offline-capable applications that sync when online, using the event log as the source of truth.

## Core Concepts

*   **Varve**: The main entry point to the database, handling transactions and coordination.
*   **Event**: A user-defined struct containing domain data. Must be efficiently serializable via `rkyv`.
*   **Metadata**: Sidecar information for an event (e.g., `StreamID`, timestamp, `UserID`).
*   **Stream**: A logical sequence of events sharing a `StreamID`, ordered by version.
*   **Processor**: A consumer that persistently tracks its position in the event log, ideal for building read models or projections.

