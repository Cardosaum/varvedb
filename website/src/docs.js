import './docs.css'
import hljs from 'highlight.js';
import 'highlight.js/styles/atom-one-dark.css';

// Content Templates (Migrated from Markdown)
const pages = {
  '/': `
    <div class="alert alert-warning">
      <span class="alert-title">Warning</span>
      <strong>UNDER DEVELOPMENT</strong>: This project is currently in early development and is <strong>NOT</strong> production ready. APIs and storage formats are subject to change.
    </div>

    <h1>Introduction</h1>
    <p><strong>VarveDB</strong> is a high-performance, embedded, append-only event store for Rust. It is designed to provide a persistent, ACID-compliant event log optimized for high-throughput event sourcing applications.</p>
    <p>By leveraging <strong>LMDB</strong> (Lightning Memory-Mapped Database) for reliable storage and <strong>rkyv</strong> for zero-copy deserialization, VarveDB ensures minimal overhead and maximum performance.</p>

    <h2>Key Features</h2>
    <ul>
      <li><strong>Zero-Copy Access</strong>: Events are mapped directly from disk to memory using <code>rkyv</code>, avoiding expensive deserialization steps.</li>
      <li><strong>ACID Transactions</strong>: Writes are Atomic, Consistent, Isolated, and Durable.</li>
      <li><strong>Optimistic Concurrency</strong>: Built-in stream versioning prevents race conditions and ensures data integrity.</li>
      <li><strong>Reactive Interface</strong>: Real-time event subscriptions via <code>tokio::watch</code> allow for building responsive, event-driven systems.</li>
      <li><strong>Authenticated Encryption</strong>: Optional AES-256-GCM encryption with Additional Authenticated Data (AAD) binding ensures data security at rest.</li>
      <li><strong>GDPR Compliance</strong>: Support for crypto-shredding via key deletion allows for compliant data removal.</li>
    </ul>

    <h2>Why VarveDB?</h2>
    <p>VarveDB fills the gap for a lightweight, embedded event store in the Rust ecosystem. Unlike heavy external databases like Kafka or EventStoreDB, VarveDB runs directly within your application process. This makes it ideal for:</p>
    <ul>
      <li><strong>Microservices</strong>: Keep your services self-contained with their own event logs.</li>
      <li><strong>IoT and Edge Devices</strong>: Efficient storage with low resource footprint.</li>
      <li><strong>High-Frequency Trading</strong>: Ultra-low latency event persistence.</li>
      <li><strong>Local-First Applications</strong>: Robust data storage for offline-capable apps.</li>
    </ul>

    <h2>Core Concepts</h2>
    <ul>
      <li><strong>Stream</strong>: An ordered sequence of events, identified by a <code>StreamID</code>.</li>
      <li><strong>Event</strong>: The fundamental unit of data, which can be any serializable Rust struct.</li>
      <li><strong>Writer</strong>: Appends events to the store.</li>
      <li><strong>Reader</strong>: Reads events from the store.</li>
      <li><strong>Processor</strong>: Subscribes to new events and processes them in real-time.</li>
    </ul>
  `,
  '/usage': `
    <h1>Usage Guide</h1>
    <p>This guide covers how to install, configure, and use VarveDB in your Rust application.</p>

    <h2>Installation</h2>
    <p>Add <code>varvedb</code> to your <code>Cargo.toml</code>:</p>
    <pre><code class="language-toml">[dependencies]
varvedb = "0.2" # Check crates.io for the latest version
rkyv = "0.8"
tokio = { version = "1", features = ["full"] }</code></pre>

    <h2>Defining Events</h2>
    <p>Events in VarveDB must be serializable by <code>rkyv</code>. You need to derive <code>Archive</code>, <code>Serialize</code>, and <code>Deserialize</code>.</p>
    <pre><code class="language-rust">use rkyv::{Archive, Serialize, Deserialize};

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(derive(Debug))]
pub struct MyEvent {
    pub id: u32,
    pub action: String,
    pub timestamp: u64,
}</code></pre>

    <h2>Initialization</h2>
    <p>To start, you need to open the storage engine.</p>
    <pre><code class="language-rust">use varvedb::storage::{Storage, StorageConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = StorageConfig {
        path: "./data".into(),
        encryption_enabled: true,
        master_key: Some([0u8; 32]), // In production, load this from a secure location!
        ..Default::default()
    };
    
    let storage = Storage::open(config)?;
    Ok(())
}</code></pre>

    <h2>Writing Events</h2>
    <p>Use the <code>Writer</code> to append events to a stream.</p>
    <pre><code class="language-rust">use varvedb::engine::Writer;

// ... storage initialization ...

let mut writer = Writer::<MyEvent>::new(storage.clone());

// Append an event to stream "user-123", expecting it to be empty (version 0)
let event = MyEvent { id: 1, action: "login".into(), timestamp: 1600000000 };
writer.append("user-123", 0, event)?;</code></pre>

    <h2>Reading Events</h2>
    <p>Use the <code>Reader</code> to access events.</p>
    <pre><code class="language-rust">use varvedb::engine::Reader;

// ... storage initialization ...

let reader = Reader::<MyEvent>::new(storage.clone());
let txn = storage.env.read_txn()?;

// Read the first event (global sequence 1)
if let Some(event) = reader.get(&txn, 1)? {
    println!("Event: {:?}", event);
}</code></pre>

    <h2>Processing Events</h2>
    <p>To process events in real-time, implement the <code>EventHandler</code> trait and use the <code>Processor</code>.</p>
    <pre><code class="language-rust">use varvedb::engine::{Processor, EventHandler};
use varvedb::ArchivedMyEvent; // Generated by rkyv

struct MyHandler;

impl EventHandler<MyEvent> for MyHandler {
    fn handle(&mut self, event: &ArchivedMyEvent) -> varvedb::error::Result<()> {
        println!("Handling event: {:?}", event);
        Ok(())
    }
}

// ... inside async runtime ...

let rx = writer.subscribe();

// Create a unique consumer ID (e.g., by hashing a string)
use std::hash::{Hash, Hasher};
let mut hasher = std::collections::hash_map::DefaultHasher::new();
"my-consumer-group".hash(&mut hasher);
let consumer_id = hasher.finish();

let mut processor = Processor::new(reader, MyHandler, consumer_id, rx);

// This will run indefinitely, processing new events as they arrive
processor.run().await?;</code></pre>
  `,
  '/architecture': `
    <h1>Architecture</h1>
    <p>VarveDB is built on a layered architecture that separates storage concerns from the high-level event sourcing API.</p>

    <h2>High-Level Overview</h2>
    <p>The system consists of three main components that interact with the underlying storage:</p>
    <ol>
      <li><strong>Writer</strong>: Handles appending events to the log. It manages transactions, concurrency control, and encryption.</li>
      <li><strong>Reader</strong>: Provides efficient access to stored events. It uses zero-copy deserialization to read data directly from the memory-mapped file.</li>
      <li><strong>Processor</strong>: A background task that subscribes to new events and dispatches them to registered handlers.</li>
    </ol>

    <h2>Storage Layer (LMDB)</h2>
    <p>VarveDB uses <strong>LMDB</strong> as its storage engine. LMDB is a B-tree-based key-value store that is memory-mapped, providing read performance comparable to in-memory data structures.</p>

    <h3>Data Layout</h3>
    <p>Data is organized into several "databases" (tables) within the LMDB environment:</p>
    <ul>
      <li><strong><code>events_log</code></strong>: The main append-only log.
        <ul>
            <li><strong>Key</strong>: Global Sequence Number (<code>u64</code>, big-endian).</li>
            <li><strong>Value</strong>: Serialized event data (rkyv bytes).</li>
        </ul>
      </li>
      <li><strong><code>stream_index</code></strong>: Maps stream versions to global sequence numbers.
        <ul>
            <li><strong>Key</strong>: <code>StreamID</code> + <code>StreamVersion</code>.</li>
            <li><strong>Value</strong>: Global Sequence Number.</li>
        </ul>
      </li>
      <li><strong><code>consumer_cursors</code></strong>: Tracks the progress of event processors.
        <ul>
            <li><strong>Key</strong>: Consumer Group Name (<code>String</code>).</li>
            <li><strong>Value</strong>: Last Processed Global Sequence Number.</li>
        </ul>
      </li>
      <li><strong><code>keystore</code></strong>: Stores encryption keys for streams (if encryption is enabled).
        <ul>
            <li><strong>Key</strong>: <code>StreamID</code>.</li>
            <li><strong>Value</strong>: Encrypted Stream Key (wrapped with Master Key).</li>
        </ul>
      </li>
    </ul>

    <h2>Serialization (rkyv)</h2>
    <p>VarveDB uses <strong>rkyv</strong> for serialization. Unlike Serde, which typically deserializes data into new heap-allocated structs, rkyv guarantees a memory representation that is the same on disk as it is in memory.</p>
    <p>This allows the <code>Reader</code> to return references to the data inside the memory-mapped file, completely avoiding memory allocation and copying during reads.</p>

    <h2>Concurrency Model</h2>
    <ul>
      <li><strong>Writes</strong>: LMDB allows only one write transaction at a time. The <code>Writer</code> ensures that appends are serialized.</li>
      <li><strong>Reads</strong>: Multiple read transactions can occur in parallel without blocking writes.</li>
      <li><strong>Optimistic Concurrency</strong>: When appending to a stream, the user must provide the <em>expected</em> version of that stream. If the current version in the <code>stream_index</code> does not match, the write is rejected with a <code>ConcurrencyConflict</code> error.</li>
    </ul>

    <h2>Encryption</h2>
    <p>When encryption is enabled:</p>
    <ol>
      <li>A unique 256-bit key is generated for each new stream.</li>
      <li>This stream key is encrypted (wrapped) using the provided <strong>Master Key</strong> and stored in the <code>keystore</code>.</li>
      <li>Events appended to the stream are encrypted using the stream key and AES-256-GCM.</li>
      <li>The <code>StreamID</code> and <code>SequenceNumber</code> are used as Additional Authenticated Data (AAD) to bind the ciphertext to its specific location, preventing replay attacks (e.g., copying a valid encrypted event from one stream to another).</li>
    </ol>
  `
};

// Router Logic
function render() {
  const hash = window.location.hash || '#/';
  const path = hash.replace('#', '');
  const content = pages[path] || '<h1>404 Not Found</h1>';
  
  document.getElementById('content').innerHTML = content;
  
  // Initialize Syntax Highlighting
  hljs.highlightAll();
  
  // Update Sidebar Active State
  document.querySelectorAll('.sidebar-link').forEach(link => {
    link.classList.remove('active');
    if (link.getAttribute('data-path') === path) {
      link.classList.add('active');
    }
  });

  // Scroll to top
  window.scrollTo(0, 0);
}

window.addEventListener('hashchange', render);
window.addEventListener('DOMContentLoaded', () => {
  render();

  // Mobile Sidebar Toggle
  const toggle = document.querySelector('.docs-menu-toggle');
  const sidebar = document.querySelector('.sidebar');
  
  if (toggle && sidebar) {
    toggle.addEventListener('click', () => {
      sidebar.classList.toggle('open');
    });

    // Close sidebar when clicking a link on mobile
    document.querySelectorAll('.sidebar-link').forEach(link => {
      link.addEventListener('click', () => {
        if (window.innerWidth <= 900) {
          sidebar.classList.remove('open');
        }
      });
    });
  }
});
