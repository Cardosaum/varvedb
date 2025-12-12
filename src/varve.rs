use std::path::Path;

use chacha20poly1305::aead::AeadMutInPlace;
use chacha20poly1305::aead::Key;
use chacha20poly1305::KeyInit;
use heed3::byteorder::BigEndian;
use heed3::types::Bytes;
use heed3::types::U64;
use heed3::EncryptedDatabase;
use heed3::EncryptedEnv;
use heed3::EnvFlags;
use heed3::EnvOpenOptions;
use heed3::Error as HeedError;
use heed3::PutFlags;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::Arena;

const EVENTS_DB_NAME: &str = "events";

pub type EventsDb = EncryptedDatabase<U64<BigEndian>, Bytes>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Heed(#[from] HeedError),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub struct SerializerBuffer<const N: usize> {
    buffer: [u8; N],
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
        let env = unsafe {
            EnvOpenOptions::new()
                .read_txn_with_tls()
                .flags(EnvFlags::WRITE_MAP)
                .open_encrypted::<E, _>(key, path)?
        };
        let events_db: EventsDb = {
            let mut wtxn = env.write_txn()?;
            env.create_database(&mut wtxn, Some(EVENTS_DB_NAME))?
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

/// Type alias for our serializer with error handling
type HighSerializer<'a> = Strategy<
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
        T: for<'a> rkyv::Serialize<HighSerializer<'a>>,
    {
        // Clear the buffer before each use
        self.serializer_buffer.fill(0);

        // Use Arena for scratch space allocation
        let mut arena = Arena::new();

        // Serialize directly into our buffer
        let writer = rkyv::ser::writer::Buffer::from(&mut self.serializer_buffer);
        let sharing = rkyv::ser::sharing::Share::new();
        let mut serializer = rkyv::ser::Serializer::new(writer, arena.acquire(), sharing);

        // Wrap in Strategy for error handling (works by reference)
        let strategy = Strategy::<_, rkyv::rancor::Error>::wrap(&mut serializer);

        // Serialize the value
        let _resolver = rkyv::Serialize::serialize(event, strategy)
            .map_err(|e| Error::Serialization(format!("{:?}", e)))?;

        // Get the position (how many bytes were written)
        let pos = serializer.into_writer().len();

        // Get the serialized bytes
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
