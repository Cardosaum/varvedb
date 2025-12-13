use std::path::Path;

use chacha20poly1305::{
    aead::{AeadMutInPlace, Key},
    KeyInit,
};
use heed3::{EncryptedEnv, Env, EnvFlags, EnvOpenOptions, RoTxn, WithoutTls};

use crate::{
    constants,
    types::{EventsDb, SequenceKey},
};

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
