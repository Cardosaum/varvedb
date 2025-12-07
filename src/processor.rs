// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

use crate::engine::Reader;
use crate::traits::MetadataExt;
use crate::varve::Varve;
use rkyv::api::high::HighValidator;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Error as RancorError;

pub trait EventHandler<E>
where
    E: rkyv::Archive,
{
    fn handle(&mut self, event: &E::Archived) -> crate::error::Result<()>;
}

/// Configuration for the event processor.
#[derive(Clone, Copy, Debug)]
pub struct ProcessorConfig {
    /// Maximum number of events to process before committing the cursor.
    pub batch_size: usize,
    /// Maximum time to wait before committing the cursor, even if batch_size is not reached.
    pub batch_timeout: std::time::Duration,
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: crate::constants::DEFAULT_BATCH_SIZE,
            batch_timeout: std::time::Duration::from_millis(
                crate::constants::DEFAULT_BATCH_TIMEOUT_MS,
            ),
        }
    }
}

pub struct Processor<E, H> {
    reader: Reader<E>,
    handler: H,
    consumer_id: u64,
    rx: tokio::sync::watch::Receiver<u64>,
    config: ProcessorConfig,
}

impl<E, H> Processor<E, H>
where
    E: rkyv::Archive
        + for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                RancorError,
            >,
        > + std::fmt::Debug,
    E::Archived: for<'a> CheckBytes<HighValidator<'a, RancorError>>,
    H: EventHandler<E>,
{
    /// Creates a new `Processor`.
    ///
    /// *   `consumer_id`: A unique identifier for this consumer. Used to persist the cursor position.
    pub fn new<M>(
        varve: &Varve<E, M>,
        handler: H,
        consumer_id: impl Into<u64>, // Maintain Into<u64> flexibility
    ) -> Self
    where
        M: MetadataExt,
    {
        // Initialize reader and event subscription from Varve.
        let reader = varve.reader().clone();
        let rx = varve.subscribe();

        Self {
            reader,
            handler,
            consumer_id: consumer_id.into(),
            rx,
            config: ProcessorConfig::default(),
        }
    }

    /// Sets the configuration for the processor.
    pub fn with_config(mut self, config: ProcessorConfig) -> Self {
        self.config = config;
        self
    }

    // Placeholder for cancellation token if needed
    pub fn with_cancellation_token(self, _token: ()) -> Self {
        self
    }

    /// Starts the event processing loop.
    pub async fn run(&mut self) -> crate::error::Result<()> {
        let mut current_seq = {
            let txn = self.reader.storage().env.read_txn()?;
            self.reader
                .storage()
                .consumer_cursors
                .get(&txn, &self.consumer_id)?
                .unwrap_or(0)
        };

        loop {
            let head_seq = *self.rx.borrow();

            if current_seq < head_seq {
                current_seq = self.process_backlog(current_seq, head_seq)?;
            }

            if current_seq >= *self.rx.borrow() {
                self.rx.changed().await.map_err(|_| {
                    crate::error::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "Sender dropped",
                    ))
                })?;
            }
        }
    }

    fn process_backlog(
        &mut self,
        mut current_seq: u64,
        target_seq: u64,
    ) -> crate::error::Result<u64> {
        let mut pending_updates = 0;
        let mut last_commit = std::time::Instant::now();
        let mut read_txn: Option<heed::RoTxn> = None;

        while current_seq < target_seq {
            if read_txn.is_none() {
                read_txn = Some(self.reader.storage().env.read_txn()?);
            }
            let txn = read_txn.as_ref().unwrap();

            let mut processed_any = false;
            let mut reached_snapshot_end = false;

            while current_seq < target_seq {
                let next_seq = current_seq + 1;
                if let Some(event) = self.reader.get(txn, next_seq)? {
                    self.handler.handle(&event)?;
                    current_seq = next_seq;
                    pending_updates += 1;
                    processed_any = true;
                } else {
                    reached_snapshot_end = true;
                    break;
                }

                if pending_updates >= self.config.batch_size {
                    break;
                }
            }

            if pending_updates >= self.config.batch_size
                || (processed_any && last_commit.elapsed() >= self.config.batch_timeout)
            {
                let mut wtxn = self.reader.storage().env.write_txn()?;
                self.reader.storage().consumer_cursors.put(
                    &mut wtxn,
                    &self.consumer_id,
                    &current_seq,
                )?;
                wtxn.commit()?;
                pending_updates = 0;
                last_commit = std::time::Instant::now();
            }

            if reached_snapshot_end {
                read_txn = None;
            }
        }

        if pending_updates > 0 {
            let mut wtxn = self.reader.storage().env.write_txn()?;
            self.reader.storage().consumer_cursors.put(
                &mut wtxn,
                &self.consumer_id,
                &current_seq,
            )?;
            wtxn.commit()?;
        }

        Ok(current_seq)
    }
}
