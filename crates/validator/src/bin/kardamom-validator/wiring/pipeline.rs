//! The trie-aware writer and the optional L1 output attester: everything
//! between the open subscriptions and the receipts sink.

use anyhow::{Context, Result};
use kardamom_engine::{MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal};
use kardamom_state::StateWriter;
use kardamom_validator::ValidatorWriterQueue;
use kardamom_validator::attester::{self, AttesterConfig, AttesterHandle};

use super::startup::Streamed;

impl Streamed {
    /// Spawn the BAL and receipts verification pumps. Both run for the
    /// process lifetime, stopped only by `pump_shutdown` at shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if either subscription fails to open.
    pub(crate) fn spawn_pumps(self) -> Result<Self> {
        crate::pumps::BalPump::new(
            &self.opened.base.rt,
            &self.opened.base.channels,
            crate::pumps::BalSinks {
                bals: self.streams.bals.clone(),
                claims: self.streams.claims.clone(),
                extract_claims: self
                    .streams
                    .interop_serve
                    .as_ref()
                    .map(|s| s.claims.clone()),
            },
            self.streams.pump_shutdown.clone(),
        )?
        .spawn();
        crate::pumps::ReceiptsPump::new(
            &self.opened.base.rt,
            &self.opened.base.channels,
            self.opened.base.args.executor_count,
            self.streams.receipts.clone(),
            self.streams.pump_shutdown.clone(),
        )?
        .spawn();
        Ok(self)
    }

    /// Seed genesis into a fresh env, then spawn the trie-aware writer:
    /// each block commit advances the MPT state root. Runs after
    /// [`crate::adoption::bootstrap_trie_if_adopted`], against the open
    /// env, before the writer spawns.
    ///
    /// # Errors
    ///
    /// Returns an error if seeding genesis, bootstrapping the trie, or
    /// spawning the writer fails.
    pub(crate) fn spawn_writer(self) -> Result<Written> {
        let args = &self.opened.base.args;
        let (genesis_accounts, genesis_code) =
            kardamom_engine::bin_support::build_genesis_alloc(self.opened.state.genesis.as_ref());
        let seeded =
            kardamom_state::seed_genesis(&self.opened.state.env, &genesis_accounts, &genesis_code)
                .context("seed genesis into validator state env")?;
        tracing::info!(
            state_dir = %args.state_dir.display(),
            genesis_accounts = genesis_accounts.len(),
            seeded,
            "validator state env opened"
        );

        let trie_mode = match args.trie_shadow_check {
            Some(every_n) => kardamom_state::TrieMode::ShadowCheck {
                every_n: every_n.get(),
            },
            None => kardamom_state::TrieMode::Incremental,
        };
        // Adoption, half two: rebuild the mirror and trie when the
        // adoption marker, or a truly trie-less image, says to. This must
        // run before the trie-aware writer spawns.
        crate::adoption::bootstrap_trie_if_adopted(&args.state_dir, &self.opened.state.env)?;
        // `StateEnv` is `Arc`-backed and clones cheaply; cloning it out
        // here, instead of moving the field, keeps `self.opened` whole so
        // it can nest into `Written` as one field below.
        let writer = StateWriter::spawn_with_trie(self.opened.state.env.clone(), trie_mode)
            .context("spawn trie-aware state writer")?;
        let snapshots = MdbxSnapshotSource::new(writer.snapshot_rx.clone());
        let writer_signal = MdbxWriterSignal::new(writer.snapshot_rx.clone());
        let writer_queue = ValidatorWriterQueue::new(
            MdbxWriterQueue::new(writer.delta_tx.clone()),
            self.streams.bals.clone(),
            self.streams.divergence.clone(),
        )
        // Blocks at or below the recovery resume point were verified
        // before the restart. Replay re-execution against already-applied
        // state gives empty deltas that cannot match the BAL. Comparing
        // them would report a false divergence on every restart.
        .with_verify_floor(self.opened.state.recovery.last_committed_block);

        Ok(Written {
            streamed: self,
            writer: WriterPorts {
                writer,
                snapshots,
                writer_signal,
                writer_queue,
            },
        })
    }
}

/// The trie-aware writer and its ports: everything [`Streamed::spawn_writer`]
/// adds.
pub(crate) struct WriterPorts {
    pub(super) writer: kardamom_state::WriterHandle,
    pub(super) snapshots: MdbxSnapshotSource,
    pub(super) writer_signal: MdbxWriterSignal,
    pub(super) writer_queue: ValidatorWriterQueue<MdbxWriterQueue>,
}

/// [`Streamed`], plus the trie-aware writer [`Streamed::spawn_writer`]
/// spawned.
pub(crate) struct Written {
    pub(super) streamed: Streamed,
    pub(super) writer: WriterPorts,
}

impl Written {
    /// Enable the L1 output attester when all three flags
    /// (`--l1-rpc-url`, `--output-oracle`, `--attester-key`) are present.
    /// Without all three, the validator does no automatic attestation. Any
    /// other combination is a configuration error. The task lives as long
    /// as a handle clone does; the handle is held for the process
    /// lifetime.
    ///
    /// # Errors
    ///
    /// Returns an error if a partial set of the three flags is given, or
    /// if the attester fails to spawn (a bad key or a bad L1 RPC URL).
    pub(crate) fn spawn_attester(self) -> Result<Attested> {
        let args = &self.streamed.opened.base.args;
        let attester_handle = match (
            args.l1_rpc_url.clone(),
            args.output_oracle,
            args.attester_key.clone(),
        ) {
            (Some(l1_rpc_url), Some(oracle), Some(key)) => {
                let attester::SpawnedAttester {
                    handle,
                    task: _task,
                } = attester::spawn_attester(&AttesterConfig {
                    l1_rpc_url,
                    oracle,
                    signer: key.into_signer(),
                    post_interval_blocks: args.attester_post_interval.get(),
                });
                tracing::info!(
                    oracle = %oracle,
                    post_interval_blocks = args.attester_post_interval.get().get(),
                    "L1 output attester enabled"
                );
                Some(handle)
            }
            (None, None, None) => None, // Default: no automatic attestation.
            _ => anyhow::bail!(
                "attestation needs --l1-rpc-url, --output-oracle and \
                 --attester-key together (got a partial set)"
            ),
        };

        Ok(Attested {
            written: self,
            attester_handle,
        })
    }
}

/// [`Written`], plus the resolved (possibly absent) attester handle
/// [`Written::spawn_attester`] adds.
pub(crate) struct Attested {
    pub(super) written: Written,
    pub(super) attester_handle: Option<AttesterHandle>,
}
