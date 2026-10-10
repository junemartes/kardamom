//! `deposit`: a small deposit on L1 through the lockbox from the
//! canary's L1 account to the first ring account, then the wait for the
//! L2 credit. Stages: the L1 inclusion, the L1 finality, and the L2
//! credit, whose receipt hash is the deposit's source hash. The first
//! deposit, while the first ring account holds less than it, is the
//! larger one that funds the ring and the pool.
//!
//! The signed L1 transaction goes to a journal before the submit, so a
//! restart sends the same bytes again before it signs another.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::TxEip1559;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256, keccak256};
use kardamom_bench::signers::DerivedSigner;
use tokio::time::Instant;

use super::{Context, Probe};
use crate::contracts::{address_word, call};
use crate::metrics;
use crate::outcome::{Outcome, Stage};
use crate::ring::journal::{InFlight, Journal};
use crate::rpc::{Receipt, Rpc, RpcError};
use crate::wait::Poll;

const NAME: &str = "deposit";
/// The L2 gas limit of the deposit: a plain credit.
const L2_GAS: u64 = 21_000;
/// The L1 gas limit of `depositETH` with empty data.
const L1_GAS: u64 = 150_000;
/// The poll of L1, which closes a block every 12 s.
const L1_POLL: Duration = Duration::from_secs(4);

/// What the `deposit` probe needs beyond the shared context.
#[derive(Debug, Clone)]
pub struct DepositSettings {
    pub l1: Rpc,
    pub signer: DerivedSigner,
    pub lockbox: Address,
    pub amount: U256,
    pub first_amount: U256,
    pub floor: U256,
}

impl DepositSettings {
    /// The settings when the L1 endpoint, the key and the lockbox are all
    /// given; `None` otherwise, and the probe does not run.
    ///
    /// # Errors
    ///
    /// Returns an error when the key does not parse or the endpoint URL
    /// does not.
    pub fn new(args: &crate::config::Args, timeout: Duration) -> anyhow::Result<Option<Self>> {
        let (Some(url), Some(key), Some(lockbox)) = (&args.l1_rpc, &args.l1_key, args.lockbox)
        else {
            return Ok(None);
        };
        let signer: alloy_signer_local::PrivateKeySigner = key
            .parse()
            .map_err(|e| anyhow::anyhow!("the canary's L1 key does not parse: {e}"))?;
        let endpoint = crate::config::Endpoint {
            name: "l1".to_string(),
            url: url.clone(),
        };
        Ok(Some(Self {
            l1: Rpc::new(endpoint, timeout)?,
            signer: DerivedSigner {
                address: signer.address(),
                signer,
            },
            lockbox,
            amount: args.deposit_wei,
            first_amount: args.first_deposit_wei,
            floor: args.l1_floor_wei,
        }))
    }
}

#[derive(Debug)]
pub struct Deposit {
    ctx: Arc<Context>,
    settings: DepositSettings,
    journal: Journal,
}

/// A deposit that L1 holds: its receipt and its source hash.
struct OnL1 {
    receipt: Receipt,
    source: B256,
}

impl Deposit {
    /// The probe, with its L1 journal under `dir`.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal directory cannot be made.
    pub async fn new(
        ctx: Arc<Context>,
        settings: DepositSettings,
        dir: &Path,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(dir).await?;
        let journal = Journal::new(dir, settings.signer.address);
        Ok(Self {
            ctx,
            settings,
            journal,
        })
    }

    fn l1_error(e: &RpcError) -> Outcome {
        tracing::warn!(error = %e, "deposit: L1 call failed");
        Outcome::L1Error
    }

    async fn attempt(&self) -> Result<(), Outcome> {
        let account = self.settings.signer.address;
        let balance = self
            .settings
            .l1
            .balance(account)
            .await
            .map_err(|e| Self::l1_error(&e))?;
        ::metrics::gauge!(metrics::BALANCE_WEI, "layer" => "l1", "account" => format!("{account:#x}"))
            .set(metrics::wei(balance));
        if balance < self.settings.floor {
            return Err(Outcome::Unfunded);
        }
        let started = Instant::now();
        let entry = self.pending_or_new().await?;
        let on_l1 = self.included(&entry).await?;
        let included = Instant::now();
        metrics::stage(
            NAME,
            "l1",
            "l1_inclusion",
            included.saturating_duration_since(started),
        );
        self.finalized(on_l1.receipt.block()).await?;
        let finalized = Instant::now();
        metrics::stage(
            NAME,
            "l1",
            "l1_finality",
            finalized.saturating_duration_since(included),
        );
        self.credited(on_l1.source).await?;
        metrics::stage(NAME, "l1", "l2_credit", finalized.elapsed());
        Ok(())
    }

    /// The transaction in the journal, or a new deposit, signed and
    /// journaled, then submitted.
    async fn pending_or_new(&self) -> Result<InFlight, Outcome> {
        let pending = self.journal.load().await.map_err(|_| Outcome::L1Error)?;
        let entry = match pending {
            Some(entry) => entry,
            None => self.sign().await?,
        };
        match self.settings.l1.send_raw(&entry.raw).await {
            Err(RpcError::Refused { .. }) if !entry.published => {
                self.forget().await;
                Err(Outcome::L1Error)
            }
            _ => {
                let published = InFlight {
                    published: true,
                    ..entry
                };
                self.journal
                    .store(&published)
                    .await
                    .map_err(|_| Outcome::L1Error)?;
                Ok(published)
            }
        }
    }

    async fn sign(&self) -> Result<InFlight, Outcome> {
        let l1 = &self.settings.l1;
        let account = self.settings.signer.address;
        let chain_id = l1.chain_id().await.map_err(|e| Self::l1_error(&e))?;
        let nonce = l1.nonce(account).await.map_err(|e| Self::l1_error(&e))?;
        let price = l1.gas_price().await.map_err(|e| Self::l1_error(&e))?;
        let tip = l1
            .max_priority_fee()
            .await
            .map_err(|e| Self::l1_error(&e))?
            .saturating_to::<u128>();
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: L1_GAS,
            max_fee_per_gas: price.saturating_mul(2).saturating_add(tip),
            max_priority_fee_per_gas: tip,
            to: TxKind::Call(self.settings.lockbox),
            value: self.amount().await?,
            access_list: alloy_eips::eip2930::AccessList::default(),
            input: Self::deposit_input(self.ctx.ring.addresses()[0]),
        };
        let signed = self
            .settings
            .signer
            .sign_raw(tx)
            .map_err(|_| Outcome::L1Error)?;
        let entry = InFlight {
            nonce,
            hash: signed.hash,
            raw: signed.raw,
            published: false,
        };
        self.journal
            .store(&entry)
            .await
            .map_err(|_| Outcome::L1Error)?;
        Ok(entry)
    }

    /// The first deposit funds the ring: it is the larger amount while
    /// the first ring account holds less than it.
    async fn amount(&self) -> Result<U256, Outcome> {
        let first = self.ctx.ring.addresses()[0];
        let held = self.ctx.endpoint(0).balance(first).await?;
        Ok(if held < self.settings.first_amount {
            self.settings.first_amount
        } else {
            self.settings.amount
        })
    }

    /// `depositETH(to, gasLimit, "")`: the head words, then the empty
    /// tail.
    fn deposit_input(to: Address) -> Bytes {
        call(
            "depositETH(address,uint64,bytes)",
            &[
                address_word(to),
                U256::from(L2_GAS),
                U256::from(96),
                U256::ZERO,
            ],
        )
    }

    async fn forget(&self) {
        if let Err(e) = self.journal.clear().await {
            tracing::warn!(error = %e, "deposit: journal clear failed");
        }
    }

    /// Wait for the L1 receipt, then find the deposit's log.
    async fn included(&self, entry: &InFlight) -> Result<OnL1, Outcome> {
        let receipt = Poll::within(self.ctx.timing.l1_timeout, L1_POLL)
            .until(|| async { self.settings.l1.receipt(entry.hash).await.ok().flatten() })
            .await
            .ok_or(Outcome::Timeout(Stage::L1Inclusion))?;
        self.forget().await;
        if !receipt.succeeded() {
            return Err(Outcome::L1Error);
        }
        let topic = keccak256("DepositInitiated(uint64,address,address,uint256,uint64,bytes)");
        let log = receipt
            .logs
            .iter()
            .find(|log| log.address == self.settings.lockbox && log.topics.first() == Some(&topic))
            .ok_or(Outcome::L1Error)?;
        let index = log.log_index.ok_or(Outcome::L1Error)?.to::<u64>();
        let block = receipt.block_hash.ok_or(Outcome::L1Error)?;
        Ok(OnL1 {
            source: kardamom_types::epoch::source_hash(block, index),
            receipt,
        })
    }

    async fn finalized(&self, block: u64) -> Result<(), Outcome> {
        Poll::within(self.ctx.timing.l1_timeout, L1_POLL)
            .until(|| async {
                self.settings
                    .l1
                    .finalized()
                    .await
                    .ok()
                    .filter(|head| *head >= block)
            })
            .await
            .map(|_| ())
            .ok_or(Outcome::Timeout(Stage::L1Finality))
    }

    async fn credited(&self, source: B256) -> Result<(), Outcome> {
        let rpc = self.ctx.endpoint(0);
        Poll::within(self.ctx.timing.credit_timeout, L1_POLL)
            .until(|| async { rpc.receipt(source).await.ok().flatten() })
            .await
            .map(|_| ())
            .ok_or(Outcome::Timeout(Stage::L2Credit))
    }
}

impl Probe for Deposit {
    fn interval(&self) -> Duration {
        self.ctx.timing.deposit
    }

    async fn run(&mut self) {
        ::metrics::gauge!(metrics::BALANCE_FLOOR_WEI, "layer" => "l1")
            .set(metrics::wei(self.settings.floor));
        let outcome = self.attempt().await.err().unwrap_or(Outcome::Success);
        outcome.record(NAME, "l1");
    }
}
