//! The transfers of the carry window scenarios.
//!
//! Each sender sends one transfer at a time, and the next one only after
//! the receipt of the last one. So a lost transaction stops its sender, and
//! the round fails. A submit that fails is sent again, byte for byte, until
//! the receipt exists or the budget ends: a client does the same after a
//! sequencer restart or a leader change. The load counts these resends.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use anyhow::{Context, Result};

use crate::harness::l2::{self, DerivedSigner, L2Client, SignedTransfer};

/// The recipient of every load transfer.
const PAYEE: Address = Address::repeat_byte(0xC7);

/// How long one transfer may take to get its receipt, resends included.
const LAND_BUDGET: Duration = Duration::from_mins(2);

/// The pause between two sends of one transfer.
const RESEND_PAUSE: Duration = Duration::from_millis(500);

/// One sender of the load, and the nonce of its next transfer.
struct Sender {
    signer: DerivedSigner,
    nonce: u64,
}

/// The load senders. Every transfer that a round sends gets its receipt
/// before the round ends.
pub(super) struct Load {
    rpc: L2Client,
    chain_id: u64,
    senders: Vec<Sender>,
    /// The transfers with a receipt.
    pub(super) landed: u64,
    /// The sends after the first send of a transfer.
    pub(super) resends: u64,
}

/// The outcome of one transfer of a round.
struct Landed {
    resends: u64,
}

impl Load {
    /// The load on the dev accounts `indexes`, which start at nonce 0.
    pub(super) fn new(
        rpc: L2Client,
        chain_id: u64,
        indexes: std::ops::RangeInclusive<usize>,
    ) -> Result<Self> {
        let signers = l2::dev_signers_through(*indexes.end())?;
        let senders = signers
            .into_iter()
            .skip(*indexes.start())
            .map(|signer| Sender { signer, nonce: 0 })
            .collect();
        Ok(Self {
            rpc,
            chain_id,
            senders,
            landed: 0,
            resends: 0,
        })
    }

    /// Send `rounds` rounds of transfers, one from each sender a round.
    ///
    /// # Errors
    /// Returns an error when a transfer gets no receipt within the budget.
    pub(super) async fn rounds(&mut self, rounds: u32) -> Result<()> {
        for _ in 0..rounds {
            self.round().await?;
        }
        Ok(())
    }

    /// One transfer from each sender, all at once.
    async fn round(&mut self) -> Result<()> {
        let txs = self
            .senders
            .iter()
            .map(|s| l2::sign_transfer(&s.signer, self.chain_id, s.nonce, PAYEE, 1))
            .collect::<Result<Vec<_>>>()?;
        let outcomes =
            futures::future::try_join_all(txs.iter().map(|tx| Self::land(&self.rpc, tx))).await?;
        self.senders
            .iter_mut()
            .for_each(|s| s.nonce = s.nonce.saturating_add(1));
        let sent = u64::try_from(txs.len()).context("count fits u64")?;
        self.landed = self.landed.saturating_add(sent);
        self.resends = outcomes
            .iter()
            .fold(self.resends, |sum, o| sum.saturating_add(o.resends));
        Ok(())
    }

    /// Send `tx` until its receipt exists, outside a round.
    ///
    /// # Errors
    /// Returns an error when `tx` gets no receipt within the budget.
    pub(super) async fn land_one(rpc: &L2Client, tx: &SignedTransfer) -> Result<()> {
        Self::land(rpc, tx).await.map(|_| ())
    }

    /// Send `tx` until its receipt exists.
    async fn land(rpc: &L2Client, tx: &SignedTransfer) -> Result<Landed> {
        let started = Instant::now();
        let mut sends = 0u64;
        while started.elapsed() < LAND_BUDGET {
            if Self::attempt(rpc, tx, &mut sends).await {
                return Ok(Landed {
                    resends: sends.saturating_sub(1),
                });
            }
        }
        anyhow::bail!(
            "nonce {} of {} got no receipt in {LAND_BUDGET:?} ({sends} sends)",
            tx.nonce,
            tx.sender
        )
    }

    /// One send of `tx`, counted in `sends`. True when its receipt exists;
    /// else wait before the next send. The answer of the send does not
    /// decide: a refused resend of a transaction that landed has a receipt
    /// too.
    async fn attempt(rpc: &L2Client, tx: &SignedTransfer, sends: &mut u64) -> bool {
        *sends = sends.saturating_add(1);
        let _answer = rpc.send_raw(&tx.raw).await;
        if Self::has_receipt(rpc, tx).await {
            return true;
        }
        tokio::time::sleep(RESEND_PAUSE).await;
        false
    }

    async fn has_receipt(rpc: &L2Client, tx: &SignedTransfer) -> bool {
        rpc.receipt(tx.hash).await.result.ok().flatten().is_some()
    }

    /// Run rounds on a task until [`Running::stop`].
    pub(super) fn start(self) -> Running {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let task = tokio::spawn(async move {
            let mut load = self;
            while !flag.load(Ordering::Relaxed) {
                load.round().await?;
            }
            Ok(load)
        });
        Running { stop, task }
    }
}

/// A load that runs on its own task.
pub(super) struct Running {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<Result<Load>>,
}

impl Running {
    /// Stop after the round that runs, and give the load back. Every
    /// transfer of that round has its receipt.
    ///
    /// # Errors
    /// Returns an error when a transfer got no receipt, or when the task
    /// panicked.
    pub(super) async fn stop(self) -> Result<Load> {
        self.stop.store(true, Ordering::Relaxed);
        self.task.await.context("load task join")?
    }
}
