//! The canary's L2 accounts. Each account has one nonce owner at a time:
//! a probe leases the account, and the lease is the only way to sign
//! with it. Before a lease signs a new nonce, it resolves the account's
//! in-flight transaction from the journal: a receipt or a committed
//! nonce past it settles it; otherwise the same signed bytes go out
//! again, and the account stays blocked while another account serves
//! the probe.
//!
//! The leases are per-account async mutexes taken with `try_lock`: the
//! probe tasks run on several threads and contend for the same
//! accounts, and a lease never waits for a busy account.

pub mod journal;

use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use alloy_consensus::TxEip1559;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use futures::{StreamExt, TryStreamExt};
pub use kardamom_bench::signers::DerivedSigner;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::time::Instant;

use crate::feed::BoardHandle;
use crate::outcome::Outcome;
use crate::rpc::{Rpc, RpcError};
use crate::wait::Poll;
use journal::{InFlight, Journal};

/// The rejection reasons of the sealer, which orders every transaction.
const SEALER_REFUSALS: [&str; 3] = ["past-deadline", "da-lag", "record-lag"];

/// The lowest fee cap the canary signs, in wei per gas: one gwei. A
/// chain with no fee schedule answers a zero gas price.
const MIN_FEE_CAP: u128 = 1_000_000_000;

/// What a transaction does: the target, the value, the input, the gas
/// limit, and the tip rate in wei per gas.
#[derive(Debug, Clone)]
pub struct Call {
    pub to: TxKind,
    pub value: U256,
    pub input: Bytes,
    pub gas_limit: u64,
    pub tip: u128,
}

impl Call {
    /// A call with no tip.
    #[must_use]
    pub fn new(to: TxKind, value: U256, input: Bytes, gas_limit: u64) -> Self {
        Self {
            to,
            value,
            input,
            gas_limit,
            tip: 0,
        }
    }
}

/// A transaction the ingress published.
#[derive(Debug, Clone, Copy)]
pub struct Sent {
    pub hash: B256,
    pub nonce: u64,
    /// When the submit started.
    pub at: Instant,
    /// The submit's round trip: submit to hash.
    pub took: Duration,
}

/// The signers of the derivation indices `first..first + count` of
/// `phrase`: the ring.
///
/// # Errors
///
/// Returns an error when the range overflows or the phrase does not
/// derive.
pub fn signers(phrase: &str, first: u32, count: NonZeroU32) -> anyhow::Result<Vec<DerivedSigner>> {
    let end = first
        .checked_add(count.get())
        .ok_or_else(|| anyhow::anyhow!("the ring's derivation range overflows"))?;
    kardamom_bench::mnemonic::derive_range(phrase, first..end)
}

/// One account's state, owned by its lease.
#[derive(Debug)]
struct Account {
    address: Address,
    next_nonce: Option<u64>,
    in_flight: Option<InFlight>,
    journal: Journal,
}

#[derive(Debug)]
struct Slot {
    signer: DerivedSigner,
    funded: AtomicBool,
    account: Arc<Mutex<Account>>,
}

/// Why one account could not serve a lease.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Miss {
    Unfunded,
    Busy,
    /// The account holds an unresolved transaction.
    Blocked,
    /// A call failed while the account was checked.
    Rpc(Outcome),
}

/// The misses of one lease attempt over the ring.
#[derive(Debug, Default)]
struct Misses(Vec<Miss>);

impl Misses {
    fn all_busy(&self) -> bool {
        self.0.iter().all(|m| *m == Miss::Busy)
    }

    fn with(mut self, miss: Miss) -> Self {
        self.0.push(miss);
        self
    }

    /// The outcome a user would see: the first call failure, else
    /// `unfunded` when no account has funds, else `account_stalled`.
    fn outcome(self) -> Outcome {
        let all_unfunded = self.0.iter().all(|m| *m == Miss::Unfunded);
        self.0
            .into_iter()
            .find_map(|m| match m {
                Miss::Rpc(outcome) => Some(outcome),
                _ => None,
            })
            .unwrap_or(if all_unfunded {
                Outcome::Unfunded
            } else {
                Outcome::AccountStalled
            })
    }
}

/// The ring of L2 accounts.
#[derive(Debug)]
pub struct Ring {
    slots: Vec<Arc<Slot>>,
    chain_id: u64,
    cursor: AtomicUsize,
    board: BoardHandle,
}

impl Ring {
    /// The ring of `signers` on chain `chain_id`, with each account's
    /// journal under `dir`. `board` holds the status feed's rejections.
    /// Every account starts funded; the balance task corrects that.
    ///
    /// # Errors
    ///
    /// Returns an error when `dir` cannot be made or a journal does not
    /// read.
    pub async fn open(
        dir: &Path,
        signers: Vec<DerivedSigner>,
        chain_id: u64,
        board: BoardHandle,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(dir).await?;
        let slots = futures::stream::iter(signers)
            .then(|signer| Self::slot(dir, signer))
            .try_collect()
            .await?;
        Ok(Self {
            slots,
            chain_id,
            cursor: AtomicUsize::new(0),
            board,
        })
    }

    async fn slot(dir: &Path, signer: DerivedSigner) -> anyhow::Result<Arc<Slot>> {
        let journal = Journal::new(dir, signer.address);
        let in_flight = journal.load().await?;
        Ok(Arc::new(Slot {
            funded: AtomicBool::new(true),
            account: Arc::new(Mutex::new(Account {
                address: signer.address,
                next_nonce: None,
                in_flight,
                journal,
            })),
            signer,
        }))
    }

    /// The ring's addresses, in derivation order.
    #[must_use]
    pub fn addresses(&self) -> Vec<Address> {
        self.slots.iter().map(|s| s.signer.address).collect()
    }

    /// Mark account `index` funded or not. An unfunded account serves
    /// no lease.
    pub fn set_funded(&self, index: usize, funded: bool) {
        if let Some(slot) = self.slots.get(index) {
            slot.funded.store(funded, Ordering::Relaxed);
        }
    }

    /// Lease the first account, in turn from a rotating start, that is
    /// funded, free, and holds no unresolved transaction. The checks of
    /// an account call `rpc`. While every miss is an account in use by
    /// another probe, the lease waits for one, up to `wait`.
    ///
    /// # Errors
    ///
    /// The outcome a user would see when no account can send.
    pub async fn lease(&self, rpc: &Rpc, wait: Poll) -> Result<Lease, Outcome> {
        wait.until(|| async {
            match self.lease_once(rpc).await {
                Err(misses) if misses.all_busy() => None,
                tried => Some(tried.map_err(Misses::outcome)),
            }
        })
        .await
        .unwrap_or(Err(Outcome::AccountStalled))
    }

    /// Lease ring account `index`, waiting up to `wait` while another
    /// probe holds it.
    ///
    /// # Errors
    ///
    /// The outcome a user would see when the account cannot send.
    pub async fn lease_index(&self, index: usize, rpc: &Rpc, wait: Poll) -> Result<Lease, Outcome> {
        let index = index % self.slots.len();
        wait.until(|| async {
            match self.try_slot(index, rpc).await {
                Err(Miss::Busy) => None,
                tried => Some(tried.map_err(|miss| Misses::default().with(miss).outcome())),
            }
        })
        .await
        .unwrap_or(Err(Outcome::AccountStalled))
    }

    /// The number of ring accounts.
    #[must_use]
    pub fn size(&self) -> usize {
        self.slots.len()
    }

    async fn lease_once(&self, rpc: &Rpc) -> Result<Lease, Misses> {
        let count = self.slots.len();
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        // A lease stops the fold; a miss joins the misses.
        let attempt = futures::stream::iter(0..count)
            .then(|offset| self.try_slot(start.wrapping_add(offset) % count, rpc))
            .map(|tried| tried.map_or_else(Ok, Err))
            .try_fold(Misses::default(), |misses, miss| async move {
                Ok(misses.with(miss))
            })
            .await;
        attempt.map_or_else(Ok, Err)
    }

    async fn try_slot(&self, index: usize, rpc: &Rpc) -> Result<Lease, Miss> {
        let slot = &self.slots[index];
        if !slot.funded.load(Ordering::Relaxed) {
            return Err(Miss::Unfunded);
        }
        let account = Arc::clone(&slot.account)
            .try_lock_owned()
            .map_err(|_| Miss::Busy)?;
        let mut lease = Lease {
            slot: Arc::clone(slot),
            chain_id: self.chain_id,
            account,
            gap: false,
            board: self.board.clone(),
        };
        lease.prepare(rpc).await?;
        Ok(lease)
    }
}

/// The sole right to sign with one account, until it drops.
#[derive(Debug)]
pub struct Lease {
    slot: Arc<Slot>,
    chain_id: u64,
    account: OwnedMutexGuard<Account>,
    gap: bool,
    board: BoardHandle,
}

impl Lease {
    /// The leased account's address.
    #[must_use]
    pub fn address(&self) -> Address {
        self.account.address
    }

    /// Whether the lease found the chain's nonce past the canary's.
    #[must_use]
    pub fn found_gap(&self) -> bool {
        self.gap
    }

    /// Resolve the in-flight transaction, then take the committed nonce
    /// when it is ahead of the canary's.
    async fn prepare(&mut self, rpc: &Rpc) -> Result<(), Miss> {
        self.reconcile(rpc).await?;
        let committed = rpc
            .nonce(self.address())
            .await
            .map_err(|e| Miss::Rpc(e.into()))?;
        let next = self.account.next_nonce.unwrap_or(committed);
        self.gap = committed > next;
        self.account.next_nonce = Some(next.max(committed));
        Ok(())
    }

    /// Settle the in-flight transaction when the chain holds it; else send
    /// the same bytes again. A transaction that stays unresolved blocks
    /// the account.
    async fn reconcile(&mut self, rpc: &Rpc) -> Result<(), Miss> {
        let Some(entry) = self.account.in_flight.clone() else {
            return Ok(());
        };
        let landed = rpc
            .receipt(entry.hash)
            .await
            .map_err(|e| Miss::Rpc(e.into()))?;
        let used = rpc
            .nonce(self.address())
            .await
            .map_err(|e| Miss::Rpc(e.into()))?;
        if landed.is_some() || used > entry.nonce {
            self.settle_entry(entry.nonce).await;
            return Ok(());
        }
        if self.sealer_refused(entry.hash).await {
            self.account.next_nonce = Some(entry.nonce);
            self.forget().await;
            crate::metrics::stalled(self.address(), None);
            return Ok(());
        }
        self.resend(rpc, entry).await
    }

    /// Whether the status feed reported that the sealer refused `hash`.
    /// The sealer is the one orderer and drops a hash it saw again, so a
    /// refused transaction never lands, and its nonce is free once the
    /// committed nonce has not passed it.
    async fn sealer_refused(&self, hash: B256) -> bool {
        let reason = self.board.stages(hash).await.rejected;
        reason.is_some_and(|r| SEALER_REFUSALS.contains(&r.as_str()))
    }

    /// Send an unresolved transaction again. A refusal of one that never
    /// reached the chain frees its nonce; any other answer keeps the
    /// account blocked on it.
    async fn resend(&mut self, rpc: &Rpc, entry: InFlight) -> Result<(), Miss> {
        let answer = rpc.send(&entry.raw).await;
        if matches!(answer, Err(RpcError::Refused { .. })) && !entry.published {
            self.account.next_nonce = Some(entry.nonce);
            self.forget().await;
            return Ok(());
        }
        self.mark_published(entry).await;
        crate::metrics::stalled(self.address(), Some(self.account.in_flight_nonce()));
        Err(answer.map_or_else(|e| Miss::Rpc(e.into()), |_| Miss::Blocked))
    }

    /// Sign `call` at the next nonce, write it to the journal, and submit
    /// it to `rpc`.
    ///
    /// # Errors
    ///
    /// The outcome a user would see. A refused submit frees the nonce; a
    /// submit with no answer leaves the transaction in flight.
    pub async fn send(&mut self, rpc: &Rpc, call: Call) -> Result<Sent, Outcome> {
        let price = rpc.gas_price().await?;
        let nonce = self.account.next_nonce.ok_or(Outcome::NonceGap)?;
        let tx = TxEip1559 {
            chain_id: self.chain_id,
            nonce,
            gas_limit: call.gas_limit,
            max_fee_per_gas: price
                .saturating_mul(2)
                .max(MIN_FEE_CAP)
                .saturating_add(call.tip),
            max_priority_fee_per_gas: call.tip,
            to: call.to,
            value: call.value,
            access_list: alloy_eips::eip2930::AccessList::default(),
            input: call.input,
        };
        let signed = self
            .slot
            .signer
            .sign_raw(tx)
            .map_err(|_| Outcome::RpcError("sign".to_string()))?;
        let entry = InFlight {
            nonce,
            hash: signed.hash,
            raw: signed.raw,
            published: false,
        };
        self.account
            .journal
            .store(&entry)
            .await
            .map_err(|_| Outcome::RpcError("journal".to_string()))?;
        self.account.in_flight = Some(entry.clone());
        let at = Instant::now();
        let answer = rpc.send(&entry.raw).await;
        let took = at.elapsed();
        match answer {
            Ok(_) => {
                self.mark_published(entry).await;
                Ok(Sent {
                    hash: signed.hash,
                    nonce,
                    at,
                    took,
                })
            }
            Err(e @ RpcError::Refused { .. }) => {
                self.forget().await;
                Err(e.into())
            }
            Err(e) => {
                self.mark_published(entry).await;
                Err(e.into())
            }
        }
    }

    /// The in-flight transaction landed: its nonce is used.
    pub async fn settle(mut self) {
        let nonce = self.account.in_flight_nonce();
        self.settle_entry(nonce).await;
    }

    async fn settle_entry(&mut self, nonce: u64) {
        self.account.next_nonce = Some(nonce.saturating_add(1));
        self.forget().await;
        crate::metrics::stalled(self.address(), None);
    }

    /// Drop the in-flight transaction from memory and the journal. A
    /// journal that does not clear keeps the entry, which a later lease
    /// resolves again.
    async fn forget(&mut self) {
        if let Err(e) = self.account.journal.clear().await {
            tracing::warn!(account = %self.address(), error = %e, "journal clear failed");
            return;
        }
        self.account.in_flight = None;
    }

    async fn mark_published(&mut self, mut entry: InFlight) {
        entry.published = true;
        if let Err(e) = self.account.journal.store(&entry).await {
            tracing::warn!(account = %self.address(), error = %e, "journal write failed");
        }
        self.account.in_flight = Some(entry);
    }
}

impl Account {
    /// The nonce of the in-flight transaction, or the next nonce when
    /// there is none.
    fn in_flight_nonce(&self) -> u64 {
        self.in_flight
            .as_ref()
            .map(|f| f.nonce)
            .or(self.next_nonce)
            .unwrap_or(0)
    }
}
