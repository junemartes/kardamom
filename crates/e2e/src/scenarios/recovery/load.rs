//! The case load: one sender submits 1-wei transfers through one
//! ingress, a few nonces in flight, for the whole case. A failed submit
//! tries again with the same signed bytes, as a client does. Before it
//! tries again, it asks every ingress for the receipt: the failed submit
//! can have landed, for example when its ingress died after the publish.
//!
//! The report judges the load the way the chaos load verdict does, and
//! more strictly: every offered transfer must land, the chain must hold
//! exactly one receipt for each, in nonce order, and the payee balance
//! must equal the count of landed transfers.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256};
use anyhow::{Context, Result};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::harness::l2::{self, DerivedSigner, L2Client, SignedTransfer};
use crate::scenarios::{Placement, assert_receipt_ok, receipt_placement};

/// The pause between two receipt lookups while a submit waits.
const LOOKUP_EVERY: Duration = Duration::from_secs(1);

/// The settings of one load.
#[derive(Clone)]
pub struct LoadPlan {
    pub sender: DerivedSigner,
    /// Every transfer pays 1 wei to this address. Each load has its own
    /// payee, so the payee balance counts the landed transfers.
    pub payee: Address,
    pub chain_id: u64,
    /// The ingress that takes the submits.
    pub submit: L2Client,
    /// Every ingress, for the receipt of a submit that failed.
    pub receipts: Vec<L2Client>,
    pub first_nonce: u64,
    /// The nonces in flight at once.
    pub window: NonZeroUsize,
    /// The pause before the next new nonce.
    pub pace: Duration,
    /// The pause before a failed submit tries again.
    pub retry: Duration,
    /// How long one nonce may take before the load gives up on it.
    pub give_up: Duration,
}

/// How a transfer landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    /// The submit answered with the transfer's hash.
    Submit,
    /// A submit failed, and an ingress served the receipt.
    Lookup,
}

/// What happened to one nonce.
#[derive(Debug, Clone)]
pub struct NonceOutcome {
    pub tx: SignedTransfer,
    pub attempts: u32,
    /// `None` when the load gave up on the nonce.
    pub via: Option<Via>,
    /// The last error a submit of this nonce saw.
    pub last_error: Option<String>,
    /// From the first submit until the transfer landed or the load gave
    /// up.
    pub took: Duration,
}

/// The state that the nonce tasks share with the load.
struct Shared {
    plan: LoadPlan,
    landed: AtomicU64,
}

impl Shared {
    /// Submit `tx` until it lands or the nonce bound passes.
    async fn deliver(self: Arc<Self>, tx: SignedTransfer) -> NonceOutcome {
        let started = Instant::now();
        let deadline = started + self.plan.give_up;
        let mut outcome = NonceOutcome {
            tx,
            attempts: 0,
            via: None,
            last_error: None,
            took: Duration::ZERO,
        };
        while outcome.via.is_none() && Instant::now() < deadline {
            self.attempt(&mut outcome).await;
        }
        outcome.took = started.elapsed();
        if outcome.via.is_some() {
            self.landed.fetch_add(1, Ordering::AcqRel);
        }
        outcome
    }

    /// One submit, and on its failure one receipt lookup and the pause.
    /// While the submit waits, the receipt lookup runs every
    /// [`LOOKUP_EVERY`]: a resubmit of a transfer that already landed can
    /// park at its ingress until the park bound, and a client that reads
    /// receipts from every ingress learns of the landing first.
    async fn attempt(&self, outcome: &mut NonceOutcome) {
        outcome.attempts = outcome.attempts.saturating_add(1);
        let sent = tokio::select! {
            sent = self.plan.submit.send_raw(&outcome.tx.raw) => sent,
            via = self.watch_receipts(&outcome.tx) => {
                outcome.via = Some(via);
                return;
            }
        };
        outcome.via = match sent.result {
            Ok(hash) if hash == outcome.tx.hash => Some(Via::Submit),
            Ok(hash) => {
                outcome.last_error = Some(format!("the submit answered another hash {hash}"));
                None
            }
            Err(e) => {
                outcome.last_error = Some(e.to_string());
                self.lookup(&outcome.tx).await
            }
        };
        if outcome.via.is_none() {
            tokio::time::sleep(self.plan.retry).await;
        }
    }

    /// Poll every ingress for the receipt of `tx` until one serves it.
    async fn watch_receipts(&self, tx: &SignedTransfer) -> Via {
        loop {
            if let Some(via) = self.lookup_after_pause(tx).await {
                return via;
            }
        }
    }

    async fn lookup_after_pause(&self, tx: &SignedTransfer) -> Option<Via> {
        tokio::time::sleep(LOOKUP_EVERY).await;
        self.lookup(tx).await
    }

    /// Whether any ingress serves a receipt for `tx`.
    async fn lookup(&self, tx: &SignedTransfer) -> Option<Via> {
        let reads =
            futures::future::join_all(self.plan.receipts.iter().map(|c| c.receipt(tx.hash))).await;
        reads
            .into_iter()
            .any(|r| matches!(r.result, Ok(Some(_))))
            .then_some(Via::Lookup)
    }
}

/// The driver of a running load: it keeps `window` nonces in flight
/// until the stop signal, then waits for the nonces in flight.
struct Driver {
    shared: Arc<Shared>,
    stop: watch::Receiver<bool>,
    next: u64,
    tasks: JoinSet<NonceOutcome>,
    outcomes: Vec<NonceOutcome>,
}

impl Driver {
    async fn run(mut self) -> Result<LoadReport> {
        while self.step().await? {}
        self.outcomes.sort_by_key(|o| o.tx.nonce);
        Ok(LoadReport {
            plan: self.shared.plan.clone(),
            outcomes: self.outcomes,
        })
    }

    /// One turn: start a nonce while the window has room and the load
    /// runs, else collect one finished nonce. Returns false at the end.
    async fn step(&mut self) -> Result<bool> {
        let running = !*self.stop.borrow();
        if running && self.tasks.len() < self.shared.plan.window.get() {
            self.spawn_next()?;
            tokio::time::sleep(self.shared.plan.pace).await;
            return Ok(true);
        }
        match self.tasks.join_next().await {
            Some(done) => {
                self.outcomes.push(done.context("a nonce task failed")?);
                Ok(true)
            }
            None => Ok(running),
        }
    }

    fn spawn_next(&mut self) -> Result<()> {
        let plan = &self.shared.plan;
        let tx = l2::sign_transfer(&plan.sender, plan.chain_id, self.next, plan.payee, 1)?;
        self.next = self.next.checked_add(1).context("load nonce overflows")?;
        self.tasks.spawn(self.shared.clone().deliver(tx));
        Ok(())
    }
}

/// A load in progress.
pub struct RunningLoad {
    shared: Arc<Shared>,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<LoadReport>>,
}

impl RunningLoad {
    /// Start the load on the current runtime.
    #[must_use]
    pub fn start(plan: LoadPlan) -> Self {
        let (stop, stop_rx) = watch::channel(false);
        let next = plan.first_nonce;
        let shared = Arc::new(Shared {
            plan,
            landed: AtomicU64::new(0),
        });
        let driver = Driver {
            shared: shared.clone(),
            stop: stop_rx,
            next,
            tasks: JoinSet::new(),
            outcomes: Vec::new(),
        };
        Self {
            shared,
            stop,
            task: tokio::spawn(driver.run()),
        }
    }

    /// The transfers landed so far.
    #[must_use]
    pub fn landed(&self) -> u64 {
        self.shared.landed.load(Ordering::Acquire)
    }

    /// Wait until more than `floor` transfers landed: the load flows.
    ///
    /// # Errors
    /// Returns an error when no transfer past `floor` lands within
    /// `bound`.
    pub async fn await_landed_above(&self, floor: u64, bound: Duration) -> Result<Duration> {
        let started = Instant::now();
        crate::harness::metrics::poll_until(
            &format!("the case load lands a transfer past {floor}"),
            bound,
            Duration::from_millis(50),
            async || Ok((self.landed() > floor).then_some(())),
        )
        .await?;
        Ok(started.elapsed())
    }

    /// Stop new nonces, and wait for the nonces in flight.
    ///
    /// # Errors
    /// Returns an error when the load task fails.
    pub async fn finish(self) -> Result<LoadReport> {
        self.stop.send_replace(true);
        self.task.await.context("the load task panicked")?
    }
}

/// The result of one load.
pub struct LoadReport {
    plan: LoadPlan,
    pub outcomes: Vec<NonceOutcome>,
}

impl LoadReport {
    /// The transfers that landed.
    #[must_use]
    pub fn landed(&self) -> usize {
        self.outcomes.iter().filter(|o| o.via.is_some()).count()
    }

    /// The submits that the load sent, over every nonce.
    #[must_use]
    pub fn attempts(&self) -> u64 {
        self.outcomes.iter().map(|o| u64::from(o.attempts)).sum()
    }

    /// The nonce after the last offered one.
    #[must_use]
    pub fn end_nonce(&self) -> u64 {
        self.outcomes
            .last()
            .map_or(self.plan.first_nonce, |o| o.tx.nonce.saturating_add(1))
    }

    /// No loss: every offered transfer landed.
    ///
    /// # Errors
    /// Returns an error naming each nonce that did not land.
    pub fn assert_all_landed(&self, what: &str) -> Result<()> {
        let lost: Vec<String> = self
            .outcomes
            .iter()
            .filter(|o| o.via.is_none())
            .map(|o| {
                format!(
                    "nonce {} after {} attempts ({})",
                    o.tx.nonce,
                    o.attempts,
                    o.last_error.as_deref().unwrap_or("no error")
                )
            })
            .collect();
        anyhow::ensure!(
            lost.is_empty(),
            "{what}: {} of {} offered transfers never landed: {}",
            lost.len(),
            self.outcomes.len(),
            lost.join("; ")
        );
        anyhow::ensure!(
            !self.outcomes.is_empty(),
            "{what}: the load offered nothing"
        );
        Ok(())
    }

    /// The chain holds every landed transfer exactly once, in nonce
    /// order, as every ingress in `readers` serves it:
    /// - each receipt names the transfer's hash and succeeded;
    /// - the placements rise with the nonce;
    /// - the sender's nonce is the end of the load;
    /// - the payee holds one wei per landed transfer.
    ///
    /// # Errors
    /// Returns an error naming the first broken property.
    pub async fn assert_on_chain(&self, what: &str, readers: &[L2Client]) -> Result<()> {
        for reader in readers {
            self.assert_served_by(what, reader).await?;
        }
        Ok(())
    }

    async fn assert_served_by(&self, what: &str, reader: &L2Client) -> Result<()> {
        let mut placements = Vec::with_capacity(self.outcomes.len());
        for outcome in &self.outcomes {
            placements.push(Self::placement(what, reader, &outcome.tx).await?);
        }
        let disorder: Vec<String> = placements
            .windows(2)
            .zip(&self.outcomes)
            .filter(|(pair, _)| (pair[1].block, pair[1].index) <= (pair[0].block, pair[0].index))
            .map(|(pair, o)| format!("nonce {} at {:?} then {:?}", o.tx.nonce, pair[0], pair[1]))
            .collect();
        anyhow::ensure!(
            disorder.is_empty(),
            "{what}: {} places the sender out of nonce order: {}",
            reader.url,
            disorder.join("; ")
        );
        let count = Self::hex_u64(
            reader
                .get_transaction_count(self.plan.sender.address)
                .await
                .result,
        )?;
        anyhow::ensure!(
            count == self.end_nonce(),
            "{what}: {} reports the sender's nonce {count}, the load ended at {}",
            reader.url,
            self.end_nonce()
        );
        let paid = Self::hex_u256(reader.get_balance(self.plan.payee).await.result)?;
        anyhow::ensure!(
            paid == U256::from(self.landed()),
            "{what}: {} reports the payee balance {paid}, but {} transfers landed: a transfer is lost or doubled",
            reader.url,
            self.landed()
        );
        Ok(())
    }

    /// The receipt of `tx` as `reader` serves it, waited for briefly: a
    /// restarted ingress fills its cache from the executor.
    async fn placement(what: &str, reader: &L2Client, tx: &SignedTransfer) -> Result<Placement> {
        let receipt = crate::harness::metrics::poll_until(
            &format!(
                "{what}: {} serves the receipt of nonce {}",
                reader.url, tx.nonce
            ),
            Duration::from_secs(10),
            Duration::from_millis(100),
            async || Ok(reader.receipt(tx.hash).await.result.ok().flatten()),
        )
        .await?;
        assert_receipt_ok(&receipt, &format!("{what}: nonce {}", tx.nonce))?;
        let named = crate::scenarios::receipt_field(&receipt, "transactionHash");
        anyhow::ensure!(
            named == Some(&format!("{}", tx.hash)),
            "{what}: the receipt of nonce {} names {named:?}, not {}",
            tx.nonce,
            tx.hash
        );
        receipt_placement(&receipt)
    }

    fn hex_u64(read: std::result::Result<String, l2::RpcError>) -> Result<u64> {
        let text = read.map_err(|e| anyhow::anyhow!("account read failed: {e}"))?;
        u64::from_str_radix(text.trim_start_matches("0x"), 16)
            .with_context(|| format!("parse {text:?}"))
    }

    fn hex_u256(read: std::result::Result<String, l2::RpcError>) -> Result<U256> {
        let text = read.map_err(|e| anyhow::anyhow!("balance read failed: {e}"))?;
        U256::from_str_radix(text.trim_start_matches("0x"), 16)
            .with_context(|| format!("parse {text:?}"))
    }

    /// One line for the test log.
    #[must_use]
    pub fn summary(&self) -> String {
        let looked_up = self
            .outcomes
            .iter()
            .filter(|o| o.via == Some(Via::Lookup))
            .count();
        let slowest = self.outcomes.iter().max_by_key(|o| o.took);
        format!(
            "{} offered, {} landed ({looked_up} through a receipt lookup), {} submits, slowest {}",
            self.outcomes.len(),
            self.landed(),
            self.attempts(),
            slowest.map_or_else(String::new, |o| format!(
                "nonce {} in {:.1}s ({:?})",
                o.tx.nonce,
                o.took.as_secs_f64(),
                o.last_error
            ))
        )
    }
}
