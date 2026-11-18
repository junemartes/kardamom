//! A workload lands its setup transactions before its load starts. The
//! load calls the contracts that the setup creates and funds, so a call
//! that arrives before its setup would revert and spoil the verdict.

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use futures::{StreamExt as _, TryStreamExt as _};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;

use crate::load::hex_u64;
use crate::load::plan::PlannedTx;

/// The chains of one phase that land at the same time. Each chain holds
/// one submit or one receipt query in flight.
const CHAINS_IN_FLIGHT: usize = 128;

// This is a liveness bound, expressed as one. A setup can start the
// moment an earlier soak's verdict lands, so the chain is still draining
// that backlog, with the setup queued behind it. How long that takes is a
// property of the runner, not of the code under test. A fixed wall-clock
// deadline would race the drain instead.
//
// So a setup waits as long as the chain is advancing, and fails only when
// it stops. A stalled pipeline is caught in seconds; a merely slow one is
// waited out. The overall cap stays as a backstop against waiting forever
// on a chain that advances but never includes the transaction.
const STALL_LIMIT: Duration = Duration::from_mins(1);
const HARD_CAP: Duration = Duration::from_mins(10);

/// The poll interval of a receipt that has not landed.
const POLL: Duration = Duration::from_millis(200);

/// The transactions a workload lands before its load starts, in phases.
/// A phase starts after each transaction of the phase before it has a
/// successful receipt. A phase holds chains: each chain is the
/// transactions of one sender in nonce order, and the chains of a phase
/// land at the same time.
#[derive(Default)]
pub(crate) struct Setup {
    phases: Vec<Vec<Vec<PlannedTx>>>,
}

impl Setup {
    /// Add a phase of `chains` after the phases this setup has.
    pub(crate) fn phase(mut self, chains: Vec<Vec<PlannedTx>>) -> Self {
        self.phases.push(chains);
        self
    }

    /// Land each phase in order.
    ///
    /// # Errors
    ///
    /// Returns an error if a submit is rejected, a setup transaction
    /// reverts, the chain stops advancing while a transaction is not
    /// mined, or an accepted transaction is not included within the hard
    /// cap.
    pub(crate) async fn land(self, client: &HttpClient) -> anyhow::Result<()> {
        for (index, chains) in self.phases.iter().enumerate() {
            Phase {
                client,
                index,
                chains,
            }
            .land()
            .await?;
        }
        Ok(())
    }
}

/// One phase of a [`Setup`].
struct Phase<'a> {
    client: &'a HttpClient,
    index: usize,
    chains: &'a [Vec<PlannedTx>],
}

impl Phase<'_> {
    async fn land(self) -> anyhow::Result<()> {
        let started = Instant::now();
        // The futures go into a vector before the stream takes them. A
        // stream that maps with a closure here makes the future of a
        // whole run fail the `Send` bound of `tokio::spawn`.
        let lands: Vec<_> = self
            .chains
            .iter()
            .map(|txs| {
                Chain {
                    client: self.client,
                    txs,
                }
                .land()
            })
            .collect();
        futures::stream::iter(lands)
            .buffer_unordered(CHAINS_IN_FLIGHT)
            .try_collect::<Vec<()>>()
            .await?;
        tracing::info!(
            phase = self.index,
            chains = self.chains.len(),
            txs = self.chains.iter().map(Vec::len).sum::<usize>(),
            secs = format!("{:.1}", started.elapsed().as_secs_f64()),
            "setup phase landed"
        );
        Ok(())
    }
}

/// The setup transactions of one sender, in nonce order.
struct Chain<'a> {
    client: &'a HttpClient,
    txs: &'a [PlannedTx],
}

impl Chain<'_> {
    /// Submit each transaction in nonce order, then wait until each one
    /// has a successful receipt.
    async fn land(self) -> anyhow::Result<()> {
        for tx in self.txs {
            self.submit(tx).await?;
        }
        let started = Instant::now();
        for tx in self.txs {
            self.confirm(tx, started).await?;
        }
        Ok(())
    }

    /// Wait until `tx` has a successful receipt. `started` is the end of
    /// the submits of this chain.
    async fn confirm(&self, tx: &PlannedTx, started: Instant) -> anyhow::Result<()> {
        Confirm::new(self.client, tx, started).await.run().await
    }

    async fn submit(&self, tx: &PlannedTx) -> anyhow::Result<()> {
        let _: alloy_primitives::B256 = self
            .client
            .request("eth_sendRawTransaction", rpc_params![tx.raw.clone()])
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "setup submit (sender {} nonce {}): {e}",
                    tx.sender,
                    tx.nonce
                )
            })?;
        Ok(())
    }
}

/// The wait for the receipt of one setup transaction: the last head
/// block seen, and when it last changed.
struct Confirm<'a> {
    client: &'a HttpClient,
    tx: &'a PlannedTx,
    /// The start of the wait of the whole chain, for the hard cap.
    started: Instant,
    last_block: Option<u64>,
    last_progress: Instant,
}

impl<'a> Confirm<'a> {
    async fn new(client: &'a HttpClient, tx: &'a PlannedTx, started: Instant) -> Self {
        let mut confirm = Self {
            client,
            tx,
            started,
            last_block: None,
            last_progress: Instant::now(),
        };
        confirm.last_block = confirm.head_block().await;
        confirm
    }

    async fn head_block(&self) -> Option<u64> {
        self.client
            .request::<String, _>("eth_blockNumber", rpc_params![])
            .await
            .ok()
            .and_then(|h| hex_u64(&h))
    }

    /// Poll until the receipt lands, or fail if the chain stalls or the
    /// wait takes too long overall.
    async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let ControlFlow::Continue(()) = self.tick_or_wait().await? else {
                return Ok(());
            };
        }
    }

    /// One poll. Returns [`ControlFlow::Break`] once [`Self::tick`] sees
    /// the receipt land; otherwise sleeps the poll interval and returns
    /// [`ControlFlow::Continue`].
    async fn tick_or_wait(&mut self) -> anyhow::Result<ControlFlow<()>> {
        let cf = self.tick().await?;
        if cf.is_continue() {
            tokio::time::sleep(POLL).await;
        }
        Ok(cf)
    }

    /// Check for a landed receipt, else update the stall clock and check
    /// both deadlines.
    async fn tick(&mut self) -> anyhow::Result<ControlFlow<()>> {
        let (sender, nonce) = (self.tx.sender, self.tx.nonce);
        let v: Option<serde_json::Value> = self
            .client
            .request("eth_getTransactionReceipt", rpc_params![self.tx.hash])
            .await
            .unwrap_or(None);
        if let Some(r) = v {
            anyhow::ensure!(
                r["status"].as_str() == Some("0x1"),
                "setup tx reverted (sender {sender} nonce {nonce}): {r}"
            );
            return Ok(ControlFlow::Break(()));
        }
        let now = self.head_block().await;
        if now.is_some() && now != self.last_block {
            self.last_block = now;
            self.last_progress = Instant::now();
        }
        anyhow::ensure!(
            self.last_progress.elapsed() < STALL_LIMIT,
            "setup tx not mined (sender {sender} nonce {nonce}): chain STOPPED \
             advancing — no new block for {}s while waiting (head {:?})",
            STALL_LIMIT.as_secs(),
            self.last_block
        );
        anyhow::ensure!(
            self.started.elapsed() < HARD_CAP,
            "setup tx not mined (sender {sender} nonce {nonce}) within {}s although \
             the chain kept advancing to {:?} — the tx was accepted but never included",
            HARD_CAP.as_secs(),
            self.last_block
        );
        Ok(ControlFlow::Continue(()))
    }
}
