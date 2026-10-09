//! The market task: the test RWA token, the pool, and their probes
//! (`rwa`, `swap`, `liquidity`). One task owns the token and the pool, so
//! a probe's reserve snapshot, its transaction and its check never
//! interleave with another canary probe, and the supply ledger has one
//! writer. Each probe runs on its own timer inside the task.
//!
//! At its first start the task sets the market up, one step at a time
//! with its progress in the data directory: it deploys the token and the
//! pool, puts the pool on the allowlist, mints 1,000,000 tokens to the
//! first ring account, approves the pool for every ring account, adds
//! 0.002 ETH and 100,000 tokens as the first liquidity, and moves a stash
//! of tokens to the swapper.

mod pool;
mod rwa;

use std::sync::Arc;

use alloy_primitives::{Address, Bytes, TxKind, U256};
use tokio::time::{Instant, Interval, MissedTickBehavior};

use crate::contracts::{CALL_GAS, DEPLOY_GAS, Pool, Reserves, Rwa};
use crate::outcome::Outcome;
use crate::probes::{Context, Done};
use crate::ring::Call;
use crate::rpc::Rpc;
use crate::store::{Market, SupplyChange};

/// One token: 10^18 units.
fn tokens(n: u64) -> U256 {
    U256::from(n).saturating_mul(U256::from(10).pow(U256::from(18)))
}

/// The ring account that holds the minted supply and the pool's first
/// liquidity, and owns the token.
const OWNER: usize = 0;
/// The ring accounts of the `rwa` probe's transfers.
const HOLDER: usize = 1;
const RECEIVER: usize = 2;
/// The ring account of the swaps.
const SWAPPER: usize = 3;

/// The setup steps, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    DeployToken,
    DeployPool,
    AllowPool,
    MintSupply,
    Approve(usize),
    AddLiquidity,
    Stash,
}

/// The three probes of the task.
#[derive(Debug, Clone, Copy)]
enum Job {
    Rwa,
    Swap,
    Liquidity,
}

impl Job {
    fn name(self) -> &'static str {
        match self {
            Self::Rwa => "rwa",
            Self::Swap => "swap",
            Self::Liquidity => "liquidity",
        }
    }
}

/// The three timers.
struct Ticks {
    rwa: Interval,
    swap: Interval,
    liquidity: Interval,
}

impl Ticks {
    /// The timers of `ctx`. The first `liquidity` run waits one swap
    /// interval, so a swap shows the reserves first.
    fn new(ctx: &Context) -> Self {
        let timer = |every, first: Instant| {
            let mut t = tokio::time::interval_at(first, every);
            t.set_missed_tick_behavior(MissedTickBehavior::Skip);
            t
        };
        let now = Instant::now();
        Self {
            rwa: timer(ctx.timing.rwa, now),
            swap: timer(ctx.timing.swap, now),
            liquidity: timer(ctx.timing.liquidity, now + ctx.timing.swap),
        }
    }

    async fn next(&mut self) -> Job {
        tokio::select! {
            _ = self.rwa.tick() => Job::Rwa,
            _ = self.swap.tick() => Job::Swap,
            _ = self.liquidity.tick() => Job::Liquidity,
        }
    }
}

/// The market task.
pub struct MarketTask {
    ctx: Arc<Context>,
    state: Market,
    /// The pool's reserves and sequence after the last canary call.
    last: Option<(Reserves, U256)>,
    /// The direction of the next swap.
    eth_in_next: bool,
    turn: usize,
}

impl MarketTask {
    /// The task, with the state the data directory holds.
    ///
    /// # Errors
    ///
    /// Returns an error when the state file does not parse.
    pub async fn new(ctx: Arc<Context>) -> anyhow::Result<Self> {
        let state = ctx.store.market().await?;
        Ok(Self {
            ctx,
            state,
            last: None,
            eth_in_next: true,
            turn: 0,
        })
    }

    /// Run the probes on their timers, for ever.
    pub async fn run(mut self) {
        let mut ticks = Ticks::new(&self.ctx);
        loop {
            let job = ticks.next().await;
            self.run_job(job).await;
        }
    }

    async fn run_job(&mut self, job: Job) {
        let rpc = self.ctx.endpoint(self.turn).clone();
        self.turn = self.turn.wrapping_add(1);
        let outcome = match self.ready(&rpc).await {
            Err(outcome) => outcome,
            Ok((token, pool)) => self.probe(job, &rpc, token, pool).await,
        };
        outcome.record(job.name(), &rpc.endpoint.name);
    }

    async fn probe(&mut self, job: Job, rpc: &Rpc, token: Rwa, pool: Pool) -> Outcome {
        let done = match job {
            Job::Rwa => self.rwa(rpc, token).await,
            Job::Swap => self.swap(rpc, pool).await,
            Job::Liquidity => self.liquidity(rpc, pool).await,
        };
        done.err().unwrap_or(Outcome::Success)
    }

    /// The token and the pool, after the setup steps not done yet.
    async fn ready(&mut self, rpc: &Rpc) -> Result<(Rwa, Pool), Outcome> {
        let steps = self.steps();
        let todo = steps
            .get(usize::from(self.state.setup)..)
            .unwrap_or_default();
        for step in todo {
            self.setup(*step, rpc).await?;
        }
        Ok((
            Rwa(self.state.rwa.ok_or(Outcome::StateMismatch)?),
            Pool(self.state.pool.ok_or(Outcome::StateMismatch)?),
        ))
    }

    fn steps(&self) -> Vec<Step> {
        [
            Step::DeployToken,
            Step::DeployPool,
            Step::AllowPool,
            Step::MintSupply,
        ]
        .into_iter()
        .chain((0..self.ctx.ring.size()).map(Step::Approve))
        .chain([Step::AddLiquidity, Step::Stash])
        .collect()
    }

    /// Run one setup step and store the progress.
    async fn setup(&mut self, step: Step, rpc: &Rpc) -> Result<(), Outcome> {
        tracing::info!(?step, "market setup");
        self.setup_step(step, rpc).await?;
        self.state.setup = self.state.setup.saturating_add(1);
        self.save().await;
        Ok(())
    }

    async fn setup_step(&mut self, step: Step, rpc: &Rpc) -> Result<(), Outcome> {
        let ring = self.ctx.ring.addresses();
        let owner = ring[OWNER];
        match step {
            Step::DeployToken => {
                let done = self
                    .send(
                        rpc,
                        OWNER,
                        Call::new(TxKind::Create, U256::ZERO, Rwa::creation(&ring), DEPLOY_GAS),
                    )
                    .await?;
                self.state.rwa = Some(Self::created(&done)?);
            }
            Step::DeployPool => {
                let token = self.state.rwa.ok_or(Outcome::StateMismatch)?;
                let done = self
                    .send(
                        rpc,
                        OWNER,
                        Call::new(
                            TxKind::Create,
                            U256::ZERO,
                            Pool::creation(token),
                            DEPLOY_GAS,
                        ),
                    )
                    .await?;
                self.state.pool = Some(Self::created(&done)?);
            }
            Step::AllowPool => {
                let pool = self.state.pool.ok_or(Outcome::StateMismatch)?;
                self.token_call(rpc, OWNER, Rwa::set_allowed(pool)).await?;
            }
            Step::MintSupply => {
                let token = Rwa(self.state.rwa.ok_or(Outcome::StateMismatch)?);
                self.supply_change(rpc, token, owner, SupplyChange::Mint(tokens(1_000_000)))
                    .await?;
            }
            Step::Approve(index) => {
                let pool = self.state.pool.ok_or(Outcome::StateMismatch)?;
                self.token_call(rpc, index, Rwa::approve(pool)).await?;
            }
            Step::AddLiquidity => {
                let pool = Pool(self.state.pool.ok_or(Outcome::StateMismatch)?);
                let eth = U256::from(2_000_000_000_000_000u64);
                self.add_liquidity(rpc, pool, eth, tokens(100_000)).await?;
            }
            Step::Stash => {
                let swapper = ring[SWAPPER % ring.len()];
                self.token_call(rpc, OWNER, Rwa::transfer(swapper, tokens(1_000)))
                    .await?;
            }
        }
        Ok(())
    }

    /// The address a deploy created.
    fn created(done: &Done) -> Result<Address, Outcome> {
        done.landed
            .receipt
            .contract_address
            .ok_or(Outcome::StateMismatch)
    }

    /// Lease ring account `index`, send `call`, and require success.
    async fn send(&self, rpc: &Rpc, index: usize, call: Call) -> Result<Done, Outcome> {
        let lease = self.ctx.lease_index(index, rpc).await?;
        let done = self.ctx.transact("market", rpc, lease, call).await?;
        if done.landed.receipt.succeeded() {
            Ok(done)
        } else {
            Err(Outcome::ReceiptStatus0)
        }
    }

    /// A call of ring account `index` to the token.
    async fn token_call(&self, rpc: &Rpc, index: usize, input: Bytes) -> Result<Done, Outcome> {
        let token = self.state.rwa.ok_or(Outcome::StateMismatch)?;
        self.send(
            rpc,
            index,
            Call::new(TxKind::Call(token), U256::ZERO, input, CALL_GAS),
        )
        .await
    }

    async fn save(&self) {
        if let Err(e) = self.ctx.store.save_market(&self.state).await {
            tracing::warn!(error = %e, "store the market state");
        }
    }
}
