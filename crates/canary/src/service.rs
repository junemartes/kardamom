//! The wiring: the endpoints, the ring, the status feed, the probes and
//! the balance task.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::U256;
use kardamom_bench::signers::DerivedSigner;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::{Endpoint, Timing};
use crate::feed::{Board, Feed};
use crate::funds::Funds;
use crate::market::MarketTask;
use crate::probes::contract::Contract;
use crate::probes::deposit::{Deposit, DepositSettings};
use crate::probes::fees::Fees;
use crate::probes::read::Read;
use crate::probes::transfer::Transfer;
use crate::probes::{Context, drive};
use crate::ring::Ring;
use crate::rpc::Rpc;
use crate::store::Store;
use crate::wait::Poll;

/// The time between two chain id asks at start.
const START_RETRY: Duration = Duration::from_secs(2);

/// What the canary needs to start.
#[derive(Debug, Clone)]
pub struct Settings {
    pub endpoints: Vec<Endpoint>,
    pub notifier_ws: String,
    pub signers: Vec<DerivedSigner>,
    pub dir: PathBuf,
    pub timing: Timing,
    pub l2_floor: U256,
    pub topup: U256,
    pub safe_sample: std::num::NonZeroU32,
    /// The `deposit` probe's L1 side; `None` runs no deposit probe.
    pub deposit: Option<DepositSettings>,
}

/// The running canary: its tasks.
#[derive(Debug)]
pub struct Canary {
    tasks: Vec<JoinHandle<()>>,
}

impl Canary {
    /// Ask the endpoints for the chain id until one answers, then start
    /// every task.
    ///
    /// # Errors
    ///
    /// Returns an error when an endpoint URL does not parse, or the data
    /// directory does not read.
    pub async fn start(settings: Settings) -> anyhow::Result<Self> {
        let endpoints = settings
            .endpoints
            .iter()
            .cloned()
            .map(|e| Rpc::new(e, settings.timing.receipt_timeout))
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(
            !endpoints.is_empty(),
            "the canary needs an ingress endpoint"
        );
        let chain_id = Self::chain_id(&endpoints).await;
        let store = Store::new(&settings.dir);
        let ring = Arc::new(Ring::open(&store.ring_dir(), settings.signers, chain_id).await?);
        let contracts = store.contracts().await?;
        let (anchor, _) = watch::channel(store.anchor().await?);
        let (board, handle) = Board::new();
        let feed = Feed {
            url: settings.notifier_ws,
            senders: ring.addresses(),
            board: handle.clone(),
        };
        let ctx = Arc::new(Context {
            ring,
            endpoints,
            board: handle,
            timing: settings.timing,
            store,
            anchor,
            safe_sample: settings.safe_sample,
        });
        let funds = Funds {
            ctx: Arc::clone(&ctx),
            floor: settings.l2_floor,
            topup: settings.topup,
        };
        let market = MarketTask::new(Arc::clone(&ctx)).await?;
        tracing::info!(chain_id, accounts = ?ctx.ring.addresses(), "kardamom-canary starting");
        let mut tasks = vec![
            tokio::spawn(board.run()),
            tokio::spawn(feed.run()),
            tokio::spawn(funds.run()),
            tokio::spawn(market.run()),
            tokio::spawn(drive(Transfer::new(Arc::clone(&ctx)))),
            tokio::spawn(drive(Read::new(Arc::clone(&ctx)))),
            tokio::spawn(drive(Fees::new(Arc::clone(&ctx)))),
            tokio::spawn(drive(Contract::new(Arc::clone(&ctx), &contracts))),
        ];
        if let Some(deposit) = settings.deposit {
            let dir = settings.dir.join("l1");
            let probe = Deposit::new(ctx, deposit, &dir).await?;
            tasks.push(tokio::spawn(drive(probe)));
        }
        Ok(Self { tasks })
    }

    /// The chain id of the first endpoint that answers, asked in turn
    /// until one does.
    async fn chain_id(endpoints: &[Rpc]) -> u64 {
        let mut turn = 0usize;
        let found = Poll::within(Duration::MAX, START_RETRY)
            .until(|| {
                let rpc = &endpoints[turn % endpoints.len()];
                turn = turn.wrapping_add(1);
                async move { rpc.chain_id().await.ok() }
            })
            .await;
        found.unwrap_or_default()
    }

    /// Stop every task.
    pub fn stop(self) {
        self.tasks.iter().for_each(JoinHandle::abort);
    }
}
