//! Exact account coverage includes a used sender and an untouched funded account.

use std::cell::RefCell;

use super::discovery::Discovery;
use super::rebuilds::{MirrorRebuilds, RebuildPhase};
use crate::harness::Harness;
use crate::poll::{self, Budget};
use crate::rpc::{AccountState, Rpc};

/// The executor's committed account query, separate from the ingress cache.
const ACCOUNT_QUERY_PORT: u16 = 9024;
/// This load reserve stays idle in chaos-only shards.
const UNTOUCHED_ACCOUNT: u32 = 6;

mod projection;
use projection::Projection;

pub(crate) struct CacheRecovery {
    expected: Vec<AccountState>,
    mirrors: MirrorRebuilds,
}

impl CacheRecovery {
    pub(crate) async fn capture(h: &Harness) -> anyhow::Result<Self> {
        let mut expected = Vec::new();
        for account in [h.knobs.gate_account, UNTOUCHED_ACCOUNT] {
            expected.push(AccountState::replicated(h, account, 0).await?);
        }
        Ok(Self {
            expected,
            mirrors: MirrorRebuilds::read(h).await?,
        })
    }

    /// The mirrors reach `phase`, and the projection holds the exact
    /// captured accounts.
    pub(crate) async fn await_restored(
        &self,
        h: &Harness,
        ctx: &str,
        phase: RebuildPhase,
    ) -> anyhow::Result<()> {
        self.mirrors.await_phase(h, ctx, phase).await?;
        Projection::new(&self.expected)
            .wait(ctx, Budget::secs(120, 3), || self.read(h))
            .await?;
        crate::log(format!(
            "{ctx}: exact rebuilt nonce and balance match the executor for {:?}",
            self.expected
        ));
        Ok(())
    }

    async fn read(&self, h: &Harness) -> anyhow::Result<Vec<String>> {
        let master = Discovery::primary(h)
            .await?
            .ok_or_else(|| crate::chaos_fail!("no Sentinel names a live primary"))?;
        let address = std::net::SocketAddr::from((h.contract.node_at(&master)?.ip, 6379));
        let mut connection = Discovery::connect_primary(address).await?;
        let mut query = redis::pipe();
        self.expected.iter().for_each(|account| {
            query
                .cmd("HMGET")
                .arg(format!("acct:{}", account.address))
                .arg(&["nonce", "balance", "tx_idx"]);
        });
        let rows: Vec<[Option<String>; 3]> = query.query_async(&mut connection).await?;
        Ok(rows
            .into_iter()
            .map(|row| row.map(Option::unwrap_or_default).join("\n"))
            .collect())
    }
}

impl AccountState {
    pub(super) async fn replicated(h: &Harness, account: u32, floor: u64) -> anyhow::Result<Self> {
        let queries = h
            .probes
            .executors
            .iter()
            .map(|executor| {
                Rpc::new(
                    &format!("http://{}:{ACCOUNT_QUERY_PORT}", executor.ip),
                    h.knobs.chain_id,
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        ReplicaAccount {
            queries,
            account,
            floor,
        }
        .wait(Budget::secs(120, 2))
        .await
    }

    pub(super) async fn after_transfer(h: &Harness, account: u32) -> anyhow::Result<Self> {
        let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
        let nonce = rpc.nonce_of(account).await?;
        rpc.transfer_hash(account, nonce, std::time::Duration::from_secs(60))
            .await?;
        let floor = nonce
            .checked_add(1)
            .ok_or_else(|| crate::chaos_fail!("the receipted sender nonce overflows"))?;
        Self::replicated(h, account, floor).await
    }

    fn agreed(replicas: &[Self], floor: u64) -> Option<Self> {
        replicas
            .first()
            .filter(|first| first.nonce >= floor && replicas.iter().all(|state| state == *first))
            .cloned()
    }

    pub(super) fn matches_projection(&self, observed: &str) -> bool {
        let fields: Vec<_> = observed.lines().collect();
        let [nonce, balance, tx_idx] = fields.as_slice() else {
            return false;
        };
        nonce.parse::<u64>() == Ok(self.nonce)
            && alloy_primitives::U256::from_str_radix(balance, 10)
                .is_ok_and(|balance| balance == self.balance)
            && tx_idx.parse::<u64>().is_ok()
    }
}

struct ReplicaAccount {
    queries: Vec<Rpc>,
    account: u32,
    floor: u64,
}

impl ReplicaAccount {
    async fn wait(&self, budget: Budget) -> anyhow::Result<AccountState> {
        let last = RefCell::new(Vec::new());
        let outcome = poll::until(budget, |_| async {
            let mut replicas = Vec::with_capacity(self.queries.len());
            for query in &self.queries {
                replicas.push(query.account_state(self.account).await?);
            }
            let agreed = AccountState::agreed(&replicas, self.floor);
            last.replace(replicas);
            Ok(agreed)
        })
        .await?;
        let (state, _) = outcome.or_fail(|elapsed| {
            crate::chaos_fail!(
                "account #{} executor baseline does not agree at nonce >= {} after {}s: {:?}",
                self.account,
                self.floor,
                elapsed.as_secs(),
                last.borrow()
            )
        })?;
        Ok(state)
    }
}

#[cfg(test)]
mod tests;
