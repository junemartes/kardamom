//! Exact account coverage includes a used sender and an untouched funded account.

use std::cell::RefCell;

use super::rebuilds::MirrorRebuilds;
use super::{aux, live_primary, redis_cli};
use crate::harness::Harness;
use crate::poll::{self, Budget};
use crate::rpc::{AccountState, Rpc};

/// The executor's committed account query, separate from the ingress cache.
const ACCOUNT_QUERY_PORT: u16 = 9024;
/// This load reserve stays idle in chaos-only shards.
const UNTOUCHED_ACCOUNT: u32 = 6;

pub(crate) struct CacheRecovery {
    expected: Vec<AccountState>,
    mirrors: MirrorRebuilds,
}

impl CacheRecovery {
    pub(crate) async fn capture(h: &Harness) -> anyhow::Result<Self> {
        let query = Rpc::new(
            &format!("http://{}:{ACCOUNT_QUERY_PORT}", h.probes.executors[0].ip),
            h.knobs.chain_id,
        )?;
        let mut expected = Vec::new();
        for account in [h.knobs.gate_account, UNTOUCHED_ACCOUNT] {
            expected.push(query.account_state(account).await?);
        }
        Ok(Self {
            expected,
            mirrors: MirrorRebuilds::read(h).await?,
        })
    }

    pub(crate) async fn await_restored(&self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        self.mirrors.await_all(h, ctx).await?;
        let master = live_primary(h, ctx).await?;
        let last = RefCell::new(Vec::new());
        let (master_ref, last_ref) = (&master, &last);
        let outcome = poll::until(Budget::secs(120, 3), |_| async move {
            let mut missing = Vec::new();
            for account in &self.expected {
                Self::check(h, master_ref, account, &mut missing).await?;
            }
            let complete = missing.is_empty();
            last_ref.replace(missing);
            Ok::<_, anyhow::Error>(complete.then_some(()))
        })
        .await?;
        outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: exact account coverage did not return after {}s: {:?}",
                t.as_secs(),
                last.borrow()
            )
        })?;
        crate::log(format!(
            "{ctx}: exact rebuilt nonce and balance match the executor for {:?}",
            self.expected
        ));
        Ok(())
    }

    async fn check(
        h: &Harness,
        master: &str,
        account: &AccountState,
        missing: &mut Vec<String>,
    ) -> anyhow::Result<()> {
        let script = redis_cli(&format!(
            "--raw -h {master} -p 6379 HMGET acct:{} nonce balance tx_idx",
            account.address
        ));
        let observed = h.nodes.exec(&aux(h), &script).await?;
        if !account.matches_projection(&observed) {
            missing.push(format!("expected {account:?}, observed {observed:?}"));
        }
        Ok(())
    }
}

impl AccountState {
    fn matches_projection(&self, observed: &str) -> bool {
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

#[cfg(test)]
mod tests;
