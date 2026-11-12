//! A complete observation ties the account and all mirror heads to one primary.

use std::cell::RefCell;
use std::future::Future;
use std::net::SocketAddr;

use alloy_primitives::Address;
use anyhow::Context;

use super::{CTX, Heads};
use crate::cases::cache::{MIRROR_HEAD_KEYS, discovery::Discovery};
use crate::harness::Harness;
use crate::poll::{self, Budget};
use crate::rpc::AccountState;

#[derive(Debug)]
pub(super) struct Snapshot {
    pub(super) primary: String,
    pub(super) heads: Heads,
    pub(super) row: String,
}

impl Snapshot {
    pub(super) async fn current(h: &Harness, account: Address) -> anyhow::Result<Self> {
        let primary = Discovery::primary(h)
            .await?
            .context("no Sentinel names a live primary")?;
        Self::on(h, &primary, account).await
    }

    pub(super) async fn on(h: &Harness, primary: &str, account: Address) -> anyhow::Result<Self> {
        let address = SocketAddr::from((h.contract.node_at(primary)?.ip, 6379));
        Self::read(primary, address, account).await
    }

    async fn read(primary: &str, address: SocketAddr, account: Address) -> anyhow::Result<Self> {
        let mut connection = Discovery::connect_primary(address).await?;
        let keys: Vec<_> = MIRROR_HEAD_KEYS.split_ascii_whitespace().collect();
        let (role, heads, row): (Vec<redis::Value>, [Option<String>; 3], [Option<String>; 3]) =
            redis::pipe()
                .atomic()
                .cmd("ROLE")
                .cmd("MGET")
                .arg(keys)
                .cmd("HMGET")
                .arg(format!("acct:{account}"))
                .arg(&["nonce", "balance", "tx_idx"])
                .query_async(&mut connection)
                .await?;
        // Demotion between connection setup and EXEC cannot certify the snapshot.
        Discovery::require_primary(&role)?;
        Ok(Self {
            primary: primary.into(),
            heads: Heads::parse(&heads.map(Option::unwrap_or_default).join("\n"))?,
            row: row.map(Option::unwrap_or_default).join("\n"),
        })
    }
}

pub(super) struct Observation<'a> {
    expected: &'a AccountState,
    before: Option<Heads>,
    first: RefCell<Option<(String, Heads)>>,
    last: RefCell<String>,
}

impl<'a> Observation<'a> {
    pub(super) fn new(expected: &'a AccountState, before: Option<Heads>) -> Self {
        Self {
            expected,
            before,
            first: RefCell::new(None),
            last: RefCell::new("no completed observation".into()),
        }
    }

    pub(super) async fn wait<F, Fut>(&self, budget: Budget, mut read: F) -> anyhow::Result<Snapshot>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = anyhow::Result<Snapshot>>,
    {
        let outcome = poll::until(budget, |_| {
            let response = read();
            async move {
                match response.await {
                    Ok(snapshot) => Ok(self.observe(snapshot)),
                    Err(error) => {
                        *self.last.borrow_mut() = format!("unread ({error:#})");
                        Ok(None)
                    }
                }
            }
        })
        .await?;
        let (snapshot, _) = outcome.or_fail(|elapsed| {
            crate::chaos_fail!(
                "{CTX}: complete account and mirror-head evidence did not return within {}s: {}",
                elapsed.as_secs(),
                self.last.borrow()
            )
        })?;
        Ok(snapshot)
    }

    fn observe(&self, snapshot: Snapshot) -> Option<Snapshot> {
        *self.last.borrow_mut() = format!("expected {:?}, observed {snapshot:?}", self.expected);
        if !self.expected.matches_projection(&snapshot.row) {
            return None;
        }
        let Some(before) = self.before else {
            return Some(snapshot);
        };
        if !snapshot.heads.advanced_over(before) {
            return None;
        }
        let mut first = self.first.borrow_mut();
        match first.as_ref() {
            Some((primary, heads)) if primary == &snapshot.primary => {
                snapshot.heads.advanced_over(*heads).then_some(snapshot)
            }
            _ => {
                *first = Some((snapshot.primary, snapshot.heads));
                None
            }
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod protocol_tests;
