//! Fault evidence and cleanup use a bounded connection to the verified primary.

use std::net::SocketAddr;

use super::super::discovery::Discovery;
use super::{Harness, Heads};
use crate::rpc::AccountState;

pub(super) struct Store {
    address: SocketAddr,
}

#[cfg(test)]
mod tests;

impl Store {
    pub(super) async fn discover(h: &Harness) -> anyhow::Result<Self> {
        let primary = Discovery::primary(h)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no Sentinel names a live primary"))?;
        Ok(Self {
            address: SocketAddr::from((h.contract.node_at(&primary)?.ip, 6379)),
        })
    }

    async fn command<T: redis::FromRedisValue>(&self, cmd: &redis::Cmd) -> anyhow::Result<T> {
        let mut connection = Discovery::connect_primary(self.address).await?;
        Ok(cmd.query_async(&mut connection).await?)
    }

    pub(super) async fn heads(&self) -> anyhow::Result<Heads> {
        let values: [Option<String>; 3] = self
            .command(redis::cmd("MGET").arg(&["head:0", "head:1", "head:2"]))
            .await?;
        Heads::parse(&values.map(Option::unwrap_or_default).join("\n"))
    }

    pub(super) async fn row(&self, account: &AccountState) -> anyhow::Result<String> {
        let values: [Option<String>; 3] = self
            .command(
                redis::cmd("HMGET")
                    .arg(format!("acct:{}", account.address))
                    .arg(&["nonce", "balance", "tx_idx"]),
            )
            .await?;
        Ok(values.map(Option::unwrap_or_default).join("\n"))
    }

    pub(super) async fn hold(&self, heads: Heads) -> anyhow::Result<()> {
        let result: u64 = self
            .command(
                redis::cmd("EVAL")
                    .arg(format!("{} return 1", heads.keep_alive()))
                    .arg(0),
            )
            .await?;
        anyhow::ensure!(result == 1, "held heartbeat script did not finish");
        Ok(())
    }

    pub(super) async fn clear(&self) -> anyhow::Result<()> {
        self.command::<u64>(redis::cmd("DEL").arg(&["head:0", "head:1", "head:2"]))
            .await?;
        Ok(())
    }
}
