//! Independent, bounded observations of Sentinel and the primary it names.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use tokio::task::JoinSet;

use crate::contract::NodeContract;
use crate::harness::Harness;

/// One Sentinel and its named primary share this observation budget.
const PROBE_BUDGET: Duration = Duration::from_secs(3);

pub(super) struct Discovery;

impl Discovery {
    pub(super) async fn primary(h: &Harness) -> anyhow::Result<Option<String>> {
        let mut tasks = JoinSet::new();
        std::iter::once(&h.probes.validator)
            .chain(&h.probes.ingresses)
            .for_each(|node| {
                tasks.spawn(
                    Probe {
                        sentinel: SocketAddr::from((node.ip, 26379)),
                        contract: h.contract.clone(),
                    }
                    .bounded(),
                );
            });
        Self::first_live(tasks).await
    }

    async fn first_live(mut tasks: JoinSet<Option<String>>) -> anyhow::Result<Option<String>> {
        let mut primary = None;
        while primary.is_none() && !tasks.is_empty() {
            primary = tasks
                .join_next()
                .await
                .transpose()
                .context("Sentinel observation task failed")?
                .flatten();
        }
        Ok(primary)
    }
}

struct Probe {
    sentinel: SocketAddr,
    contract: NodeContract,
}

impl Probe {
    async fn bounded(self) -> Option<String> {
        match tokio::time::timeout(PROBE_BUDGET, self.primary()).await {
            Ok(Ok(primary)) => Some(primary),
            result => {
                crate::log(format!(
                    "the sentinel at {} named no live primary: {result:?}",
                    self.sentinel
                ));
                None
            }
        }
    }

    async fn primary(&self) -> anyhow::Result<String> {
        let mut connection = Self::connect(self.sentinel).await?;
        let (host, port): (String, u16) = redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg("kardamom")
            .query_async(&mut connection)
            .await?;
        // The node contract resolves Consul names on the Docker host, which
        // does not use the cluster's DNS. ROLE still checks the live process.
        let address = SocketAddr::from((self.contract.node_at(&host)?.ip, port));
        let mut primary = Self::connect(address).await?;
        let role: Vec<redis::Value> = redis::cmd("ROLE").query_async(&mut primary).await?;
        let name = role.first().context("empty Redis ROLE response")?;
        anyhow::ensure!(
            redis::from_redis_value::<String>(name.clone())? == "master",
            "Sentinel names a node that is not a primary"
        );
        Ok(host)
    }

    async fn connect(address: SocketAddr) -> anyhow::Result<redis::aio::MultiplexedConnection> {
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(PROBE_BUDGET))
            .set_response_timeout(Some(PROBE_BUDGET));
        Ok(redis::Client::open(format!("redis://{address}/"))?
            .get_multiplexed_async_connection_with_config(&config)
            .await?)
    }
}

#[cfg(test)]
mod tests;
