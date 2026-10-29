//! Every ingress proves its own Redis recovery in one complete observation.

use std::cell::RefCell;
use std::future::Future;

use alloy_primitives::Address;
use anyhow::Context;
use tokio::task::JoinSet;

use super::{DEGRADED, LOOKUPS, REDIS_LAYER, Readers};
use crate::harness::Harness;
use crate::metrics::{self, Scrape, Target};
use crate::poll::{self, Budget};
use crate::rpc::Rpc;

impl Readers {
    pub(super) fn parse(body: &str) -> Self {
        Self {
            degraded: metrics::sum(body, DEGRADED).unwrap_or(0),
            redis: metrics::sum_where(body, LOOKUPS, REDIS_LAYER).unwrap_or(0),
        }
    }

    pub(super) async fn probe(rpc: &Rpc, step: u32) -> anyhow::Result<()> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let mut bytes = [0u8; 20];
        bytes[..16].copy_from_slice(&nanos.to_le_bytes());
        bytes[16..].copy_from_slice(&step.to_le_bytes());
        rpc.balance_probe(Address::from(bytes)).await
    }

    fn recovered_from(self, before: Self) -> bool {
        self.redis > before.redis && self.degraded == before.degraded
    }
}

#[derive(Clone)]
struct Reader {
    name: String,
    target: Target,
    scrape: Scrape,
    rpc: Rpc,
}

impl Reader {
    async fn sample(&self) -> anyhow::Result<Readers> {
        let body = self
            .scrape
            .fetch(&self.target)
            .await
            .context("exporter unavailable")?;
        Ok(Readers::parse(&body))
    }

    async fn transition(&self) -> anyhow::Result<Transition> {
        let before = self.sample().await.context("before cold read")?;
        Readers::probe(&self.rpc, 1).await.context("cold read")?;
        let after = self.sample().await.context("after cold read")?;
        Ok(Transition { before, after })
    }

    async fn read(self) -> Reading {
        let result = self.transition().await;
        Reading {
            name: self.name,
            result,
        }
    }
}

#[derive(Debug)]
struct Transition {
    before: Readers,
    after: Readers,
}

#[derive(Debug)]
struct Reading {
    name: String,
    result: anyhow::Result<Transition>,
}

pub(super) struct Recovery {
    readers: Vec<Reader>,
    last: RefCell<String>,
}

impl Recovery {
    pub(super) fn for_harness(h: &Harness) -> anyhow::Result<Self> {
        let readers = h
            .probes
            .ingresses
            .iter()
            .map(|node| {
                Ok(Reader {
                    name: node.container.clone(),
                    target: h.probes.ingress_target(node),
                    scrape: h.probes.scrape().clone(),
                    rpc: Rpc::new(&node.rpc_url(), h.knobs.chain_id)?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Self::new(readers)
    }

    fn new(readers: Vec<Reader>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !readers.is_empty(),
            "reader recovery requires ingress endpoints"
        );
        Ok(Self {
            readers,
            last: RefCell::new("no completed observation".into()),
        })
    }

    pub(super) async fn wait(&self, ctx: &str) -> anyhow::Result<()> {
        self.wait_with(ctx, Budget::secs(120, 3), || self.read())
            .await
    }

    async fn read(&self) -> Vec<Reading> {
        let mut tasks = JoinSet::new();
        self.readers.iter().cloned().for_each(|reader| {
            tasks.spawn(reader.read());
        });
        tasks.join_all().await
    }

    async fn wait_with<F, Fut>(&self, ctx: &str, budget: Budget, mut read: F) -> anyhow::Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Vec<Reading>>,
    {
        let outcome = poll::until(budget, |_| {
            let reading = read();
            async move { Ok::<_, anyhow::Error>(self.observe(&reading.await)) }
        })
        .await?;
        let ((), elapsed) = outcome.or_fail(|elapsed| {
            crate::chaos_fail!(
                "{ctx}: not every ingress reader recovered within {}s (last observation: {})",
                elapsed.as_secs(),
                self.last.borrow()
            )
        })?;
        crate::log(format!(
            "{ctx}: every ingress reader uses Redis without degradation after {}s: {}",
            elapsed.as_secs(),
            self.last.borrow()
        ));
        Ok(())
    }

    fn observe(&self, readings: &[Reading]) -> Option<()> {
        let recovered = readings.len() == self.readers.len()
            && self.readers.iter().all(|reader| {
                readings
                    .iter()
                    .find(|reading| reading.name == reader.name)
                    .is_some_and(|reading| {
                        reading
                            .result
                            .as_ref()
                            .is_ok_and(|value| value.after.recovered_from(value.before))
                    })
            });
        *self.last.borrow_mut() = format!("{readings:?}");
        recovered.then_some(())
    }
}

#[cfg(test)]
mod tests;
