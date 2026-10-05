//! The harness's own view of L1 and of the lying proxy: the in-cluster
//! anvil, read directly and never through the proxy; the proxy's control
//! endpoint; and the Prometheus alerts on the aux node. A case reads the
//! truth here and compares it with what the followers believed.

use std::time::Duration;

use alloy_primitives::{Address, hex, keccak256};
use anyhow::Context;
use kardamom_l1_fault_proxy::Fault;
use serde_json::{Value, json};

use crate::harness::Harness;
use crate::poll::{self, Budget};

/// The proxy's JSON-RPC pipe and control endpoint on the aux node
/// (`ports.l1_fault_proxy` in `group_vars/all.yml`).
pub const FAULT_PROXY_PORT: u16 = 8547;
/// The monitoring job's Prometheus on the aux node.
pub const PROMETHEUS_PORT: u16 = 9090;
/// The alert on the age of the last post, as `deploy/alerts.yml` names it.
pub const STALE_POST_ALERT: &str = "KardamomBatcherLastPostStale";

/// One posted batch as its `BatchPosted` log names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Posted {
    pub index: u64,
    pub l2_block_start: u64,
    pub l2_block_end: u64,
}

/// The L1 side of a case.
pub struct L1 {
    http: reqwest::Client,
    /// The in-cluster anvil, on the control node.
    rpc: String,
    /// The fault proxy's control endpoint.
    proxy: String,
    prometheus: String,
    pub settlement: Address,
}

/// The four-byte selector of a Solidity signature.
fn selector(signature: &str) -> String {
    format!("0x{}", hex::encode(&keccak256(signature.as_bytes())[..4]))
}

/// The `i`th 32-byte word of ABI-encoded `data`, as a `u64`: the low
/// eight bytes of the word.
fn word_u64(data: &str, i: usize) -> anyhow::Result<u64> {
    let digits = data.trim_start_matches("0x");
    let start = i
        .checked_mul(64)
        .and_then(|s| s.checked_add(48))
        .context("word index overflow")?;
    let slice = digits
        .get(start..start + 16)
        .with_context(|| format!("ABI word {i} is missing from {data}"))?;
    u64::from_str_radix(slice, 16).with_context(|| format!("ABI word {i} is not a number"))
}

/// A hex quantity (`0x10`) as a `u64`.
fn quantity(value: &Value) -> anyhow::Result<u64> {
    let text = value.as_str().context("a quantity is not a string")?;
    u64::from_str_radix(text.trim_start_matches("0x"), 16)
        .with_context(|| format!("{text} is not a hex quantity"))
}

impl L1 {
    /// The L1 side of the cluster `h` describes, with the settlement
    /// address the deployer reports.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract lacks a node or the settlement
    /// address cannot be resolved.
    pub async fn new(h: &Harness) -> anyhow::Result<Self> {
        let aux = h.probes.validator.ip;
        let settlement: Address = h
            .settlement_address()
            .await?
            .parse()
            .context("the settlement address is not an address")?;
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .context("build the L1 client")?,
            rpc: h.l1_rpc()?,
            proxy: format!("http://{aux}:{FAULT_PROXY_PORT}"),
            prometheus: format!("http://{aux}:{PROMETHEUS_PORT}"),
            settlement,
        })
    }

    /// One JSON-RPC call to anvil, directly.
    async fn rpc(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let reply: Value = self
            .http
            .post(&self.rpc)
            .json(&body)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("{method} at {}", self.rpc))?
            .json()
            .await
            .with_context(|| format!("decode {method}"))?;
        anyhow::ensure!(
            reply.get("error").is_none(),
            "{method} failed: {}",
            reply["error"]
        );
        Ok(reply["result"].clone())
    }

    /// One `eth_call` of `data` against the settlement.
    async fn call(&self, data: String) -> anyhow::Result<String> {
        let result = self
            .rpc(
                "eth_call",
                json!([{ "to": format!("{:#x}", self.settlement), "data": data }, "latest"]),
            )
            .await?;
        result
            .as_str()
            .map(str::to_string)
            .context("eth_call result is not a string")
    }

    /// The L1 head.
    ///
    /// # Errors
    ///
    /// Returns an error if anvil does not answer.
    pub async fn head(&self) -> anyhow::Result<u64> {
        quantity(&self.rpc("eth_blockNumber", json!([])).await?)
    }

    /// Mine `blocks` L1 blocks. Anvil mines a block only for a
    /// transaction, so while nothing posts, the finalized tip stands
    /// still and the da-watcher publishes no epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if anvil refuses.
    pub async fn mine(&self, blocks: u64) -> anyhow::Result<()> {
        self.rpc("anvil_mine", json!([format!("{blocks:#x}")]))
            .await
            .map(|_| ())
    }

    /// The contract's `lastBatchIndex`.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub async fn last_batch_index(&self) -> anyhow::Result<u64> {
        word_u64(&self.call(selector("lastBatchIndex()")).await?, 0)
    }

    /// The contract's `batches(index)`: the batch's first and last L2
    /// block.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub async fn batch(&self, index: u64) -> anyhow::Result<(u64, u64)> {
        let data = format!("{}{index:064x}", selector("batches(uint64)"));
        let result = self.call(data).await?;
        Ok((word_u64(&result, 0)?, word_u64(&result, 1)?))
    }

    /// The L2 block the contract covers through: the last batch's
    /// `l2BlockEnd`, or 0 before the first post.
    ///
    /// # Errors
    ///
    /// Returns an error if a call fails.
    pub async fn covered_through(&self) -> anyhow::Result<u64> {
        match self.last_batch_index().await? {
            0 => Ok(0),
            last => Ok(self.batch(last).await?.1),
        }
    }

    /// Every `BatchPosted` log of the settlement, in index order.
    ///
    /// # Errors
    ///
    /// Returns an error if the log query fails or a log does not decode.
    pub async fn posted(&self) -> anyhow::Result<Vec<Posted>> {
        let topic = format!(
            "{:#x}",
            keccak256(b"BatchPosted(uint64,bytes,uint64,uint64,bytes32)")
        );
        let logs = self
            .rpc(
                "eth_getLogs",
                json!([{
                    "address": format!("{:#x}", self.settlement),
                    "topics": [topic],
                    "fromBlock": "0x0",
                    "toBlock": "latest",
                }]),
            )
            .await?;
        let mut posted = logs
            .as_array()
            .context("eth_getLogs result is not a list")?
            .iter()
            .map(decode_posted)
            .collect::<anyhow::Result<Vec<_>>>()?;
        posted.sort_by_key(|p| p.index);
        Ok(posted)
    }

    /// Every batch starts at the block after the previous one's end, and
    /// the first at block 1: the DA record has no hole and no overlap.
    ///
    /// # Errors
    ///
    /// Returns the first break in the record.
    pub async fn assert_contiguous(&self, ctx: &str) -> anyhow::Result<()> {
        let posted = self.posted().await?;
        anyhow::ensure!(
            !posted.is_empty(),
            "{}: {ctx}: L1 holds no batch",
            crate::FAIL_PREFIX
        );
        let expected_starts = std::iter::once(1).chain(posted.iter().map(|p| p.l2_block_end + 1));
        if let Some((p, expected)) = posted
            .iter()
            .zip(expected_starts)
            .find(|(p, expected)| p.l2_block_start != *expected)
        {
            anyhow::bail!(
                "{}: {ctx}: batch {} starts at block {} but the record covers through {} — the DA record has a gap or an overlap",
                crate::FAIL_PREFIX,
                p.index,
                p.l2_block_start,
                expected - 1
            );
        }
        crate::log(format!(
            "{ctx}: L1's record is contiguous: {} batches through block {}",
            posted.len(),
            posted.last().map_or(0, |p| p.l2_block_end)
        ));
        Ok(())
    }

    /// Make `faults` the proxy's active list.
    ///
    /// # Errors
    ///
    /// Returns an error if the proxy refuses or does not answer.
    pub async fn set_faults(&self, faults: &[Fault]) -> anyhow::Result<()> {
        let reply: Value = self
            .http
            .post(format!("{}/fault", self.proxy))
            .json(&faults)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .context("set the proxy's faults")?
            .json()
            .await
            .context("decode the proxy's answer")?;
        crate::log(format!("l1 fault proxy: active {}", reply["active"]));
        Ok(())
    }

    /// Stop lying.
    ///
    /// # Errors
    ///
    /// Returns an error if the proxy refuses or does not answer.
    pub async fn clear_faults(&self) -> anyhow::Result<()> {
        self.set_faults(&[]).await
    }

    /// The state of the alert named `name` (`pending` or `firing`), or
    /// `None` while it is inactive.
    ///
    /// # Errors
    ///
    /// Returns an error if Prometheus does not answer.
    pub async fn alert_state(&self, name: &str) -> anyhow::Result<Option<String>> {
        let reply: Value = self
            .http
            .get(format!("{}/api/v1/alerts", self.prometheus))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .context("read the Prometheus alerts")?
            .json()
            .await
            .context("decode the Prometheus alerts")?;
        Ok(reply["data"]["alerts"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|a| a["labels"]["alertname"] == name)
            .and_then(|a| a["state"].as_str().map(str::to_string)))
    }

    /// Prometheus loaded the rule named `name` from the alert file.
    ///
    /// # Errors
    ///
    /// Returns an error if Prometheus does not answer or lacks the rule.
    pub async fn require_rule_loaded(&self, name: &str, ctx: &str) -> anyhow::Result<()> {
        let reply: Value = self
            .http
            .get(format!("{}/api/v1/rules", self.prometheus))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .context("read the Prometheus rules")?
            .json()
            .await
            .context("decode the Prometheus rules")?;
        let loaded = reply["data"]["groups"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|g| g["rules"].as_array().into_iter().flatten())
            .any(|r| r["name"] == name);
        anyhow::ensure!(
            loaded,
            "{}: {ctx}: Prometheus did not load the alert rule {name}",
            crate::FAIL_PREFIX
        );
        crate::log(format!("{ctx}: the alert rule {name} is loaded"));
        Ok(())
    }

    /// The alert named `name` is pending or firing, and stays so for
    /// `hold`, within `budget`. A steady chain makes the post-age rule
    /// flicker for one probe period; a fault holds it.
    ///
    /// # Errors
    ///
    /// Returns an error if the alert never holds within `budget`.
    pub async fn await_alert_held(
        &self,
        name: &str,
        hold: Duration,
        budget: Duration,
        ctx: &str,
    ) -> anyhow::Result<()> {
        let every = Duration::from_secs(5);
        let needed = u32::try_from(hold.as_secs() / every.as_secs()).unwrap_or(u32::MAX);
        let run = std::cell::Cell::new(0_u32);
        let run_ref = &run;
        let outcome = poll::until(Budget::new(budget, every), |_| async move {
            // A failed read breaks the held run, like an inactive state:
            // a Prometheus restart loses the pending state too.
            let state = self.alert_state(name).await.unwrap_or_else(|e| {
                crate::log(format!("{ctx}: {e:#}; the held run starts again"));
                None
            });
            let active = state
                .as_deref()
                .is_some_and(|s| s == "pending" || s == "firing");
            run_ref.set(if active { run_ref.get() + 1 } else { 0 });
            Ok::<_, anyhow::Error>((run_ref.get() >= needed).then_some(state))
        })
        .await?;
        let (state, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: the alert {name} did not hold for {}s within {}s",
                hold.as_secs(),
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "{ctx}: the alert {name} is {} and held {}s (after {}s)",
            state.unwrap_or_default(),
            hold.as_secs(),
            elapsed.as_secs()
        ));
        Ok(())
    }
}

/// One `BatchPosted` log: the index from its topic, the range from its
/// data (the certificate's offset, the start, the end, the commitment).
fn decode_posted(log: &Value) -> anyhow::Result<Posted> {
    let index = log["topics"]
        .get(1)
        .map(quantity)
        .transpose()?
        .context("BatchPosted log without an index topic")?;
    let data = log["data"]
        .as_str()
        .context("BatchPosted log without data")?;
    Ok(Posted {
        index,
        l2_block_start: word_u64(data, 1)?,
        l2_block_end: word_u64(data, 2)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selectors_and_words_decode() {
        assert_eq!(selector("lastBatchIndex()").len(), 10);
        let data = format!("0x{:064x}{:064x}{:064x}", 7, 10, 12);
        assert_eq!(word_u64(&data, 0).unwrap(), 7);
        assert_eq!(word_u64(&data, 2).unwrap(), 12);
        assert!(word_u64(&data, 3).is_err());
        assert_eq!(quantity(&json!("0x10")).unwrap(), 16);
    }

    #[test]
    fn a_batch_posted_log_decodes() {
        let log = json!({
            "topics": ["0xaa", format!("0x{:064x}", 3)],
            "data": format!("0x{:064x}{:064x}{:064x}{:064x}", 0x80, 11, 15, 0),
        });
        assert_eq!(
            decode_posted(&log).unwrap(),
            Posted {
                index: 3,
                l2_block_start: 11,
                l2_block_end: 15
            }
        );
    }
}
