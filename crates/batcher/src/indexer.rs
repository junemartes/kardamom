//! The inbox indexer as a source: the last posted batch and its blobs,
//! by index, without an event scan of L1.
//!
//! The indexer (`kardamom-l1-indexer`) archives every `BatchPosted`
//! batch with its payload and serves them over JSON-RPC. This client
//! reads two of its methods: `indexer_batch(index)` and
//! `indexer_payload(daCert)`. The indexer stores what its own proxy
//! served and checked against the certificate.

use alloy_primitives::Bytes;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::da::PayloadSource;
use crate::error::BatcherError;
use crate::l1::BatchDescriptor;
use kardamom_types::L1Block;

/// A client of one indexer's API (`http://host:port`).
#[derive(Clone, Debug)]
pub struct IndexerClient {
    url: String,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

/// A JSON-RPC response: `result` or `error`, never both.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RpcResponse<T> {
    Ok { result: T },
    Err { error: RpcError },
}

impl<T> RpcResponse<T> {
    fn into_result(self, method: &str) -> Result<T, BatcherError> {
        match self {
            Self::Ok { result } => Ok(result),
            Self::Err { error } => Err(BatcherError::L1(format!(
                "indexer {method}: {} ({})",
                error.message, error.code
            ))),
        }
    }
}

impl IndexerClient {
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: reqwest::Client::new(),
        }
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, BatcherError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let response: RpcResponse<T> = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(|e| BatcherError::L1(format!("indexer {method} at {}: {e}", self.url)))?
            .error_for_status()
            .map_err(|e| BatcherError::L1(format!("indexer {method} at {}: {e}", self.url)))?
            .json()
            .await
            .map_err(|e| BatcherError::L1(format!("indexer {method} at {}: {e}", self.url)))?;
        response.into_result(method)
    }

    /// The posted batch `index`, or `None` while the indexer has not
    /// reached it. The indexer follows the finalized L1, so a batch posted
    /// in the last few minutes is not there yet.
    ///
    /// # Errors
    /// Returns an error when the request fails.
    pub async fn batch(&self, index: u64) -> Result<Option<BatchDescriptor>, BatcherError> {
        self.call("indexer_batch", serde_json::json!([index])).await
    }

    /// The L1 timestamp of the newest batch the follower's archive holds:
    /// the block that carried it, from its `l1_blocks` record. `None`
    /// while the archive holds no batch.
    ///
    /// # Errors
    /// Returns an error when a request fails or a record does not decode.
    pub async fn last_post_time(&self) -> Result<Option<u64>, BatcherError> {
        let cursor: serde_json::Value = self.call("indexer_status", serde_json::json!([])).await?;
        let Some(index) = cursor["last_batch"].as_u64() else {
            return Ok(None);
        };
        let Some(entry) = self
            .call::<Option<serde_json::Value>>("indexer_batch", serde_json::json!([index]))
            .await?
        else {
            return Ok(None);
        };
        let Some(block) = entry["l1_block"].as_u64() else {
            return Ok(None);
        };
        let Some(bytes) = self
            .call::<Option<Bytes>>("indexer_l1_block", serde_json::json!([block]))
            .await?
        else {
            return Ok(None);
        };
        rkyv::from_bytes::<L1Block, rkyv::rancor::Error>(&bytes)
            .map(|record| Some(record.timestamp))
            .map_err(|e| BatcherError::L1(format!("decode the l1_blocks record of {block}: {e}")))
    }

    /// Refuse an indexer that is not running: a halted follower may hold
    /// a lie of its L1 source, and a paused one makes no progress. The
    /// error names the state, the cause, and the runbook.
    ///
    /// # Errors
    /// Returns an error when the request fails, or when the indexer is
    /// halted or paused.
    pub async fn require_running(&self) -> Result<(), BatcherError> {
        let record: serde_json::Value = self.call("indexer_halt", serde_json::json!([])).await?;
        match Self::refusal(&record) {
            Some(why) => Err(BatcherError::L1(format!("indexer at {}: {why}", self.url))),
            None => Ok(()),
        }
    }

    /// Why a lifecycle record refuses its indexer, `None` while it runs.
    fn refusal(record: &serde_json::Value) -> Option<String> {
        let state = record["state"].as_str().unwrap_or("unknown");
        (state != "running").then(|| {
            format!(
                "the indexer is {state} (cause {}, runbook {}); refuse to rebuild from it",
                record["cause"].as_str().unwrap_or("none"),
                record["runbook"].as_str().unwrap_or("none"),
            )
        })
    }

    async fn payload(&self, da_cert: &Bytes) -> Result<Vec<u8>, BatcherError> {
        let bytes: Bytes = self
            .call("indexer_payload", serde_json::json!([da_cert]))
            .await?;
        Ok(bytes.to_vec())
    }
}

/// [`PayloadSource::fetch_payload`] is synchronous; the client blocks the
/// calling tokio worker in place, so the caller runs on a multi-thread
/// runtime, as the batcher and `kardamom-reconstruct` do.
impl PayloadSource for IndexerClient {
    fn fetch_payload(&self, da_cert: &Bytes) -> Result<Vec<u8>, BatcherError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.payload(da_cert))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_halted_indexer_is_refused_with_its_runbook() {
        let halted = serde_json::json!({
            "state": "halted",
            "cause": "l1_chain_break",
            "runbook": "docs/runbooks/l1_chain_break.md",
        });
        let why = IndexerClient::refusal(&halted).unwrap();
        assert!(why.contains("halted"), "{why}");
        assert!(why.contains("docs/runbooks/l1_chain_break.md"), "{why}");
        let running = serde_json::json!({"state": "running", "halted": false});
        assert_eq!(IndexerClient::refusal(&running), None);
    }

    #[test]
    fn a_result_and_an_error_parse() {
        let ok: RpcResponse<Option<BatchDescriptor>> = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":1,"result":{"index":7,"da_cert":"0x02aa","l2_block_start":10,"l2_block_end":12,"records_commitment":"0x0202020202020202020202020202020202020202020202020202020202020202","l1_block":100,"l1_tx":"0x0303030303030303030303030303030303030303030303030303030303030303"}}"#,
        )
        .unwrap();
        let d = ok.into_result("indexer_batch").unwrap().unwrap();
        assert_eq!((d.index, d.l2_block_start, d.l2_block_end), (7, 10, 12));
        assert_eq!(d.da_cert, Bytes::from(vec![0x02, 0xaa]));

        let none: RpcResponse<Option<BatchDescriptor>> =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"result":null}"#).unwrap();
        assert!(none.into_result("indexer_batch").unwrap().is_none());

        let err: RpcResponse<Bytes> = serde_json::from_str(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"no such payload"}}"#,
        )
        .unwrap();
        let e = err.into_result("indexer_payload").unwrap_err().to_string();
        assert!(e.contains("no such payload") && e.contains("-32000"), "{e}");
    }
}
