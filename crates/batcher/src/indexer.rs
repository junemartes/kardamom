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
