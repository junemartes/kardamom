//! The client side: a [`BlobSource`] over the indexer's API, for the
//! rebuild.
//!
//! The rebuild checks every blob it fetches against the versioned hash L1
//! committed to, so this client is transport only: it carries bytes and
//! reports the indexer's errors.

use alloy_eips::eip4844::Blob;
use alloy_primitives::{B256, Bytes};
use kardamom_batcher::da_store::BlobSource;
use kardamom_batcher::error::BatcherError;
use serde::Deserialize;

/// A [`BlobSource`] that asks a running indexer.
///
/// [`BlobSource::fetch_blob`] is synchronous; this client blocks the
/// calling tokio worker in place, so the caller runs on a multi-thread
/// runtime, as `kardamom-reconstruct` does.
#[derive(Clone, Debug)]
pub struct HttpBlobSource {
    url: String,
    client: reqwest::Client,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RpcResponse {
    Ok { result: Bytes },
    Err { error: RpcError },
}

impl HttpBlobSource {
    /// A client of the indexer's API at `url` (`http://host:port`).
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: reqwest::Client::new(),
        }
    }

    async fn fetch(&self, versioned_hash: B256) -> Result<Blob, BatcherError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "indexer_blob",
            "params": [versioned_hash],
        });
        let response: RpcResponse = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(|e| BatcherError::Blob(format!("indexer {}: {e}", self.url)))?
            .error_for_status()
            .map_err(|e| BatcherError::Blob(format!("indexer {}: {e}", self.url)))?
            .json()
            .await
            .map_err(|e| BatcherError::Blob(format!("indexer {}: {e}", self.url)))?;
        match response {
            RpcResponse::Ok { result } => Blob::try_from(result.as_ref())
                .map_err(|e| BatcherError::Blob(format!("indexer blob {versioned_hash}: {e}"))),
            RpcResponse::Err { error } => Err(BatcherError::Blob(format!(
                "indexer blob {versioned_hash}: {} ({})",
                error.message, error.code
            ))),
        }
    }
}

impl BlobSource for HttpBlobSource {
    fn fetch_blob(&self, versioned_hash: B256) -> Result<Blob, BatcherError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.fetch(versioned_hash))
        })
    }
}
