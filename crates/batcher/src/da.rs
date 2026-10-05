//! The data-availability layer: EigenDA, through its proxy.
//!
//! The batcher hands the proxy a payload and gets a certificate back
//! (`POST /put?commitment_mode=standard`); the certificate goes on L1 in
//! `postBatch`. A reader hands the proxy a certificate and gets the
//! payload back (`GET /get/<hex cert>?commitment_mode=standard`). The
//! proxy checks the certificate against EigenDA's verifier contract on
//! both paths, and the payload against the certificate's KZG commitment
//! on the read path, so a reader trusts its proxy, not the disperser.
//!
//! The certificate is opaque here: one version byte and an RLP body,
//! carried as bytes from the proxy to L1 and back.

use std::time::Duration;

use alloy_primitives::{Bytes, hex};

use crate::error::BatcherError;

/// A source of batch payloads by certificate: the proxy, or an archive
/// of what the proxy served.
pub trait PayloadSource {
    /// The payload the certificate names.
    ///
    /// # Errors
    /// Returns an error when the source does not serve the certificate,
    /// or when the bytes do not pass the source's check.
    fn fetch_payload(&self, da_cert: &Bytes) -> Result<Vec<u8>, BatcherError>;
}

/// A client of one EigenDA proxy (`http://host:port`).
#[derive(Clone, Debug)]
pub struct DaProxy {
    url: String,
    client: reqwest::Client,
}

impl DaProxy {
    /// A client of the proxy at `url`. A dispersal takes minutes on a real
    /// network (the disperser batches, then the certificate is confirmed
    /// on L1), so the put timeout is long.
    ///
    /// # Errors
    /// Returns an error when the HTTP client cannot be built.
    pub fn new(url: impl Into<String>) -> Result<Self, BatcherError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_mins(20))
            .build()
            .map_err(|e| BatcherError::Da(e.to_string()))?;
        Ok(Self {
            url: url.into().trim_end_matches('/').to_string(),
            client,
        })
    }

    /// Disperse `payload`; the certificate EigenDA returned for it.
    ///
    /// # Errors
    /// Returns an error when the proxy refuses or fails the dispersal.
    pub async fn put(&self, payload: &[u8]) -> Result<Bytes, BatcherError> {
        let url = format!("{}/put?commitment_mode=standard", self.url);
        let response = self
            .client
            .post(&url)
            .header("content-type", "application/octet-stream")
            .body(payload.to_vec())
            .send()
            .await
            .map_err(|e| BatcherError::Da(format!("put: {e}")))?;
        let cert = Self::body(response, "put").await?;
        if cert.is_empty() {
            return Err(BatcherError::Da("put returned an empty certificate".into()));
        }
        Ok(Bytes::from(cert))
    }

    /// The payload `da_cert` names.
    ///
    /// # Errors
    /// Returns an error when the proxy does not serve the certificate.
    pub async fn get(&self, da_cert: &Bytes) -> Result<Vec<u8>, BatcherError> {
        let url = format!(
            "{}/get/0x{}?commitment_mode=standard",
            self.url,
            hex::encode(da_cert)
        );
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| BatcherError::Da(format!("get: {e}")))?;
        Self::body(response, "get").await
    }

    async fn body(response: reqwest::Response, op: &str) -> Result<Vec<u8>, BatcherError> {
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| BatcherError::Da(format!("{op}: {e}")))?;
        if !status.is_success() {
            return Err(BatcherError::Da(format!(
                "{op}: {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        Ok(bytes.to_vec())
    }
}

/// The proxy as a synchronous source. It blocks the calling tokio worker
/// in place, so the caller runs on a multi-thread runtime, as the
/// batcher and `kardamom-reconstruct` do.
impl PayloadSource for DaProxy {
    fn fetch_payload(&self, da_cert: &Bytes) -> Result<Vec<u8>, BatcherError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.get(da_cert))
        })
    }
}
