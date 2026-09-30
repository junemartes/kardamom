//! The beacon API: the blob sidecars of a slot, checked against the
//! versioned hashes L1 committed to.
//!
//! The execution layer keeps no blob bytes; the consensus layer serves
//! them for about 18 days (`/eth/v1/beacon/blob_sidecars/{slot}`). The
//! indexer reads them while they are there and keeps them for good. A
//! sidecar's commitment hashes to a versioned hash; only a blob whose
//! hash L1 named is stored, so the beacon endpoint is a source of bytes,
//! not of truth.

use std::num::NonZeroU64;
use std::time::Duration;

use alloy_eips::eip4844::{Blob, kzg_to_versioned_hash};
use alloy_primitives::B256;
use serde::Deserialize;

use crate::IndexerError;

/// A beacon API endpoint and the chain's slot arithmetic.
#[derive(Clone, Debug)]
pub struct BeaconApi {
    base: String,
    client: reqwest::Client,
    genesis_time: u64,
    seconds_per_slot: NonZeroU64,
}

#[derive(Deserialize)]
struct Genesis {
    data: GenesisData,
}

#[derive(Deserialize)]
struct GenesisData {
    genesis_time: String,
}

#[derive(Deserialize)]
struct Sidecars {
    data: Vec<Sidecar>,
}

#[derive(Deserialize)]
struct Sidecar {
    blob: String,
    kzg_commitment: String,
}

impl Sidecar {
    fn versioned_hash(&self) -> Result<B256, IndexerError> {
        decode_hex(&self.kzg_commitment).map(|c| kzg_to_versioned_hash(&c))
    }

    fn blob(&self) -> Result<Blob, IndexerError> {
        let bytes = decode_hex(&self.blob)?;
        Blob::try_from(bytes.as_slice()).map_err(|e| IndexerError::Beacon(format!("blob: {e}")))
    }
}

impl BeaconApi {
    /// Connect to `base` and read the chain's genesis time from it.
    ///
    /// # Errors
    /// Returns an error when the endpoint does not answer `/eth/v1/beacon/genesis`.
    pub async fn connect(base: &str, seconds_per_slot: NonZeroU64) -> Result<Self, IndexerError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| IndexerError::Beacon(e.to_string()))?;
        let base = base.trim_end_matches('/').to_string();
        let genesis: Genesis = get_json(&client, &format!("{base}/eth/v1/beacon/genesis")).await?;
        let genesis_time = genesis
            .data
            .genesis_time
            .parse()
            .map_err(|e| IndexerError::Beacon(format!("genesis_time: {e}")))?;
        Ok(Self {
            base,
            client,
            genesis_time,
            seconds_per_slot,
        })
    }

    /// The slot of an execution block, from its timestamp. A timestamp
    /// before genesis is slot 0: the chain has no block there, and the
    /// beacon API answers "not found" for it.
    #[must_use]
    pub fn slot_of(&self, timestamp: u64) -> u64 {
        timestamp.saturating_sub(self.genesis_time) / self.seconds_per_slot
    }

    /// The blobs of `slot` under the versioned hashes `wanted`, in
    /// `wanted`'s order. A wanted hash the slot does not carry is an
    /// error: the bytes are not where L1 says they are.
    ///
    /// # Errors
    /// Returns an error when the endpoint fails, a sidecar does not decode,
    /// or a wanted blob is missing.
    pub async fn blobs_of(
        &self,
        slot: u64,
        wanted: &[B256],
    ) -> Result<Vec<(B256, Blob)>, IndexerError> {
        let url = format!("{}/eth/v1/beacon/blob_sidecars/{slot}", self.base);
        let sidecars: Sidecars = get_json(&self.client, &url).await?;
        let carried = sidecars
            .data
            .iter()
            .map(|s| s.versioned_hash().map(|h| (h, s)))
            .collect::<Result<Vec<_>, _>>()?;
        wanted
            .iter()
            .map(|hash| {
                let (_, sidecar) =
                    carried
                        .iter()
                        .find(|(h, _)| h == hash)
                        .ok_or(IndexerError::BlobMissing {
                            slot,
                            versioned_hash: *hash,
                        })?;
                sidecar.blob().map(|blob| (*hash, blob))
            })
            .collect()
    }
}

async fn get_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
) -> Result<T, IndexerError> {
    client
        .get(url)
        .send()
        .await
        .map_err(|e| IndexerError::Beacon(format!("{url}: {e}")))?
        .error_for_status()
        .map_err(|e| IndexerError::Beacon(format!("{url}: {e}")))?
        .json()
        .await
        .map_err(|e| IndexerError::Beacon(format!("{url}: {e}")))
}

fn decode_hex(s: &str) -> Result<Vec<u8>, IndexerError> {
    alloy_primitives::hex::decode(s).map_err(|e| IndexerError::Beacon(format!("hex: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_comes_from_the_timestamp() {
        let api = BeaconApi {
            base: String::new(),
            client: reqwest::Client::new(),
            genesis_time: 1_655_733_600,
            seconds_per_slot: NonZeroU64::new(12).unwrap(),
        };
        assert_eq!(api.slot_of(1_655_733_600), 0);
        assert_eq!(api.slot_of(1_655_733_600 + 12 * 11_250_848), 11_250_848);
        assert_eq!(api.slot_of(0), 0);
    }
}
