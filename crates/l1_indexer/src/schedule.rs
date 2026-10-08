//! The finality schedule: when the finalized tip moves next.
//!
//! The beacon chain finalizes once per epoch: the finalized checkpoint
//! moves on an epoch boundary, 32 slots of 12 s on Ethereum and Sepolia.
//! A read between two boundaries finds nothing new. So the follower
//! sleeps until the next boundary, which it computes from the chain's
//! genesis time and its slot length, then reads every slot until the tip
//! moves.
//!
//! A chain without a beacon API (anvil, with its fast finality) has no
//! schedule: the follower reads at a fixed interval.

use std::num::NonZeroU64;
use std::time::Duration;

use serde::Deserialize;

use crate::IndexerError;

/// The slot arithmetic of one beacon chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalitySchedule {
    /// The chain's genesis, in seconds since the Unix epoch.
    pub genesis_time: u64,
    pub seconds_per_slot: NonZeroU64,
    pub slots_per_epoch: NonZeroU64,
}

#[derive(Deserialize)]
struct Envelope<T> {
    data: T,
}

#[derive(Deserialize)]
struct Genesis {
    genesis_time: String,
}

#[derive(Deserialize)]
#[allow(
    clippy::struct_field_names,
    reason = "the beacon API names the fields this way"
)]
struct Spec {
    #[serde(rename = "SECONDS_PER_SLOT")]
    seconds_per_slot: String,
    #[serde(rename = "SLOTS_PER_EPOCH")]
    slots_per_epoch: String,
}

impl FinalitySchedule {
    /// Read the schedule from a beacon API: the genesis time from
    /// `/eth/v1/beacon/genesis`, the slot length and the epoch length
    /// from `/eth/v1/config/spec`.
    ///
    /// # Errors
    /// Returns [`IndexerError::Beacon`] when the endpoint does not answer
    /// or answers a value that is not a positive number.
    pub async fn from_beacon(base: &str) -> Result<Self, IndexerError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| IndexerError::Beacon(e.to_string()))?;
        let base = base.trim_end_matches('/');
        let genesis: Envelope<Genesis> =
            Self::get(&client, &format!("{base}/eth/v1/beacon/genesis")).await?;
        let spec: Envelope<Spec> =
            Self::get(&client, &format!("{base}/eth/v1/config/spec")).await?;
        Ok(Self {
            genesis_time: Self::number("genesis_time", &genesis.data.genesis_time)?,
            seconds_per_slot: Self::positive("SECONDS_PER_SLOT", &spec.data.seconds_per_slot)?,
            slots_per_epoch: Self::positive("SLOTS_PER_EPOCH", &spec.data.slots_per_epoch)?,
        })
    }

    async fn get<T: serde::de::DeserializeOwned>(
        client: &reqwest::Client,
        url: &str,
    ) -> Result<T, IndexerError> {
        client
            .get(url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| IndexerError::Beacon(e.without_url().to_string()))?
            .json()
            .await
            .map_err(|e| IndexerError::Beacon(e.without_url().to_string()))
    }

    fn number(name: &str, value: &str) -> Result<u64, IndexerError> {
        value
            .parse()
            .map_err(|e| IndexerError::Beacon(format!("{name}: {e}")))
    }

    fn positive(name: &str, value: &str) -> Result<NonZeroU64, IndexerError> {
        NonZeroU64::new(Self::number(name, value)?)
            .ok_or_else(|| IndexerError::Beacon(format!("{name} is 0")))
    }

    /// The first epoch boundary strictly after `now` (Unix seconds): the
    /// next time the finalized tip can move. Before genesis it is the
    /// genesis time.
    #[must_use]
    pub fn next_step_after(&self, now: u64) -> u64 {
        let epoch = self.seconds_per_slot.saturating_mul(self.slots_per_epoch);
        let Some(since) = now.checked_sub(self.genesis_time) else {
            return self.genesis_time;
        };
        // The boundary saturates at the end of time, where no follower
        // runs.
        (since / epoch)
            .saturating_add(1)
            .saturating_mul(epoch.get())
            .saturating_add(self.genesis_time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sepolia_like() -> FinalitySchedule {
        FinalitySchedule {
            genesis_time: 1_000,
            seconds_per_slot: NonZeroU64::new(12).unwrap(),
            slots_per_epoch: NonZeroU64::new(32).unwrap(),
        }
    }

    #[test]
    fn the_next_step_is_the_next_epoch_boundary() {
        let s = sepolia_like();
        assert_eq!(s.next_step_after(1_000), 1_384);
        assert_eq!(s.next_step_after(1_383), 1_384);
        assert_eq!(s.next_step_after(1_384), 1_768);
        assert_eq!(s.next_step_after(500), 1_000);
    }
    #[tokio::test]
    async fn a_failed_beacon_request_does_not_echo_the_key() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let error = FinalitySchedule::from_beacon(&format!("http://{address}/SECRET-SENTINEL"))
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("SECRET-SENTINEL"));
    }
}
