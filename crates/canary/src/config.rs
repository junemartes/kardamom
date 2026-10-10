//! The command line of `kardamom-canary` and the typed settings it
//! parses into.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::U256;
use clap::Parser;

/// One ingress instance: a name for the metric labels and its URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub name: String,
    pub url: String,
}

impl FromStr for Endpoint {
    type Err = String;

    /// `name=url`, or a bare URL, which names itself by its host and
    /// port.
    fn from_str(s: &str) -> Result<Self, String> {
        let (name, url) = s.split_once('=').unwrap_or_else(|| {
            let host = s.split_once("://").map_or(s, |(_, rest)| rest);
            (host.trim_end_matches('/'), s)
        });
        if name.is_empty() || url.is_empty() {
            return Err(format!("an endpoint is name=url or a URL, not {s:?}"));
        }
        Ok(Self {
            name: name.to_string(),
            url: url.to_string(),
        })
    }
}

/// A duration in milliseconds that is never zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Millis(NonZeroU64);

impl FromStr for Millis {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        s.parse::<NonZeroU64>()
            .map(Self)
            .map_err(|e| format!("a positive number of milliseconds, not {s:?}: {e}"))
    }
}

impl Millis {
    #[must_use]
    pub fn duration(self) -> Duration {
        Duration::from_millis(self.0.get())
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-canary",
    version,
    about = "kardamom canary: uses the chain as a user does and reports each result as a metric"
)]
pub struct Args {
    /// The ingress instances, comma separated, each `name=url` or a URL.
    /// The probes use each one in turn, never a load balancer.
    #[arg(
        long,
        env = "KARDAMOM_CANARY_INGRESS",
        value_delimiter = ',',
        required = true
    )]
    pub ingress: Vec<Endpoint>,
    /// The WebSocket URL of the notifier's status feed.
    #[arg(long, env = "KARDAMOM_CANARY_NOTIFIER_WS")]
    pub notifier_ws: String,
    /// The BIP-39 phrase of the L2 ring.
    #[arg(long, env = "KARDAMOM_CANARY_MNEMONIC", hide_env_values = true)]
    pub mnemonic: String,
    /// The number of ring accounts.
    #[arg(long, env = "KARDAMOM_CANARY_RING_SIZE", default_value = "4")]
    pub ring_size: NonZeroU32,
    /// The derivation index of the first ring account.
    #[arg(long, env = "KARDAMOM_CANARY_RING_OFFSET", default_value_t = 0)]
    pub ring_offset: u32,
    /// The directory of the nonce journal and the contract addresses.
    #[arg(
        long,
        env = "KARDAMOM_CANARY_DIR",
        default_value = "/opt/kardamom/canary"
    )]
    pub dir: PathBuf,
    /// The time between two `transfer` runs.
    #[arg(long, env = "KARDAMOM_CANARY_TRANSFER_MS", default_value = "5000")]
    pub transfer_ms: Millis,
    /// The time between two `read` runs.
    #[arg(long, env = "KARDAMOM_CANARY_READ_MS", default_value = "15000")]
    pub read_ms: Millis,
    /// The time between two `contract` runs.
    #[arg(long, env = "KARDAMOM_CANARY_CONTRACT_MS", default_value = "60000")]
    pub contract_ms: Millis,
    /// The time between two balance reads.
    #[arg(long, env = "KARDAMOM_CANARY_BALANCE_MS", default_value = "60000")]
    pub balance_ms: Millis,
    /// The longest wait for a receipt after the submit.
    #[arg(
        long,
        env = "KARDAMOM_CANARY_RECEIPT_TIMEOUT_MS",
        default_value = "30000"
    )]
    pub receipt_timeout_ms: Millis,
    /// The longest wait for a late status event after the receipt.
    #[arg(long, env = "KARDAMOM_CANARY_FEED_GRACE_MS", default_value = "5000")]
    pub feed_grace_ms: Millis,
    /// The longest wait for a state read to show a write.
    #[arg(long, env = "KARDAMOM_CANARY_READ_TIMEOUT_MS", default_value = "10000")]
    pub read_timeout_ms: Millis,
    /// The time between two polls of a receipt or a read.
    #[arg(long, env = "KARDAMOM_CANARY_POLL_MS", default_value = "100")]
    pub poll_ms: Millis,
    /// The lowest L2 balance, in wei, at which a ring account still
    /// sends. Under it the account is unfunded.
    #[arg(
        long,
        env = "KARDAMOM_CANARY_L2_FLOOR_WEI",
        default_value = "100000000000000"
    )]
    pub l2_floor_wei: U256,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9012")]
    pub metrics_addr: SocketAddr,
    /// Host identifier. Stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    pub host_id: String,
}

/// The cadences and the deadlines of the probes.
#[derive(Debug, Clone, Copy)]
pub struct Timing {
    pub transfer: Duration,
    pub read: Duration,
    pub contract: Duration,
    pub balance: Duration,
    pub receipt_timeout: Duration,
    pub feed_grace: Duration,
    pub read_timeout: Duration,
    pub poll: Duration,
}

impl Args {
    #[must_use]
    pub fn timing(&self) -> Timing {
        Timing {
            transfer: self.transfer_ms.duration(),
            read: self.read_ms.duration(),
            contract: self.contract_ms.duration(),
            balance: self.balance_ms.duration(),
            receipt_timeout: self.receipt_timeout_ms.duration(),
            feed_grace: self.feed_grace_ms.duration(),
            read_timeout: self.read_timeout_ms.duration(),
            poll: self.poll_ms.duration(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_endpoint_parses_with_and_without_a_name() {
        let named: Endpoint = "ingress-0=http://10.0.0.1:8545".parse().unwrap();
        assert_eq!(named.name, "ingress-0");
        assert_eq!(named.url, "http://10.0.0.1:8545");
        let bare: Endpoint = "http://127.0.0.1:8545/".parse().unwrap();
        assert_eq!(bare.name, "127.0.0.1:8545");
        assert!("=http://x".parse::<Endpoint>().is_err());
    }

    #[test]
    fn a_zero_duration_does_not_parse() {
        assert!("0".parse::<Millis>().is_err());
        assert_eq!(
            "250".parse::<Millis>().unwrap().duration(),
            Duration::from_millis(250)
        );
    }
}
