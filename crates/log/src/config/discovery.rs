//! The `[discovery]` section of [`LogConfig`](super::LogConfig): how a
//! service reaches the local Consul agent, which discovery scope it joins,
//! and the timing of its queries, checks, and reconciliation.
//!
//! Ports and the instance identity are not here. Nomad supplies them
//! through the environment; see [`crate::discovery::Instance`].

use std::net::Ipv4Addr;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Which local interface carries the advertised address. Parsed once at
/// the config boundary from either an interface name (`eth1`) or an IPv4
/// network in CIDR form (`192.168.56.0/24`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum InterfaceSelector {
    Name(String),
    Network { net: Ipv4Addr, prefix: u8 },
}

impl InterfaceSelector {
    /// Whether `ip` sits inside a `Network` selector. A `Name` selector
    /// matches by interface name instead; see
    /// [`crate::discovery::advertise_ip`].
    #[must_use]
    pub fn contains(&self, ip: Ipv4Addr) -> bool {
        match self {
            Self::Name(_) => false,
            Self::Network { net, prefix } => {
                let mask = u32::MAX.checked_shl(u32::from(32 - prefix)).unwrap_or(0);
                (u32::from(ip) & mask) == (u32::from(*net) & mask)
            }
        }
    }
}

impl TryFrom<String> for InterfaceSelector {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err("advertise_interface must not be empty".to_string());
        }
        let Some((net, prefix)) = value.split_once('/') else {
            return Ok(Self::Name(value));
        };
        let net: Ipv4Addr = net
            .parse()
            .map_err(|e| format!("advertise_interface {value}: bad network address: {e}"))?;
        let prefix: u8 = prefix
            .parse()
            .map_err(|e| format!("advertise_interface {value}: bad prefix length: {e}"))?;
        if prefix > 32 {
            return Err(format!(
                "advertise_interface {value}: prefix length must be at most 32"
            ));
        }
        Ok(Self::Network { net, prefix })
    }
}

impl From<InterfaceSelector> for String {
    fn from(value: InterfaceSelector) -> Self {
        match value {
            InterfaceSelector::Name(name) => name,
            InterfaceSelector::Network { net, prefix } => format!("{net}/{prefix}"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscoveryConfig {
    /// Off keeps every stream on the static `[channels]` URIs. On moves
    /// every migrated stream to dynamic MDC publications and discovered
    /// subscriptions.
    pub enabled: bool,
    /// The local Consul agent. Loopback is the intended local-agent
    /// endpoint, not a remote node address.
    pub consul_http_addr: String,
    /// A file holding the Consul ACL token. Read at startup, sent as the
    /// `X-Consul-Token` header, and never logged. Unset, the token comes
    /// from `CONSUL_HTTP_TOKEN`, else `CONSUL_TOKEN`, else no token is
    /// sent: the local profile runs Consul without ACLs.
    pub consul_token_file: Option<PathBuf>,
    /// Discovery isolation: a record from another cluster id or chain id
    /// is never joined.
    pub cluster_id: String,
    pub chain_id: u64,
    /// Consul datacenter of the catalog queries. Empty means the agent's
    /// own datacenter.
    pub datacenter: String,
    /// The interface whose address every publication control endpoint
    /// and receive endpoint binds and advertises. Required when enabled.
    pub advertise_interface: Option<InterfaceSelector>,
    /// One HTTP request's connect plus response budget, for registration,
    /// check, and non-blocking catalog requests.
    pub request_timeout_ms: NonZeroU64,
    /// How long one blocking catalog query waits for a change before it
    /// returns unchanged.
    pub blocking_wait_ms: NonZeroU64,
    /// Retry backoff after a failed catalog query, doubling from the
    /// minimum to the maximum.
    pub backoff_min_ms: NonZeroU64,
    pub backoff_max_ms: NonZeroU64,
    /// A publisher missing from a successful query is detached only once
    /// it has stayed missing for this long.
    pub removal_grace_ms: u64,
    /// The TTL of a registered publication's health check. The heartbeat
    /// passes it at one third of this interval.
    pub check_ttl_ms: NonZeroU64,
    /// Consul deletes a registration whose check has been critical for
    /// this long, which cleans up after a crash.
    pub deregister_after_ms: NonZeroU64,
    /// Aeron flow control strategy of every migrated publication, as the
    /// `fc` URI parameter (`max`, `min`, `tagged,g:...`). Empty keeps the
    /// driver default, which is the multicast default the static channels
    /// use today.
    pub flow_control: String,
}

impl DiscoveryConfig {
    /// Cross-field invariants serde cannot express. A disabled section
    /// needs nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if discovery is enabled with an empty cluster id,
    /// no advertise interface, a malformed Consul address, or a backoff
    /// maximum below its minimum.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if self.cluster_id.is_empty() {
            return Err("discovery.cluster_id must be set when discovery is enabled".to_string());
        }
        if self.advertise_interface.is_none() {
            return Err(
                "discovery.advertise_interface must be set when discovery is enabled".to_string(),
            );
        }
        if !self.consul_http_addr.starts_with("http://")
            && !self.consul_http_addr.starts_with("https://")
        {
            return Err(format!(
                "discovery.consul_http_addr must be an http(s) URL, got {}",
                self.consul_http_addr
            ));
        }
        if self.backoff_max_ms < self.backoff_min_ms {
            return Err("discovery.backoff_max_ms must be at least backoff_min_ms".to_string());
        }
        Ok(())
    }

    #[must_use]
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms.get())
    }

    #[must_use]
    pub fn blocking_wait(&self) -> Duration {
        Duration::from_millis(self.blocking_wait_ms.get())
    }

    #[must_use]
    pub fn backoff_min(&self) -> Duration {
        Duration::from_millis(self.backoff_min_ms.get())
    }

    #[must_use]
    pub fn backoff_max(&self) -> Duration {
        Duration::from_millis(self.backoff_max_ms.get())
    }

    #[must_use]
    pub fn removal_grace(&self) -> Duration {
        Duration::from_millis(self.removal_grace_ms)
    }

    #[must_use]
    pub fn check_ttl(&self) -> Duration {
        Duration::from_millis(self.check_ttl_ms.get())
    }

    #[must_use]
    pub fn deregister_after(&self) -> Duration {
        Duration::from_millis(self.deregister_after_ms.get())
    }
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            consul_http_addr: "http://127.0.0.1:8500".into(),
            consul_token_file: None,
            cluster_id: String::new(),
            chain_id: 0,
            datacenter: String::new(),
            advertise_interface: None,
            request_timeout_ms: NonZeroU64::new(5_000).expect("5000 != 0"),
            blocking_wait_ms: NonZeroU64::new(30_000).expect("30000 != 0"),
            backoff_min_ms: NonZeroU64::new(500).expect("500 != 0"),
            backoff_max_ms: NonZeroU64::new(10_000).expect("10000 != 0"),
            removal_grace_ms: 5_000,
            check_ttl_ms: NonZeroU64::new(10_000).expect("10000 != 0"),
            deregister_after_ms: NonZeroU64::new(60_000).expect("60000 != 0"),
            flow_control: String::new(),
        }
    }
}
