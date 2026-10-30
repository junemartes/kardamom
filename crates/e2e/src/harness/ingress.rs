//! The ingress replica types of the local stack: the park bound, the
//! spawn options, the identity of one replica, and a spawned replica.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};

use super::proc::Proc;

/// A pending-receipt park duration that must be nonzero: a zero here
/// would make several scenario timing assertions vacuously true instead
/// of testing anything (for example the `out.elapsed >= park / 2` lower
/// bound in `scenarios::nonce_gap`). Parsed once at construction; every
/// consumer reads a real `Duration` back with [`Self::as_duration`].
#[derive(Debug, Clone, Copy)]
pub struct ParkTimeout(std::num::NonZeroU64);

impl ParkTimeout {
    /// A park timeout of `secs` seconds. `NonZeroU64::saturating_mul` is
    /// a `const fn`: it cannot panic, and the result is already
    /// `NonZeroU64`, so this constructor cannot fail either.
    #[must_use]
    pub const fn from_secs(secs: std::num::NonZeroU64) -> Self {
        const MILLIS_PER_SEC: std::num::NonZeroU64 = std::num::NonZeroU64::new(1000).unwrap();
        Self(secs.saturating_mul(MILLIS_PER_SEC))
    }

    #[must_use]
    pub fn as_duration(self) -> Duration {
        Duration::from_millis(self.0.get())
    }
}

pub struct IngressOptions {
    pub pending_receipt_timeout: ParkTimeout,
    pub rpc_max_connections: std::num::NonZeroU32,
}

impl Default for IngressOptions {
    fn default() -> Self {
        const THIRTY_SECS: std::num::NonZeroU64 = std::num::NonZeroU64::new(30).unwrap();
        const DEFAULT_RPC_MAX_CONNECTIONS: std::num::NonZeroU32 =
            std::num::NonZeroU32::new(8192).unwrap();
        Self {
            pending_receipt_timeout: ParkTimeout::from_secs(THIRTY_SECS),
            rpc_max_connections: DEFAULT_RPC_MAX_CONNECTIONS,
        }
    }
}

pub struct SpawnedIngress {
    pub proc: Proc,
    pub metrics_addr: SocketAddr,
    pub rpc_url: String,
}

/// The identity of one ingress replica: its id, the ports that clients
/// and probes already hold, and its log file name.
#[derive(Debug, Clone)]
pub struct IngressSlot {
    /// `--ingress-id`. Each replica of an active/active pair has its own.
    pub id: u16,
    /// The RPC port. `None` picks a free port.
    pub rpc_port: Option<u16>,
    /// The metrics port. `None` picks a free port.
    pub metrics_port: Option<u16>,
    /// The log file name, without the `.log` extension. A respawn uses
    /// its own name, so the log of the process it replaces survives.
    pub log_name: String,
}

impl IngressSlot {
    /// The first replica of a fresh stack.
    #[must_use]
    pub fn first() -> Self {
        Self {
            id: 0,
            rpc_port: None,
            metrics_port: None,
            log_name: "ingress".to_string(),
        }
    }
}

impl SpawnedIngress {
    /// The RPC port, the text after the last colon of the RPC URL.
    ///
    /// # Errors
    /// Returns an error when the URL names no port.
    pub fn rpc_port(&self) -> Result<u16> {
        self.rpc_url
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .with_context(|| format!("no port in {}", self.rpc_url))
    }
}
