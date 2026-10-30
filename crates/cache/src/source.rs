//! Where a fresh Redis connection comes from: one URL, or the primary
//! that the Sentinels name.

use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use redis::sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};
use redis::{ConnectionAddr, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo};
use tracing::warn;

use crate::config::CacheConfig;
use crate::error::CacheError;

/// The bound of one connection attempt is this many command timeouts.
/// A frozen primary accepts the TCP connection and never answers the
/// handshake; without a bound the reconnect would wait for the thaw, and
/// the reader would never follow a sentinel failover.
const CONNECT_TIMEOUTS: u32 = 10;

/// Where a fresh connection comes from.
pub(crate) enum Source {
    Url(redis::Client),
    Sentinel {
        sentinels: Vec<ConnectionInfo>,
        master_name: String,
        node: SentinelNodeConnectionInfo,
    },
}

impl Source {
    /// The source per `cfg`. Sentinels take precedence over the direct
    /// URL.
    pub(crate) fn of(cfg: &CacheConfig) -> Result<Self, CacheError> {
        let password = cfg.password()?;
        let auth = |info: RedisConnectionInfo| {
            with_auth(info, cfg.username.as_deref(), password.as_deref())
        };
        if !cfg.sentinels.is_empty() {
            let node = SentinelNodeConnectionInfo::default()
                .set_redis_connection_info(auth(RedisConnectionInfo::default()));
            let sentinels = cfg
                .sentinels
                .iter()
                .map(|url| url.as_str().into_connection_info())
                .collect::<Result<_, _>>()?;
            return Ok(Self::Sentinel {
                sentinels,
                master_name: cfg.master_name.clone(),
                node,
            });
        }
        let url = cfg.url.as_deref().ok_or(CacheError::Disabled)?;
        let info = redis::Client::open(url)?.get_connection_info().clone();
        let redis = auth(info.redis_settings().clone());
        Ok(Self::Url(redis::Client::open(
            info.set_redis_settings(redis),
        )?))
    }

    /// The bound of one connection attempt per `cfg`, Sentinel lookup
    /// included.
    pub(crate) fn bound(cfg: &CacheConfig) -> Duration {
        cfg.timeout().saturating_mul(CONNECT_TIMEOUTS)
    }

    /// One connection. Each attempt, to the URL or through one Sentinel,
    /// ends after `bound`.
    pub(crate) async fn connect(
        &self,
        bound: Duration,
    ) -> Result<MultiplexedConnection, CacheError> {
        match self {
            Self::Url(client) => {
                tokio::time::timeout(bound, client.get_multiplexed_async_connection())
                    .await
                    .map_err(|_| CacheError::Timeout)?
                    .map_err(CacheError::from)
            }
            Self::Sentinel {
                sentinels,
                master_name,
                node,
            } => {
                let ask = Ask {
                    master_name,
                    node,
                    bound,
                };
                ask.any(&Self::expand(sentinels, bound).await).await
            }
        }
    }

    /// Every address of every configured Sentinel, in resolver order,
    /// each once. A deployment names its Sentinels with one service
    /// name that has one DNS record for each Sentinel. The client
    /// library connects to the first record only, so one hung Sentinel
    /// at that record would hide the others. The names resolve again at
    /// each connection, so a moved Sentinel is found. Each lookup ends
    /// after `bound`: a slow DNS server must not hold the reconnect, and
    /// with it the reads that skip Redis meanwhile, past the bound of the
    /// connection attempt.
    async fn expand(sentinels: &[ConnectionInfo], bound: Duration) -> Vec<ConnectionInfo> {
        Self::expand_by(sentinels, bound, |host, port| async move {
            let found = tokio::net::lookup_host((host.as_str(), port)).await?;
            Ok(found.collect::<Vec<_>>().into_iter())
        })
        .await
    }

    /// [`Self::expand`] with the address lookup `lookup`. Tests give a
    /// lookup that never answers.
    async fn expand_by<F, A>(
        sentinels: &[ConnectionInfo],
        bound: Duration,
        lookup: impl Fn(String, u16) -> F,
    ) -> Vec<ConnectionInfo>
    where
        F: Future<Output = std::io::Result<A>>,
        A: Iterator<Item = SocketAddr>,
    {
        let mut expanded = Vec::new();
        for info in sentinels {
            Self::add_new(&mut expanded, Self::resolve(info, bound, &lookup).await);
        }
        expanded
    }

    /// One configured Sentinel as one entry for each address of its
    /// host. A name that does not resolve within `bound` stays as it is,
    /// so the connection attempt reports the error. A lookup that times
    /// out counts as one that failed.
    async fn resolve<F, A>(
        info: &ConnectionInfo,
        bound: Duration,
        lookup: &impl Fn(String, u16) -> F,
    ) -> Vec<ConnectionInfo>
    where
        F: Future<Output = std::io::Result<A>>,
        A: Iterator<Item = SocketAddr>,
    {
        let ConnectionAddr::Tcp(host, port) = info.addr() else {
            return vec![info.clone()];
        };
        let Ok(Ok(found)) = tokio::time::timeout(bound, lookup(host.clone(), *port)).await else {
            return vec![info.clone()];
        };
        let at = |addr: SocketAddr| {
            info.clone()
                .set_addr(ConnectionAddr::Tcp(addr.ip().to_string(), addr.port()))
        };
        let resolved: Vec<ConnectionInfo> = found.map(at).collect();
        if resolved.is_empty() {
            return vec![info.clone()];
        }
        resolved
    }

    fn add_new(expanded: &mut Vec<ConnectionInfo>, resolved: Vec<ConnectionInfo>) {
        let new: Vec<ConnectionInfo> = resolved
            .into_iter()
            .filter(|info| !expanded.iter().any(|seen| seen.addr() == info.addr()))
            .collect();
        expanded.extend(new);
    }
}

/// One question to the Sentinels: the live primary of `master_name`.
struct Ask<'a> {
    master_name: &'a str,
    node: &'a SentinelNodeConnectionInfo,
    bound: Duration,
}

impl Ask<'_> {
    /// The primary as the first sentinel that names a live one sees it.
    ///
    /// The client library asks the sentinels in order and keeps the
    /// answer of the first that replies, even when that answer names a
    /// replica or a primary the sentinel has marked down. After two
    /// failovers in 30 s one sentinel kept naming the demoted node for
    /// ten minutes, so every connection failed while the other two
    /// sentinels named the live primary. So ask each sentinel on its
    /// own, and keep the first answer that a primary accepts. A hung
    /// Sentinel accepts the TCP connection and never answers, so each
    /// question ends after the bound, and all questions run at once: a
    /// hung Sentinel delays no other. `sentinels` is never empty: the
    /// source has at least one configured Sentinel, and each one expands
    /// to at least itself.
    async fn any(&self, sentinels: &[ConnectionInfo]) -> Result<MultiplexedConnection, CacheError> {
        let questions = sentinels
            .iter()
            .map(|sentinel| Box::pin(self.named(sentinel)));
        let (conn, _slower) = futures::future::select_ok(questions).await?;
        Ok(conn)
    }

    /// One question, with a warning when its Sentinel names no live
    /// primary.
    async fn named(&self, sentinel: &ConnectionInfo) -> Result<MultiplexedConnection, CacheError> {
        self.one(sentinel)
            .await
            .map_err(|error| Self::named_none(sentinel, error))
    }

    async fn one(&self, sentinel: &ConnectionInfo) -> Result<MultiplexedConnection, CacheError> {
        let mut client = SentinelClient::build(
            vec![sentinel.clone()],
            self.master_name.to_string(),
            Some(self.node.clone()),
            SentinelServerType::Master,
        )?;
        tokio::time::timeout(self.bound, client.get_async_connection())
            .await
            .map_err(|_| CacheError::Timeout)?
            .map_err(CacheError::from)
    }

    fn named_none(sentinel: &ConnectionInfo, error: CacheError) -> CacheError {
        warn!(sentinel = %sentinel.addr(), error = %error, "sentinel named no live primary");
        error
    }
}

/// The ACL credentials from the config, over what the URL carried.
fn with_auth(
    mut info: RedisConnectionInfo,
    username: Option<&str>,
    password: Option<&str>,
) -> RedisConnectionInfo {
    if let Some(username) = username {
        info = info.set_username(username);
    }
    if let Some(password) = password {
        info = info.set_password(password);
    }
    info
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
