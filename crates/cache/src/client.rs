//! The Redis client: the account projection, the receipt index, and the
//! mirror heads.
//!
//! One `MultiplexedConnection` serves every task; it is `Clone`, and the
//! handle swaps it for a fresh one after a failure. In Sentinel mode the
//! fresh connection comes from asking the sentinels for the primary again,
//! so a failover heals on the next command. The swap goes through an
//! `ArcSwap` and a single-flight flag, never a lock.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256};
use arc_swap::ArcSwap;
use kardamom_types::{AccountRow, BPosition, Receipt, TX_TYPE_XCHAIN};
use redis::aio::MultiplexedConnection;
use redis::sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType};
use redis::{RedisConnectionInfo, Script};
use tracing::{info, warn};

use crate::config::CacheConfig;
use crate::error::CacheError;
use crate::{keys, metrics, script};

/// The bound of one connection attempt is this many command timeouts.
/// A frozen primary accepts the TCP connection and never answers the
/// handshake; without a bound the reconnect would wait for the thaw, and
/// the reader would never follow a sentinel failover.
const CONNECT_TIMEOUTS: u32 = 10;

fn connect_timeout(cfg: &CacheConfig) -> Duration {
    cfg.timeout().saturating_mul(CONNECT_TIMEOUTS)
}

/// One connection attempt within `bound`.
async fn connect_within(
    source: &Source,
    bound: Duration,
) -> Result<MultiplexedConnection, CacheError> {
    tokio::time::timeout(bound, source.connect())
        .await
        .map_err(|_| CacheError::Timeout)?
}

/// One account as the cache holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountView {
    pub nonce: u64,
    pub balance: U256,
    /// The canonical index of the batch end that wrote this value.
    pub tx_idx: u64,
}

/// The outcome of one `write_rows` pipeline, by row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowsWritten {
    pub applied: u64,
    pub discarded: u64,
    pub disagreed: u64,
}

impl RowsWritten {
    fn from_codes(codes: &[i64]) -> Self {
        codes.iter().fold(Self::default(), |mut acc, code| {
            match *code {
                script::APPLIED => acc.applied += 1,
                script::DISAGREED => acc.disagreed += 1,
                _ => acc.discarded += 1,
            }
            acc
        })
    }
}

/// Where a fresh connection comes from.
enum Source {
    Url(redis::Client),
    Sentinel {
        addrs: Vec<String>,
        master_name: String,
        node: SentinelNodeConnectionInfo,
    },
}

impl Source {
    async fn connect(&self) -> Result<MultiplexedConnection, CacheError> {
        match self {
            Self::Url(client) => Ok(client.get_multiplexed_async_connection().await?),
            Self::Sentinel {
                addrs,
                master_name,
                node,
            } => {
                let mut client = SentinelClient::build(
                    addrs.clone(),
                    master_name.clone(),
                    Some(node.clone()),
                    SentinelServerType::Master,
                )?;
                Ok(client.get_async_connection().await?)
            }
        }
    }
}

/// The shared client. Cheap to clone; every clone shares the connection
/// and the reconnect flag.
#[derive(Clone)]
pub struct AccountCache {
    conn: Arc<ArcSwap<MultiplexedConnection>>,
    source: Arc<Source>,
    reconnecting: Arc<AtomicBool>,
    timeout: Duration,
    /// The bound of one connection attempt, sentinel lookup included.
    connect_timeout: Duration,
    receipt_ttl: Duration,
    head_ttl: Duration,
    apply_row: Arc<Script>,
}

impl AccountCache {
    /// Connect per `cfg`. Sentinels take precedence over the direct URL.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Disabled`] when no address is configured,
    /// [`CacheError::MissingPassword`] when the password variable is
    /// named but unset, and the Redis error when the first connection
    /// fails.
    pub async fn connect(cfg: &CacheConfig) -> Result<Self, CacheError> {
        let source = Self::source(cfg)?;
        let conn = connect_within(&source, connect_timeout(cfg)).await?;
        info!(sentinel = !cfg.sentinels.is_empty(), "cache connected");
        Ok(Self {
            conn: Arc::new(ArcSwap::from_pointee(conn)),
            source: Arc::new(source),
            reconnecting: Arc::new(AtomicBool::new(false)),
            timeout: cfg.timeout(),
            connect_timeout: connect_timeout(cfg),
            receipt_ttl: Duration::from_secs(cfg.receipt_ttl_secs.get()),
            head_ttl: Duration::from_secs(cfg.head_ttl_secs.get()),
            apply_row: Arc::new(Script::new(script::APPLY_ROW)),
        })
    }

    /// Connect per `cfg`, and keep trying every `retry` while the
    /// Redis layer names no primary. After a failover the sentinels can
    /// disagree for minutes, and a process that exits then spends its
    /// Nomad restart budget in seconds: three exits inside one minute
    /// stop the task for 40 s. A caller that must have Redis waits
    /// instead.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::Disabled`] when no address is configured,
    /// and [`CacheError::MissingPassword`] when the password variable
    /// is named but unset. Both are configuration faults, so neither
    /// retries.
    pub async fn connect_waiting(cfg: &CacheConfig, retry: Duration) -> Result<Self, CacheError> {
        loop {
            let error = match Self::connect(cfg).await {
                Ok(cache) => return Ok(cache),
                Err(e @ (CacheError::Disabled | CacheError::MissingPassword(_))) => return Err(e),
                Err(e) => e,
            };
            warn!(error = %error, "cache not reachable yet; waiting");
            tokio::time::sleep(retry).await;
        }
    }

    fn source(cfg: &CacheConfig) -> Result<Source, CacheError> {
        let password = cfg.password()?;
        let auth = |info: RedisConnectionInfo| {
            with_auth(info, cfg.username.as_deref(), password.as_deref())
        };
        if !cfg.sentinels.is_empty() {
            let node = SentinelNodeConnectionInfo::default()
                .set_redis_connection_info(auth(RedisConnectionInfo::default()));
            return Ok(Source::Sentinel {
                addrs: cfg.sentinels.clone(),
                master_name: cfg.master_name.clone(),
                node,
            });
        }
        let url = cfg.url.as_deref().ok_or(CacheError::Disabled)?;
        let info = redis::Client::open(url)?.get_connection_info().clone();
        let redis = auth(info.redis_settings().clone());
        Ok(Source::Url(redis::Client::open(
            info.set_redis_settings(redis),
        )?))
    }

    /// Run one command against the current connection, within the
    /// timeout. A failure schedules one reconnect and reports the error;
    /// the caller degrades.
    async fn run<T: redis::FromRedisValue>(&self, cmd: &redis::Cmd) -> Result<T, CacheError> {
        let mut conn = (**self.conn.load()).clone();
        let result = tokio::time::timeout(self.timeout, cmd.query_async::<T>(&mut conn)).await;
        self.settle(result)
    }

    /// Run one pipeline. Same rule as [`Self::run`].
    async fn run_pipe<T: redis::FromRedisValue>(
        &self,
        pipe: &redis::Pipeline,
    ) -> Result<T, CacheError> {
        let mut conn = (**self.conn.load()).clone();
        let result = tokio::time::timeout(self.timeout, pipe.query_async::<T>(&mut conn)).await;
        self.settle(result)
    }

    /// Load the row script on the current connection's server.
    async fn load_script(&self) -> Result<(), CacheError> {
        let mut conn = (**self.conn.load()).clone();
        let result = tokio::time::timeout(
            self.timeout,
            self.apply_row.prepare_invoke().load_async(&mut conn),
        )
        .await;
        self.settle(result).map(|_sha: String| ())
    }

    /// Map a timed command's outcome and schedule a reconnect on failure.
    fn settle<T>(
        &self,
        result: Result<redis::RedisResult<T>, tokio::time::error::Elapsed>,
    ) -> Result<T, CacheError> {
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => {
                self.reconnect_later();
                Err(CacheError::Redis(e))
            }
            Err(_) => {
                self.reconnect_later();
                Err(CacheError::Timeout)
            }
        }
    }

    /// Whether a reconnect is in flight. A reader skips its reads
    /// meanwhile: each would pay the full timeout for nothing.
    #[must_use]
    pub fn reconnecting(&self) -> bool {
        self.reconnecting.load(Ordering::Acquire)
    }

    /// Replace the connection in the background, once per failure burst.
    fn reconnect_later(&self) {
        let claimed = self
            .reconnecting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if !claimed {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            match connect_within(&this.source, this.connect_timeout).await {
                Ok(conn) => {
                    this.conn.store(Arc::new(conn));
                    info!("cache reconnected");
                }
                Err(e) => warn!(error = %e, "cache reconnect failed; next failure retries"),
            }
            this.reconnecting.store(false, Ordering::Release);
        });
    }

    /// The account's projected state. `None` when the cache has no entry.
    ///
    /// # Errors
    ///
    /// Returns the Redis error or [`CacheError::Timeout`]; the caller
    /// treats both as unknown.
    pub async fn account(&self, address: Address) -> Result<Option<AccountView>, CacheError> {
        let started = Instant::now();
        let fields: HashMap<String, String> = self
            .run(redis::cmd("HGETALL").arg(keys::account(address)))
            .await?;
        metrics::record_lookup_seconds(started.elapsed().as_secs_f64());
        if fields.is_empty() {
            return Ok(None);
        }
        parse_view(&fields).map(Some)
    }

    /// Write a batch's rows through the monotone rule, in one pipeline.
    ///
    /// # Errors
    ///
    /// Returns the Redis error or [`CacheError::Timeout`]. The rows are
    /// then not written; the mirror retries the batch.
    pub async fn write_rows(
        &self,
        end: BPosition,
        rows: &[AccountRow],
    ) -> Result<RowsWritten, CacheError> {
        if rows.is_empty() {
            return Ok(RowsWritten::default());
        }
        let tag = keys::position_tag(end);
        // A pipeline calls EVALSHA and does not load a missing script, and
        // a fresh primary after a failover has none. One SCRIPT LOAD per
        // batch is one round trip; a batch lands every few milliseconds.
        self.load_script().await?;
        let mut pipe = redis::pipe();
        for row in rows {
            let mut invocation = self.apply_row.key(keys::account(row.address));
            invocation
                .arg(&tag)
                .arg(row.nonce)
                .arg(row.balance.to_string());
            pipe.invoke_script(&invocation);
        }
        let codes: Vec<i64> = self.run_pipe(&pipe).await?;
        let written = RowsWritten::from_codes(&codes);
        metrics::record_rows_written(&written);
        Ok(written)
    }

    /// Index a batch's receipts by `(sender, nonce)`, with the receipt
    /// TTL. Deposits and cross-chain deliveries carry no sender nonce and
    /// are skipped.
    ///
    /// # Errors
    ///
    /// Returns the Redis error or [`CacheError::Timeout`], or
    /// [`CacheError::Codec`] when a receipt does not encode.
    pub async fn write_receipts(&self, receipts: &[Receipt]) -> Result<(), CacheError> {
        let mut pipe = redis::pipe();
        let indexed = receipts
            .iter()
            .filter(|r| !r.is_deposit() && r.tx_type != TX_TYPE_XCHAIN);
        for receipt in indexed {
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(receipt)
                .map_err(|e| CacheError::Codec(e.to_string()))?;
            pipe.cmd("SET")
                .arg(keys::receipt(receipt.from, receipt.nonce))
                .arg(bytes.as_slice())
                .arg("EX")
                .arg(self.receipt_ttl.as_secs())
                .ignore();
        }
        if pipe.cmd_iter().next().is_none() {
            return Ok(());
        }
        self.run_pipe::<()>(&pipe).await
    }

    /// The indexed receipt of `(sender, nonce)`, when one is stored.
    ///
    /// # Errors
    ///
    /// Returns the Redis error, [`CacheError::Timeout`], or
    /// [`CacheError::Codec`] when the stored bytes do not decode.
    pub async fn receipt(
        &self,
        sender: Address,
        nonce: u64,
    ) -> Result<Option<Receipt>, CacheError> {
        let bytes: Option<Vec<u8>> = self
            .run(redis::cmd("GET").arg(keys::receipt(sender, nonce)))
            .await?;
        bytes
            .map(|b| {
                rkyv::from_bytes::<Receipt, rkyv::rancor::Error>(&b)
                    .map_err(|e| CacheError::Codec(e.to_string()))
            })
            .transpose()
    }

    /// The heads of the mirrors `ids`, as canonical indexes. A dead
    /// mirror's head has aged out and reads `None`.
    ///
    /// # Errors
    ///
    /// Returns the Redis error or [`CacheError::Timeout`].
    pub async fn heads(&self, ids: &[u32]) -> Result<Vec<Option<u64>>, CacheError> {
        let mut cmd = redis::cmd("MGET");
        for id in ids {
            cmd.arg(keys::head(*id));
        }
        let tags: Vec<Option<String>> = self.run(&cmd).await?;
        Ok(tags
            .iter()
            .map(|tag| tag.as_deref().and_then(keys::parse_tag))
            .collect())
    }

    /// Publish this mirror's head with the liveness TTL.
    ///
    /// # Errors
    ///
    /// Returns the Redis error or [`CacheError::Timeout`].
    pub async fn set_head(&self, id: u32, head: BPosition) -> Result<(), CacheError> {
        self.run(
            redis::cmd("SET")
                .arg(keys::head(id))
                .arg(keys::position_tag(head))
                .arg("EX")
                .arg(self.head_ttl.as_secs()),
        )
        .await
    }

    /// Publish the sequencer's optimistic floor of `address`. Informational.
    ///
    /// # Errors
    ///
    /// Returns the Redis error or [`CacheError::Timeout`].
    pub async fn set_pending(&self, address: Address, nonce: u64) -> Result<(), CacheError> {
        self.run(
            redis::cmd("SET")
                .arg(keys::pending(address))
                .arg(nonce)
                .arg("EX")
                .arg(60u64),
        )
        .await
    }

    /// The optimistic floor of `address`, when the sequencer published one.
    ///
    /// # Errors
    ///
    /// Returns the Redis error or [`CacheError::Timeout`].
    pub async fn pending(&self, address: Address) -> Result<Option<u64>, CacheError> {
        self.run(redis::cmd("GET").arg(keys::pending(address)))
            .await
    }

    /// Wait for one replica to acknowledge the writes so far. Returns how
    /// many replicas acknowledged: zero when none caught up in half the
    /// command timeout, which is not an error. The mirror counts a zero
    /// and goes on.
    ///
    /// # Errors
    ///
    /// Returns the Redis error or [`CacheError::Timeout`].
    pub async fn wait_replica(&self) -> Result<u32, CacheError> {
        // The server-side wait ends before the client timeout, so a
        // replica-less primary answers zero instead of timing out.
        let millis = u64::try_from(self.timeout.as_millis() / 2).unwrap_or(u64::MAX);
        self.run(redis::cmd("WAIT").arg(1).arg(millis)).await
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

/// Decode an account hash. A field that does not parse is a codec error:
/// the writer is the only one that writes these fields.
fn parse_view(fields: &HashMap<String, String>) -> Result<AccountView, CacheError> {
    let field = |name: &str| {
        fields
            .get(name)
            .ok_or_else(|| CacheError::Codec(format!("account hash has no {name}")))
    };
    let nonce = field("nonce")?
        .parse::<u64>()
        .map_err(|e| CacheError::Codec(format!("nonce: {e}")))?;
    let balance = U256::from_str_radix(field("balance")?, 10)
        .map_err(|e| CacheError::Codec(format!("balance: {e}")))?;
    let tx_idx = keys::parse_tag(field("tx_idx")?)
        .ok_or_else(|| CacheError::Codec("tx_idx is not a position tag".into()))?;
    Ok(AccountView {
        nonce,
        balance,
        tx_idx,
    })
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn an_unreachable_cache_waits_instead_of_failing() {
        let cfg = CacheConfig {
            url: Some("redis://127.0.0.1:1/".to_string()),
            ..CacheConfig::default()
        };
        let waited = tokio::time::timeout(
            Duration::from_millis(300),
            AccountCache::connect_waiting(&cfg, Duration::from_millis(20)),
        )
        .await;
        assert!(waited.is_err(), "the connect must keep waiting");
    }

    #[tokio::test]
    async fn a_cache_with_no_address_fails_at_once() {
        let outcome =
            AccountCache::connect_waiting(&CacheConfig::default(), Duration::from_secs(60)).await;
        let Err(error) = outcome else {
            panic!("a cache with no address must fail");
        };
        assert!(matches!(error, CacheError::Disabled), "got {error:?}");
    }

    use super::*;

    #[test]
    fn codes_fold_into_counts() {
        let written = RowsWritten::from_codes(&[1, 1, 0, -1, 7]);
        assert_eq!(
            written,
            RowsWritten {
                applied: 2,
                discarded: 2,
                disagreed: 1
            }
        );
    }

    #[test]
    fn a_hash_parses_and_a_broken_one_is_a_codec_error() {
        let mut fields = HashMap::new();
        fields.insert("nonce".to_string(), "7".to_string());
        fields.insert("balance".to_string(), U256::MAX.to_string());
        fields.insert("tx_idx".to_string(), keys::index_tag(42));
        let view = parse_view(&fields).unwrap();
        assert_eq!(view.nonce, 7);
        assert_eq!(view.balance, U256::MAX);
        assert_eq!(view.tx_idx, 42);
        fields.insert("tx_idx".to_string(), "42".to_string());
        assert!(matches!(parse_view(&fields), Err(CacheError::Codec(_))));
    }
}
