//! The Redis client: the account projection, the receipt index, and the
//! mirror heads.
//!
//! One `MultiplexedConnection` serves every task; it is `Clone`, and the
//! handle swaps it for a fresh one after a connection failure or a long
//! stall (see [`crate::link`]). In Sentinel mode the fresh connection
//! comes from asking the sentinels for the primary again, so a failover
//! heals on the next command. The swap goes through an `ArcSwap` and a
//! single-flight flag, never a lock.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256};
use arc_swap::ArcSwap;
use kardamom_types::{AccountRow, BPosition, Receipt, TX_TYPE_XCHAIN};
use redis::Script;
use redis::aio::MultiplexedConnection;
use tracing::{info, warn};

use crate::config::CacheConfig;
use crate::error::CacheError;
use crate::link::Link;
use crate::source::Source;
use crate::{keys, metrics, script};

/// The keys one `SCAN` page asks for. Redis may return more or fewer.
const SCAN_PAGE: u64 = 1_000;

/// The state of one key sweep: the next `SCAN` cursor, `None` once the
/// scan is done, and the keys deleted so far.
struct Sweep {
    next: Option<u64>,
    deleted: u64,
}

impl Default for Sweep {
    /// A sweep that starts at the first cursor.
    fn default() -> Self {
        Self {
            next: Some(0),
            deleted: 0,
        }
    }
}

/// One account as the cache holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountView {
    pub nonce: u64,
    pub balance: U256,
    /// The canonical index of the batch end that wrote this value.
    pub tx_idx: u64,
}

/// One batch of a [`AccountCache::write_batches`] pipeline: its account
/// rows at its end position, and its receipts.
#[derive(Clone, Copy, Debug)]
pub struct BatchWrite<'a> {
    pub end: BPosition,
    pub rows: &'a [AccountRow],
    pub receipts: &'a [Receipt],
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

/// The shared client. Cheap to clone; every clone shares the connection
/// and the reconnect flag.
#[derive(Clone)]
pub struct AccountCache {
    conn: Arc<ArcSwap<MultiplexedConnection>>,
    source: Arc<Source>,
    link: Arc<Link>,
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
        let source = Source::of(cfg)?;
        let conn = source.connect(Source::bound(cfg)).await?;
        info!(sentinel = !cfg.sentinels.is_empty(), "cache connected");
        Ok(Self {
            conn: Arc::new(ArcSwap::from_pointee(conn)),
            source: Arc::new(source),
            link: Arc::new(Link::new(cfg.timeout())),
            timeout: cfg.timeout(),
            connect_timeout: Source::bound(cfg),
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

    /// Run one command against the current connection, within the
    /// timeout. A failure reports the error, and the caller degrades. A
    /// failure that proves the connection dead or stalled schedules one
    /// reconnect.
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

    /// Map a timed command's outcome, and schedule a reconnect when
    /// the failure needs one.
    fn settle<T>(
        &self,
        result: Result<redis::RedisResult<T>, tokio::time::error::Elapsed>,
    ) -> Result<T, CacheError> {
        let error = match result {
            Ok(Ok(value)) => {
                self.link.replied();
                return Ok(value);
            }
            Ok(Err(e)) => CacheError::Redis(e),
            Err(_) => CacheError::Timeout,
        };
        if self.link.must_replace(&error) {
            self.reconnect_later();
        }
        Err(error)
    }

    /// Whether the connection failed with a connection error and no new
    /// one replaces it yet. A reader skips its reads meanwhile. A
    /// connection that only timed out is still live, and a reader keeps
    /// reading on it.
    #[must_use]
    pub fn connection_lost(&self) -> bool {
        self.link.lost()
    }

    /// Replace the connection in the background, once per failure burst.
    /// A failed attempt waits out a bounded backoff before the next
    /// failure may try again.
    fn reconnect_later(&self) {
        if !self.link.claim() {
            return;
        }
        crate::metrics::record_reconnect();
        let this = self.clone();
        tokio::spawn(async move {
            match this.source.connect(this.connect_timeout).await {
                Ok(conn) => {
                    this.conn.store(Arc::new(conn));
                    this.link.replaced();
                    info!("cache reconnected");
                }
                Err(e) => {
                    warn!(error = %e, "cache reconnect failed; next failure retries");
                    this.link.failed().await;
                }
            }
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
        self.write_batches(&[BatchWrite {
            end,
            rows,
            receipts: &[],
        }])
        .await
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
        self.write_batches(&[BatchWrite {
            end: BPosition::from_index(0),
            rows: &[],
            receipts,
        }])
        .await
        .map(|_| ())
    }

    /// Write the rows and the receipts of several batches in one pipeline.
    /// Each row goes through the monotone rule with the end position of its
    /// own batch, so the batches may come in any order and more than once.
    /// One pipeline is one round trip, where a write per batch costs one
    /// for each batch.
    ///
    /// # Errors
    ///
    /// Returns the Redis error or [`CacheError::Timeout`], or
    /// [`CacheError::Codec`] when a receipt does not encode. Nothing of the
    /// batches is then known to be written; the mirror retries them all.
    pub async fn write_batches(
        &self,
        batches: &[BatchWrite<'_>],
    ) -> Result<RowsWritten, CacheError> {
        let mut pipe = redis::pipe();
        let with_rows = batches.iter().any(|b| !b.rows.is_empty());
        for batch in batches {
            self.push_rows(&mut pipe, batch.end, batch.rows);
            self.push_receipts(&mut pipe, batch.receipts)?;
        }
        if pipe.cmd_iter().next().is_none() {
            return Ok(RowsWritten::default());
        }
        // A pipeline calls EVALSHA and does not load a missing script, and
        // a fresh primary after a failover has none. One SCRIPT LOAD per
        // pipeline is one round trip.
        if with_rows {
            self.load_script().await?;
        }
        let codes: Vec<i64> = self.run_pipe(&pipe).await?;
        let written = RowsWritten::from_codes(&codes);
        metrics::record_rows_written(&written);
        Ok(written)
    }

    /// Add the row script of each row to `pipe`, tagged with `end`. Each
    /// invocation answers one code.
    fn push_rows(&self, pipe: &mut redis::Pipeline, end: BPosition, rows: &[AccountRow]) {
        if rows.is_empty() {
            return;
        }
        let tag = keys::position_tag(end);
        for row in rows {
            let mut invocation = self.apply_row.key(keys::account(row.address));
            invocation
                .arg(&tag)
                .arg(row.nonce)
                .arg(row.balance.to_string());
            pipe.invoke_script(&invocation);
        }
    }

    /// Add a `SET` with the receipt TTL for each indexed receipt to
    /// `pipe`. The replies are ignored, so the codes of the pipeline stay
    /// the codes of the rows.
    fn push_receipts(
        &self,
        pipe: &mut redis::Pipeline,
        receipts: &[Receipt],
    ) -> Result<(), CacheError> {
        let indexed = receipts
            .iter()
            .filter(|r| !r.is_deposit() && r.tx_type != TX_TYPE_XCHAIN);
        for receipt in indexed {
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(receipt)
                .map_err(|e| CacheError::Codec(e.to_string()))?;
            let set = pipe
                .cmd("SET")
                .arg(keys::receipt(receipt.from, receipt.nonce))
                .arg(bytes.as_slice())
                .arg("EX")
                .arg(self.receipt_ttl.as_secs());
            // One receipt of a (sender, nonce) uses the nonce. A skip that
            // used none, such as a copy of a transaction that the sealer
            // ordered again, takes the key only while it has no receipt.
            if !receipt.uses_nonce() {
                set.arg("NX");
            }
            set.ignore();
        }
        Ok(())
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

    /// Delete the account projection and the receipt index: every
    /// `acct:` and `rcpt:` key. The heads and the pending floors stay.
    /// The mirror calls this after an executor rolls back: the rows of the
    /// lost blocks carry positions above the new stream, so the monotone
    /// rule would keep them. Returns how many keys it deleted.
    ///
    /// # Errors
    ///
    /// Returns the Redis error or [`CacheError::Timeout`]. The keys
    /// deleted before the failure stay deleted; the caller runs the whole
    /// clear again.
    pub async fn clear_projection(&self) -> Result<u64, CacheError> {
        let mut deleted = 0u64;
        for pattern in [keys::ACCOUNT_PATTERN, keys::RECEIPT_PATTERN] {
            deleted = deleted.saturating_add(self.delete_matching(pattern).await?);
        }
        Ok(deleted)
    }

    /// Delete every key that matches `pattern`, one `SCAN` page at a time.
    async fn delete_matching(&self, pattern: &str) -> Result<u64, CacheError> {
        let mut sweep = Sweep::default();
        while let Some(cursor) = sweep.next {
            sweep = self.sweep_page(pattern, cursor, sweep.deleted).await?;
        }
        Ok(sweep.deleted)
    }

    /// One `SCAN` page from `cursor`, and an `UNLINK` of its keys.
    async fn sweep_page(
        &self,
        pattern: &str,
        cursor: u64,
        deleted: u64,
    ) -> Result<Sweep, CacheError> {
        let (next, found): (u64, Vec<String>) = self
            .run(
                redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(SCAN_PAGE),
            )
            .await?;
        let unlinked: u64 = if found.is_empty() {
            0
        } else {
            self.run(redis::cmd("UNLINK").arg(&found)).await?
        };
        Ok(Sweep {
            next: (next != 0).then_some(next),
            deleted: deleted.saturating_add(unlinked),
        })
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
            AccountCache::connect_waiting(&CacheConfig::default(), Duration::from_mins(1)).await;
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
