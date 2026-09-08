//! Startup cursor reconcile: the watcher's resume position must agree with
//! the destination chain's own lane cursor, `Inbox.nextSeq[origin]`.
//!
//! The cursor file alone is not enough. An operator who restores
//! the destination from a snapshot, or who seeds `--interop-start-seq`
//! wrongly, restarts the watcher with a cursor AHEAD of what the chain has
//! delivered. The watcher then publishes from that cursor, the sealer (its
//! own lane cursor restored with the same snapshot) rejects, or worse, a
//! sealer with an unknown cursor seeds from the record and the skipped
//! messages are lost with no layer raising a fault. So the watcher reads the
//! destination's `Inbox.nextSeq[origin]` at startup and applies three rules:
//!
//! * `cursor < next_seq`: the file is STALE. Safe: those seqs were
//!   delivered. Warn, and advance to `next_seq`.
//! * `cursor == next_seq`: ok.
//! * `cursor > next_seq`: the file is AHEAD. Refuse to start. The operator
//!   must reset the cursor file (or the chain) on purpose.
//!
//! Destination commits are pipelined. A watcher that restarts right after
//! a publish can read a `next_seq` one record behind for a moment, so the
//! "ahead" verdict is retried a few times before it is final.

use std::future::Future;
use std::num::NonZeroU32;
use std::ops::ControlFlow;
use std::time::Duration;

use alloy_primitives::{B256, U256};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use kardamom_types::xchain::{INBOX, Inbox};
use tracing::{info, warn};

/// Why the reconcile could not confirm a resume position.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    /// The destination endpoint could not be reached.
    #[error("connect to destination {url} failed: {detail}")]
    Connect { url: String, detail: String },
    /// The destination read failed (transport, decode, or a value that is
    /// not a u64).
    #[error("destination read of Inbox.nextSeq[{origin}] failed: {detail}")]
    Read { origin: u64, detail: String },
    /// The local cursor is ahead of the destination's lane cursor. Starting
    /// would skip `cursor - next_seq` messages for good.
    #[error(
        "cursor {cursor} for origin {origin} is AHEAD of the destination's Inbox.nextSeq \
         ({next_seq}): refusing to start. Reset the cursor file to {next_seq} on purpose, or \
         restore the destination, then restart"
    )]
    AheadCursor {
        origin: u64,
        cursor: u64,
        next_seq: u64,
    },
}

/// A read of the destination's `Inbox.nextSeq[origin]`.
pub trait DestinationStateReader: Send + Sync {
    /// # Errors
    ///
    /// Returns [`ReconcileError::Read`] if the destination cannot be read.
    fn inbox_next_seq(
        &self,
        origin_chain_id: u64,
    ) -> impl Future<Output = Result<u64, ReconcileError>> + Send;
}

/// Reads the slot with `eth_getStorageAt` over JSON-RPC. `ws://` and
/// `wss://` URLs use a WebSocket client; anything else uses HTTP. The
/// validator's `--serve-feed` endpoint serves the method (see
/// `kardamom-validator`'s `interop::state_rpc`).
pub struct RpcDestinationReader {
    client: EitherClient,
}

enum EitherClient {
    Ws(WsClient),
    Http(HttpClient),
}

impl RpcDestinationReader {
    /// # Errors
    ///
    /// Returns [`ReconcileError::Connect`] if `url` cannot be reached.
    pub async fn connect(url: &str) -> Result<Self, ReconcileError> {
        let connect_err = |detail: String| ReconcileError::Connect {
            url: url.to_string(),
            detail,
        };
        let client = if url.starts_with("ws://") || url.starts_with("wss://") {
            EitherClient::Ws(
                WsClientBuilder::default()
                    .build(url)
                    .await
                    .map_err(|e| connect_err(e.to_string()))?,
            )
        } else {
            EitherClient::Http(
                HttpClientBuilder::default()
                    .build(url)
                    .map_err(|e| connect_err(e.to_string()))?,
            )
        };
        Ok(Self { client })
    }

    async fn get_storage_at(&self, slot: B256) -> Result<B256, jsonrpsee::core::ClientError> {
        let params = rpc_params![INBOX, U256::from_be_bytes(slot.0), "latest"];
        match &self.client {
            EitherClient::Ws(c) => c.request("eth_getStorageAt", params).await,
            EitherClient::Http(c) => c.request("eth_getStorageAt", params).await,
        }
    }
}

impl DestinationStateReader for RpcDestinationReader {
    async fn inbox_next_seq(&self, origin_chain_id: u64) -> Result<u64, ReconcileError> {
        let word = self
            .get_storage_at(Inbox::next_seq_slot(origin_chain_id))
            .await
            .map_err(|e| ReconcileError::Read {
                origin: origin_chain_id,
                detail: e.to_string(),
            })?;
        u64::try_from(U256::from_be_bytes(word.0)).map_err(|_| ReconcileError::Read {
            origin: origin_chain_id,
            detail: format!("Inbox.nextSeq is not a u64: {word}"),
        })
    }
}

/// How many times an "ahead" verdict is re-read before it is final, and the
/// pause between reads. Pipelined destination commits settle within a few
/// blocks, so a few seconds covers a restart that raced a publish.
#[derive(Debug, Clone, Copy)]
pub struct ReconcileRetry {
    pub attempts: NonZeroU32,
    pub pause: Duration,
}

/// [`ReconcileRetry::default`]'s attempt count. A `const` so the
/// `NonZeroU32::new(..).unwrap()` panic path (never reached: `5` is
/// nonzero) is checked once at compile time, not on every call.
const DEFAULT_RECONCILE_ATTEMPTS: NonZeroU32 = NonZeroU32::new(5).unwrap();

impl Default for ReconcileRetry {
    fn default() -> Self {
        Self {
            attempts: DEFAULT_RECONCILE_ATTEMPTS,
            pause: Duration::from_secs(1),
        }
    }
}

impl ReconcileRetry {
    /// Apply the three rules. Returns the cursor to start from.
    ///
    /// # Errors
    /// - [`ReconcileError::Read`] if the destination cannot be read.
    /// - [`ReconcileError::AheadCursor`] if `cursor > next_seq` after the
    ///   retries.
    pub async fn reconcile<R: DestinationStateReader>(
        &self,
        reader: &R,
        origin_chain_id: u64,
        cursor: u64,
    ) -> Result<u64, ReconcileError> {
        // `verdict` always returns `Break` on the last attempt (see its
        // `attempt == self.attempts.get()` case), so this loop always
        // returns. It has no `break`, so its type is `!`: there is no
        // code path after it, and so no unreachable arm to write.
        let mut attempt = 1;
        loop {
            let next_seq = reader.inbox_next_seq(origin_chain_id).await?;
            match self.verdict(origin_chain_id, cursor, next_seq, attempt) {
                ControlFlow::Break(result) => return result,
                ControlFlow::Continue(()) => {
                    tokio::time::sleep(self.pause).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Apply the three rules to one read of the destination's
    /// `Inbox.nextSeq`. `Break` carries the reconcile's final answer;
    /// `Continue` means the caller retries after a pause.
    fn verdict(
        &self,
        origin_chain_id: u64,
        cursor: u64,
        next_seq: u64,
        attempt: u32,
    ) -> ControlFlow<Result<u64, ReconcileError>> {
        if cursor == next_seq {
            info!(
                target: "da_watcher::interop",
                origin = origin_chain_id,
                cursor,
                "cursor reconciled with the destination's Inbox.nextSeq"
            );
            return ControlFlow::Break(Ok(cursor));
        }
        if cursor < next_seq {
            warn!(
                target: "da_watcher::interop",
                origin = origin_chain_id,
                cursor,
                next_seq,
                "cursor is STALE (behind the destination's Inbox.nextSeq); advancing to \
                 next_seq. This is safe: those seqs were delivered"
            );
            return ControlFlow::Break(Ok(next_seq));
        }
        if attempt == self.attempts.get() {
            return ControlFlow::Break(Err(ReconcileError::AheadCursor {
                origin: origin_chain_id,
                cursor,
                next_seq,
            }));
        }
        warn!(
            target: "da_watcher::interop",
            origin = origin_chain_id,
            cursor,
            next_seq,
            attempt,
            "cursor is ahead of the destination's Inbox.nextSeq; re-reading in case a \
             pipelined commit is still settling"
        );
        ControlFlow::Continue(())
    }
}

/// The interop path requires exactly one of `--interop-dest-rpc` or
/// `--interop-skip-cursor-reconcile`.
#[derive(Debug, thiserror::Error)]
#[error(
    "the interop path requires --interop-dest-rpc (the destination JSON-RPC that serves \
     eth_getStorageAt, for the startup cursor reconcile); pass --interop-skip-cursor-reconcile \
     to skip it in tests"
)]
pub struct MissingCursorReconcile;

/// Where the interop watcher's startup cursor reconcile reads the
/// destination's lane cursor from. [`Self::parse`] turns the two CLI flags
/// (`--interop-dest-rpc` and `--interop-skip-cursor-reconcile`) into this
/// once, so the binary and a harness that spawns it name one thing one
/// way, and the invalid pair (neither flag set) cannot reach
/// [`ReconcileRetry::reconcile`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorReconcile {
    /// Reconcile against this destination JSON-RPC endpoint.
    Rpc(String),
    /// Skip the reconcile. Tests only: a cursor ahead of the destination is
    /// a permanent lane hole.
    Skip,
}

impl CursorReconcile {
    /// Parse `--interop-dest-rpc` and `--interop-skip-cursor-reconcile`.
    ///
    /// # Errors
    ///
    /// Returns [`MissingCursorReconcile`] when `dest_rpc` is `None` and
    /// `skip` is `false`.
    pub fn parse(dest_rpc: Option<String>, skip: bool) -> Result<Self, MissingCursorReconcile> {
        match (dest_rpc, skip) {
            (Some(url), _) => Ok(Self::Rpc(url)),
            (None, true) => Ok(Self::Skip),
            (None, false) => Err(MissingCursorReconcile),
        }
    }

    /// The CLI flags that reproduce this value, for a harness that spawns
    /// the `kardamom-da-watcher` binary as a subprocess.
    #[must_use]
    pub fn cli_args(&self) -> Vec<String> {
        match self {
            Self::Rpc(url) => vec!["--interop-dest-rpc".to_string(), url.clone()],
            Self::Skip => vec!["--interop-skip-cursor-reconcile".to_string()],
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A destination whose `Inbox.nextSeq` follows a script, one value per
    /// read, and holds the last value after the script runs out. Not
    /// shared across threads: one test owns it and reads it through `&self`,
    /// so a plain `Mutex` is enough.
    struct Scripted {
        values: Mutex<Vec<u64>>,
        reads: Mutex<u32>,
    }

    impl Scripted {
        fn new(values: &[u64]) -> Self {
            Self {
                values: Mutex::new(values.to_vec()),
                reads: Mutex::new(0),
            }
        }
        fn reads(&self) -> u32 {
            *self.reads.lock().unwrap()
        }
    }

    impl DestinationStateReader for Scripted {
        fn inbox_next_seq(
            &self,
            _origin: u64,
        ) -> impl Future<Output = Result<u64, ReconcileError>> + Send {
            *self.reads.lock().unwrap() += 1;
            let mut v = self.values.lock().unwrap();
            let next = if v.len() > 1 {
                Ok(v.remove(0))
            } else {
                Ok(v[0])
            };
            std::future::ready(next)
        }
    }

    fn fast() -> ReconcileRetry {
        ReconcileRetry {
            attempts: NonZeroU32::new(3).unwrap(),
            pause: Duration::from_millis(1),
        }
    }

    #[tokio::test]
    async fn an_equal_cursor_is_ok() {
        let dest = Scripted::new(&[7]);
        assert_eq!(fast().reconcile(&dest, 1, 7).await.unwrap(), 7);
        assert_eq!(dest.reads(), 1);
    }

    #[tokio::test]
    async fn a_stale_cursor_advances_to_next_seq() {
        let dest = Scripted::new(&[7]);
        assert_eq!(fast().reconcile(&dest, 1, 3).await.unwrap(), 7);
        // A fresh pair with a fresh destination starts at 0 either way.
        let dest = Scripted::new(&[0]);
        assert_eq!(fast().reconcile(&dest, 1, 0).await.unwrap(), 0);
    }

    /// The ahead cursor is the dangerous side. Refuse.
    #[tokio::test]
    async fn an_ahead_cursor_refuses_to_start() {
        let dest = Scripted::new(&[3]);
        let err = fast().reconcile(&dest, 1, 7).await.unwrap_err();
        assert!(
            matches!(
                err,
                ReconcileError::AheadCursor {
                    origin: 1,
                    cursor: 7,
                    next_seq: 3
                }
            ),
            "got {err:?}"
        );
        assert_eq!(dest.reads(), 3, "every retry was used before the refusal");
        assert!(err.to_string().contains("refusing to start"));
    }

    /// A restart that races a pipelined commit reads one record low for a
    /// moment. The retry sees the settled value and accepts.
    #[tokio::test]
    async fn a_settling_commit_is_retried_not_refused() {
        let dest = Scripted::new(&[6, 6, 7]);
        assert_eq!(fast().reconcile(&dest, 1, 7).await.unwrap(), 7);
        assert_eq!(dest.reads(), 3);
    }

    #[tokio::test]
    async fn a_read_failure_is_reported_not_guessed() {
        struct Down;
        impl DestinationStateReader for Down {
            fn inbox_next_seq(
                &self,
                origin: u64,
            ) -> impl Future<Output = Result<u64, ReconcileError>> + Send {
                std::future::ready(Err(ReconcileError::Read {
                    origin,
                    detail: "connection refused".into(),
                }))
            }
        }
        let err = fast().reconcile(&Down, 1, 0).await.unwrap_err();
        assert!(
            matches!(err, ReconcileError::Read { origin: 1, .. }),
            "{err:?}"
        );
    }

    /// The real reader against a real jsonrpsee server that serves
    /// `eth_getStorageAt`: the slot, the address, and the block tag reach
    /// the server as the Ethereum shape, and the word decodes to a u64.
    #[tokio::test]
    async fn the_rpc_reader_reads_the_inbox_slot_over_the_wire() {
        use jsonrpsee::server::{RpcModule, Server};

        let seen: Arc<Mutex<Vec<(alloy_primitives::Address, U256, String)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let mut module = RpcModule::new(seen.clone());
        module
            .register_method("eth_getStorageAt", |params, seen, _| {
                let (addr, slot, tag): (alloy_primitives::Address, U256, String) =
                    params.parse().unwrap();
                seen.lock().unwrap().push((addr, slot, tag));
                let mut w = [0u8; 32];
                w[24..].copy_from_slice(&42u64.to_be_bytes());
                Ok::<B256, jsonrpsee::types::ErrorObjectOwned>(B256::from(w))
            })
            .unwrap();
        let server = Server::builder().build("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let handle = server.start(module);

        for url in [format!("ws://{addr}"), format!("http://{addr}")] {
            let reader = RpcDestinationReader::connect(&url).await.unwrap();
            assert_eq!(reader.inbox_next_seq(412_346).await.unwrap(), 42);
            assert_eq!(fast().reconcile(&reader, 412_346, 42).await.unwrap(), 42);
        }
        let calls = seen.lock().unwrap();
        assert!(calls.len() >= 2);
        for (addr, slot, tag) in calls.iter() {
            assert_eq!(*addr, INBOX);
            assert_eq!(
                *slot,
                U256::from_be_bytes(Inbox::next_seq_slot(412_346).0),
                "the Inbox.nextSeq mapping slot for the origin"
            );
            assert_eq!(tag, "latest");
        }
        handle.stop().unwrap();
    }

    #[test]
    fn cursor_reconcile_parses_the_rpc_url() {
        let cr = CursorReconcile::parse(Some("ws://host:9944".to_string()), false).unwrap();
        assert_eq!(cr, CursorReconcile::Rpc("ws://host:9944".to_string()));
        assert_eq!(cr.cli_args(), ["--interop-dest-rpc", "ws://host:9944"]);
    }

    #[test]
    fn cursor_reconcile_parses_the_skip_flag() {
        let cr = CursorReconcile::parse(None, true).unwrap();
        assert_eq!(cr, CursorReconcile::Skip);
        assert_eq!(cr.cli_args(), ["--interop-skip-cursor-reconcile"]);
    }

    #[test]
    fn cursor_reconcile_requires_one_of_the_two_flags() {
        assert!(CursorReconcile::parse(None, false).is_err());
    }
}
