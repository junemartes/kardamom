//! Startup cursor reconcile: the watcher's resume position must agree with
//! the destination chain's own lane cursor, `Inbox.nextSeq[origin]`.
//!
//! The cursor file alone is not enough (audit H9). An operator who restores
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

use std::time::Duration;

use alloy_primitives::{B256, U256};
use async_trait::async_trait;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::{WsClient, WsClientBuilder};
use kardamom_types::xchain::{INBOX, inbox_next_seq_slot};
use tracing::{info, warn};

/// Why the reconcile could not confirm a resume position.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
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
#[async_trait]
pub trait DestinationStateReader: Send + Sync {
    async fn inbox_next_seq(&self, origin_chain_id: u64) -> Result<u64, ReconcileError>;
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
    pub async fn connect(url: &str) -> Result<Self, ReconcileError> {
        let read_err = |detail: String| ReconcileError::Read { origin: 0, detail };
        let client = if url.starts_with("ws://") || url.starts_with("wss://") {
            EitherClient::Ws(
                WsClientBuilder::default()
                    .build(url)
                    .await
                    .map_err(|e| read_err(format!("connect {url}: {e}")))?,
            )
        } else {
            EitherClient::Http(
                HttpClientBuilder::default()
                    .build(url)
                    .map_err(|e| read_err(format!("connect {url}: {e}")))?,
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

#[async_trait]
impl DestinationStateReader for RpcDestinationReader {
    async fn inbox_next_seq(&self, origin_chain_id: u64) -> Result<u64, ReconcileError> {
        let word = self
            .get_storage_at(inbox_next_seq_slot(origin_chain_id))
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
    pub attempts: u32,
    pub pause: Duration,
}

impl Default for ReconcileRetry {
    fn default() -> Self {
        Self {
            attempts: 5,
            pause: Duration::from_secs(1),
        }
    }
}

/// Apply the three rules. Returns the cursor to start from.
///
/// # Errors
/// - [`ReconcileError::Read`] if the destination cannot be read.
/// - [`ReconcileError::AheadCursor`] if `cursor > next_seq` after the
///   retries.
pub async fn reconcile_cursor<R: DestinationStateReader + ?Sized>(
    reader: &R,
    origin_chain_id: u64,
    cursor: u64,
    retry: ReconcileRetry,
) -> Result<u64, ReconcileError> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let next_seq = reader.inbox_next_seq(origin_chain_id).await?;
        if cursor == next_seq {
            info!(
                target: "da_watcher::interop",
                origin = origin_chain_id,
                cursor,
                "cursor reconciled with the destination's Inbox.nextSeq"
            );
            return Ok(cursor);
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
            return Ok(next_seq);
        }
        if attempt >= retry.attempts.max(1) {
            return Err(ReconcileError::AheadCursor {
                origin: origin_chain_id,
                cursor,
                next_seq,
            });
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
        tokio::time::sleep(retry.pause).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A destination whose `Inbox.nextSeq` follows a script, one value per
    /// read, and holds the last value after the script runs out.
    struct Scripted {
        values: Arc<Mutex<Vec<u64>>>,
        reads: Arc<Mutex<u32>>,
    }

    impl Scripted {
        fn new(values: &[u64]) -> Self {
            Self {
                values: Arc::new(Mutex::new(values.to_vec())),
                reads: Arc::new(Mutex::new(0)),
            }
        }
        fn reads(&self) -> u32 {
            *self.reads.lock().unwrap()
        }
    }

    #[async_trait]
    impl DestinationStateReader for Scripted {
        async fn inbox_next_seq(&self, _origin: u64) -> Result<u64, ReconcileError> {
            *self.reads.lock().unwrap() += 1;
            let mut v = self.values.lock().unwrap();
            if v.len() > 1 {
                Ok(v.remove(0))
            } else {
                Ok(v[0])
            }
        }
    }

    fn fast() -> ReconcileRetry {
        ReconcileRetry {
            attempts: 3,
            pause: Duration::from_millis(1),
        }
    }

    #[tokio::test]
    async fn an_equal_cursor_is_ok() {
        let dest = Scripted::new(&[7]);
        assert_eq!(reconcile_cursor(&dest, 1, 7, fast()).await.unwrap(), 7);
        assert_eq!(dest.reads(), 1);
    }

    #[tokio::test]
    async fn a_stale_cursor_advances_to_next_seq() {
        let dest = Scripted::new(&[7]);
        assert_eq!(reconcile_cursor(&dest, 1, 3, fast()).await.unwrap(), 7);
        // A fresh pair with a fresh destination starts at 0 either way.
        let dest = Scripted::new(&[0]);
        assert_eq!(reconcile_cursor(&dest, 1, 0, fast()).await.unwrap(), 0);
    }

    /// Audit H9: the ahead cursor is the dangerous side. Refuse.
    #[tokio::test]
    async fn an_ahead_cursor_refuses_to_start() {
        let dest = Scripted::new(&[3]);
        let err = reconcile_cursor(&dest, 1, 7, fast()).await.unwrap_err();
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
        assert_eq!(reconcile_cursor(&dest, 1, 7, fast()).await.unwrap(), 7);
        assert_eq!(dest.reads(), 3);
    }

    #[tokio::test]
    async fn a_read_failure_is_reported_not_guessed() {
        struct Down;
        #[async_trait]
        impl DestinationStateReader for Down {
            async fn inbox_next_seq(&self, origin: u64) -> Result<u64, ReconcileError> {
                Err(ReconcileError::Read {
                    origin,
                    detail: "connection refused".into(),
                })
            }
        }
        let err = reconcile_cursor(&Down, 1, 0, fast()).await.unwrap_err();
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
            assert_eq!(
                reconcile_cursor(&reader, 412_346, 42, fast())
                    .await
                    .unwrap(),
                42
            );
        }
        let calls = seen.lock().unwrap();
        assert!(calls.len() >= 2);
        for (addr, slot, tag) in calls.iter() {
            assert_eq!(*addr, INBOX);
            assert_eq!(
                *slot,
                U256::from_be_bytes(inbox_next_seq_slot(412_346).0),
                "the Inbox.nextSeq mapping slot for the origin"
            );
            assert_eq!(tag, "latest");
        }
        handle.stop().unwrap();
    }
}
