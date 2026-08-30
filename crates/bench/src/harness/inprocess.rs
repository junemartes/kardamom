//! This is an in-process ingress stand-in: the real `IngressProxy` over
//! in-memory [`MockChannels`], with a simple fake executor that reflects
//! every published `TxEnvelope` straight back as a success `Receipt`.

use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};

use kardamom_ingress::{IngressConfig, IngressHandle, IngressProxy, MockChannels};
use kardamom_types::{AckPolicy, BPosition, Receipt};

use crate::config::{MAX_IN_FLIGHT_SLACK, REQUEST_TIMEOUT};

/// Start an in-process [`IngressProxy`] over in-memory [`MockChannels`],
/// with a simple fake executor that turns every published `TxEnvelope`
/// straight into a success `Receipt`. Returns a jsonrpsee client that
/// points at the proxy's ephemeral loopback port, and a handle that
/// stops everything on [`InProcessIngress::shutdown`].
///
/// This is the stand-in behind the profiling
/// [`Harness`](crate::harness::Harness) and the crate's smoke tests,
/// used since the removal of `kardamom-node`. It exercises the real
/// ingress hot path (signature recovery, routing, RPC framing, receipt
/// release) with no live Aeron media driver and no real sequencer,
/// executor, or sealer. A full in-process Aeron pipeline harness is a
/// follow-up item.
///
/// `ack_policy` is forced to [`AckPolicy::OnOffer`], so a submission is
/// released as soon as its receipt arrives. There is no recorder or
/// quorum watermark here.
pub async fn spawn_inprocess_ingress(
    chain_id: u64,
    shards: u32,
    max_in_flight: usize,
) -> anyhow::Result<(HttpClient, InProcessIngress)> {
    let (mock, shard_rxs) = MockChannels::new(shards as usize);
    let receipt_tx = mock.receipt_bus.clone();

    // One fake-executor task runs per shard. It drains published envelopes and
    // sends a success receipt back on the receipt bus, so the proxy releases
    // the parked submission. `from` and `nonce` match what the proxy parked
    // on, because it keys pending submissions by `(sender, nonce)`.
    let mut fake_exec = Vec::with_capacity(shard_rxs.len());
    for (shard, mut rx) in shard_rxs.into_iter().enumerate() {
        let receipt_tx = receipt_tx.clone();
        fake_exec.push(tokio::spawn(async move {
            let mut idx: i32 = 0;
            while let Some(env) = rx.recv().await {
                idx = idx.wrapping_add(1);
                let nonce = decode_nonce(env.raw_tx.as_ref()).unwrap_or(0);
                let receipt = Receipt {
                    tx_idx: BPosition {
                        term_id: shard as i32,
                        term_offset: idx,
                    },
                    tx_hash: env.tx_hash,
                    status: true,
                    gas_used: 21_000,
                    nonce,
                    from: env.sender,
                    ..Default::default()
                };
                // A send error means the broadcast bus has no receivers.
                // The proxy is gone, so there is nothing left to release.
                if receipt_tx.send(receipt).is_err() {
                    break;
                }
            }
        }));
    }

    let cfg = IngressConfig {
        chain_id,
        partition_count_m: shards,
        ack_policy: AckPolicy::OnOffer,
        ..IngressConfig::default()
    };
    let proxy = IngressProxy::new(cfg, mock.clone(), mock);
    let handle = proxy.start().await?;

    let url = format!("http://{}", handle.jsonrpc_addr);
    let client = HttpClientBuilder::default()
        .request_timeout(REQUEST_TIMEOUT)
        .max_concurrent_requests(max_in_flight + MAX_IN_FLIGHT_SLACK)
        .build(&url)?;
    Ok((client, InProcessIngress { handle, fake_exec }))
}

/// Owns the in-process ingress server and the fake-executor reflector
/// tasks. Call [`InProcessIngress::shutdown`], or drop this value, to
/// tear them down.
pub struct InProcessIngress {
    handle: IngressHandle,
    fake_exec: Vec<tokio::task::JoinHandle<()>>,
}

impl InProcessIngress {
    /// Stop the jsonrpsee server and abort the fake-executor tasks.
    pub async fn shutdown(self) {
        let _ = self.handle.jsonrpc_handle.stop();
        self.handle.jsonrpc_handle.stopped().await;
        for h in self.fake_exec {
            h.abort();
        }
    }
}

/// Decode the transaction nonce from raw envelope bytes, the same way the
/// ingress proxy does it, with `alloy_consensus::TxEnvelope::decode`. This
/// keeps the synthesized receipt's `(from, nonce)` matching the
/// parked-submission key. Returns `None` if the bytes do not decode; the
/// proxy would already have rejected such a transaction.
fn decode_nonce(raw: &[u8]) -> Option<u64> {
    use alloy_consensus::transaction::Transaction;
    use alloy_rlp::Decodable;
    let mut slice = raw;
    alloy_consensus::TxEnvelope::decode(&mut slice)
        .ok()
        .map(|env| env.nonce())
}
