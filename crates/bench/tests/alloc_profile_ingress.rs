//! Allocation profile of the ingress tx-submission path (ignored by
//! default — run explicitly):
//!
//!   cargo test -p kardamom-bench --test alloc_profile_ingress --release -- \
//!     --ignored --nocapture
//!
//! Drives `IngressProxy::submit_raw` — the shared hot path behind BOTH the
//! JSON-RPC and binary listeners: overload valve, per-IP rate limit, RLP
//! decode, batched secp256k1 recovery + keccak256 tx_hash, receipt-cache
//! lookup, pending-registry park, and the publish onto the tx_data shard
//! seam — entirely in-process against `MockChannels` (no Aeron, no network,
//! no jsonrpsee), under the DHAT heap profiler. A harness pump synthesizes
//! a `Receipt` per published envelope and a periodic `QuorumWatermark`
//! tick, so the measured window also covers the proxy's production
//! receipt-side processing (MDS first-wins dedup, receipt-cache insert,
//! parked-client release through the on-quorum gate).
//!
//! Boundary caveats (also in the harness report):
//! - `submit_raw` is the deepest separable function: jsonrpsee framing and
//!   the hex decode of `eth_sendRawTransaction` params live above it and
//!   are NOT measured.
//! - Per-op numbers include the harness driver (one `tokio::spawn` per
//!   submission — analogous to the per-request task the RPC server spawns)
//!   and the tiny synthetic receipt pump (flat `Receipt` construct + two
//!   broadcast sends per tx; nonce comes from a prebuilt hash map, no
//!   decode).
//!
//! Writes dhat-heap-ingress.json next to this crate's Cargo.toml
//! (per-callsite attribution, viewable with dh_view.html).

use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use alloy_primitives::{B256, Bytes, U256, keccak256};
use kardamom_bench::mnemonic;
use kardamom_bench::signers::presign_transfers;
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::{IngressProxy, MockChannels};
use kardamom_types::{BPosition, QuorumWatermark, Receipt};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const ANVIL_PHRASE: &str = "test test test test test test test test test test test junk";
const CHAIN_ID: u64 = 1;
const SENDERS: u32 = 64;
/// Submissions kept in flight per driver batch (bounded so the 1,024-slot
/// mock receipt broadcast can never lag-drop under the profiler).
const INFLIGHT: usize = 512;
const WARMUP: usize = 2 * INFLIGHT; // 1,024 outside the window
const MEASURED: usize = 10 * INFLIGHT; // 5,120 measured

type Proxy = IngressProxy<MockChannels, MockChannels>;

/// Fake downstream: drain each tx_data shard, stamp a monotone tx_ordering
/// position, and echo a synthetic `Receipt`. Nonce comes from the prebuilt
/// `tx_hash -> nonce` map so the pump does no per-tx decoding (keeps the
/// harness's own allocation footprint near zero).
fn spawn_receipt_pump(
    mock: &MockChannels,
    rx_vec: Vec<tokio::sync::mpsc::UnboundedReceiver<kardamom_types::TxEnvelope>>,
    nonce_by_hash: Arc<HashMap<B256, u64>>,
    position: Arc<AtomicI32>,
) {
    for mut rx in rx_vec {
        let receipt_bus = mock.receipt_bus.clone();
        let nonces = nonce_by_hash.clone();
        let position = position.clone();
        tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let off = position.fetch_add(1, Ordering::Relaxed) + 1;
                let nonce = *nonces.get(&envelope.tx_hash).expect("pregenerated tx");
                let receipt = Receipt {
                    tx_idx: BPosition {
                        term_id: 0,
                        term_offset: off,
                    },
                    tx_hash: envelope.tx_hash,
                    status: true,
                    gas_used: 21_000,
                    logs: Vec::new(),
                    write_set_hash: B256::ZERO,
                    from: envelope.sender,
                    nonce,
                    ..Default::default()
                };
                let _ = receipt_bus.send(receipt);
            }
        });
    }
}

/// Production quorum watermarks are periodic egress-progress snapshots, not
/// per-tx events; a per-tx watermark would turn `release_satisfied`'s
/// registry walk into an O(n^2) harness artifact. 500us models the deploy's
/// snapshot cadence closely enough for the on-quorum gate to release parked
/// clients within a tick.
fn spawn_watermark_ticker(mock: &MockChannels, position: Arc<AtomicI32>) {
    let watermark_bus = mock.watermark_bus.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_micros(500));
        loop {
            tick.tick().await;
            let _ = watermark_bus.send(QuorumWatermark {
                position: BPosition {
                    term_id: 0,
                    term_offset: position.load(Ordering::Relaxed),
                },
            });
        }
    });
}

/// Submit `raws` with at most `INFLIGHT` concurrent in-flight submissions,
/// awaiting every receipt. One spawned task per submission, like the RPC
/// server's per-request handler tasks.
async fn submit_all(proxy: Arc<Proxy>, ip: IpAddr, raws: &[Bytes]) {
    for chunk in raws.chunks(INFLIGHT) {
        let mut handles = Vec::with_capacity(chunk.len());
        for raw in chunk {
            let p = proxy.clone();
            let raw = raw.clone();
            handles.push(tokio::spawn(async move { p.submit_raw(ip, raw).await }));
        }
        for h in handles {
            h.await.expect("driver task").expect("submission receipted");
        }
    }
}

#[test]
#[ignore = "profiling run — invoke explicitly with --ignored"]
fn ingress_submission_allocation_profile() {
    // Pre-generate valid signed raw txs: 64 mnemonic-derived senders,
    // sequential nonces, round-robin interleaved (kardamom_bench helpers).
    let signers = mnemonic::derive_signers(ANVIL_PHRASE, SENDERS).unwrap();
    let raws = presign_transfers(
        &signers,
        CHAIN_ID,
        signers[0].address,
        U256::from(1u64),
        WARMUP + MEASURED,
        0,
    )
    .unwrap();
    // presign_transfers round-robins senders per nonce step: index i is
    // signer i % SENDERS at nonce i / SENDERS.
    let nonce_by_hash: Arc<HashMap<B256, u64>> = Arc::new(
        raws.iter()
            .enumerate()
            .map(|(i, raw)| (keccak256(raw), i as u64 / SENDERS as u64))
            .collect(),
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    let position = Arc::new(AtomicI32::new(0));
    let proxy: Arc<Proxy> = rt.block_on(async {
        let cfg = IngressConfig {
            // All traffic comes from one IP here; the limiter must never be
            // the thing measured (production spreads clients across IPs).
            rate_limit_per_ip_per_sec: NonZeroU32::new(1_000_000_000).unwrap(),
            rate_limit_burst: NonZeroU32::new(1_000_000).unwrap(),
            pending_receipt_timeout: Duration::from_secs(10),
            ..IngressConfig::default()
        };
        let (mock, rx_vec) = MockChannels::new(cfg.partition_count_m as usize);
        let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));
        spawn_receipt_pump(&mock, rx_vec, nonce_by_hash.clone(), position.clone());
        spawn_watermark_ticker(&mock, position.clone());
        proxy
    });

    // Warm up OUTSIDE the measured window: fills the sig-verify ring, the
    // receipt cache / seen-receipts / pending dashmap shards, and the tokio
    // runtime's internal pools.
    rt.block_on(submit_all(proxy.clone(), ip, &raws[..WARMUP]));

    // Measured window under DHAT: the full in-process submission round
    // trip, INFLIGHT concurrent per batch.
    let profiler = dhat::Profiler::builder().build();
    let stats0 = dhat::HeapStats::get();
    let t0 = std::time::Instant::now();
    rt.block_on(submit_all(proxy.clone(), ip, &raws[WARMUP..]));
    let wall = t0.elapsed();
    let stats = dhat::HeapStats::get();

    let n = MEASURED as u64;
    let allocs = stats.total_blocks - stats0.total_blocks;
    let bytes = stats.total_bytes - stats0.total_bytes;
    println!(
        "==================== INGRESS ALLOCATION PROFILE ({n} submissions) ===================="
    );
    println!("allocs/op:      {:.2}", allocs as f64 / n as f64);
    println!("bytes/op:       {:.0}", bytes as f64 / n as f64);
    println!("peak heap:      {:.2} MB", stats.max_bytes as f64 / 1e6);
    println!(
        "wall/op:        {:.2} us (batch-concurrent, {INFLIGHT} in flight)",
        wall.as_micros() as f64 / n as f64
    );
    println!(
        "implied rate:   {:.0} ktx/s",
        n as f64 / wall.as_secs_f64() / 1e3
    );

    // Quiesce the runtime (pump/watcher/ticker tasks) BEFORE finalizing the
    // profiler so the dump is not raced by background allocation.
    drop(rt);
    drop(profiler); // writes dhat-heap.json with per-callsite attribution
    let _ = std::fs::rename("dhat-heap.json", "dhat-heap-ingress.json");
}
