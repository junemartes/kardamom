//! In-process multi-ingress (active/active) cluster harness.
//!
//! K independent `IngressProxy` replicas share ONE `MockChannels` bus: every
//! replica publishes to the same per-shard tx_data lanes (multiple publishers
//! per shard → single mpsc consumer, the fake sequencer/executor) and every
//! replica subscribes to the same broadcast receipt stream. This is the
//! deterministic analogue of N ingress nodes in front of the sharded
//! sequencers. It proves the app-layer replication invariants (D1–D4 of
//! docs/agents/resilient-ingress-spec.md) without Docker or real Aeron.
//!
//! Determinism: one shared bus; the single per-shard consumer assigns positions
//! in arrival order (no collision); condition waits use a bounded poll on
//! observable state (`lookup_receipt_by_hash`), never a fixed sleep for
//! correctness.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::proxy::ingress_id_of;
use kardamom_ingress::routing::partition_for;
use kardamom_ingress::{InMemoryStateDb, IngressProxy, MockChannels};
use kardamom_types::{AckPolicy, BPosition, Receipt};

type Proxy = IngressProxy<MockChannels, MockChannels, InMemoryStateDb>;

fn nonce_of(raw: &bytes::Bytes) -> u64 {
    use alloy_consensus::TxEnvelope;
    use alloy_consensus::transaction::Transaction;
    use alloy_rlp::Decodable;
    TxEnvelope::decode(&mut raw.as_ref()).unwrap().nonce()
}

/// Shared, observable state of the single fake sequencer/executor that drains
/// every shard's tx_data lane.
#[derive(Default)]
struct ExecInner {
    /// Total envelopes drained across all shards (a re-publish increments it).
    seen_count: usize,
    /// `correlation_id` of every drained envelope, in arrival order.
    correlation_ids: Vec<u64>,
    /// Per-shard drained count.
    per_shard: Vec<usize>,
    /// tx_hashes that have been "executed" (first sight only). A tx published
    /// twice (active/active retry) is executed exactly once.
    executed: HashSet<B256>,
}

#[derive(Clone)]
struct FakeExec {
    inner: Arc<Mutex<ExecInner>>,
}

impl FakeExec {
    fn new(shards: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ExecInner {
                per_shard: vec![0; shards],
                ..Default::default()
            })),
        }
    }

    /// Record an envelope; return a `Receipt` on the FIRST sight of its
    /// tx_hash (exactly-once execution), `None` for a duplicate re-publish.
    fn observe(
        &self,
        shard: usize,
        env: &kardamom_types::TxEnvelope,
        pos: BPosition,
    ) -> Option<Receipt> {
        let mut g = self.inner.lock().unwrap();
        g.seen_count += 1;
        g.correlation_ids.push(env.correlation_id);
        g.per_shard[shard] += 1;
        if g.executed.insert(env.tx_hash) {
            Some(Receipt {
                tx_idx: pos,
                tx_hash: env.tx_hash,
                status: true,
                gas_used: 21_000,
                logs: Vec::new(),
                write_set_hash: B256::ZERO,
                from: env.sender,
                nonce: nonce_of(&env.raw_tx),
                ..Default::default()
            })
        } else {
            None
        }
    }

    fn seen_count(&self) -> usize {
        self.inner.lock().unwrap().seen_count
    }
    fn correlation_ids(&self) -> Vec<u64> {
        self.inner.lock().unwrap().correlation_ids.clone()
    }
    fn per_shard(&self) -> Vec<usize> {
        self.inner.lock().unwrap().per_shard.clone()
    }
}

/// K active/active `IngressProxy` replicas over one shared bus + one fake
/// sequencer/executor draining every shard.
struct Cluster {
    proxies: Vec<Arc<Proxy>>,
    exec: FakeExec,
    _drains: Vec<tokio::task::JoinHandle<()>>,
}

impl Cluster {
    fn start(replicas: u16, shards: u32) -> Self {
        let (mock, receivers) = MockChannels::new(shards as usize);
        let exec = FakeExec::new(shards as usize);

        // One drain task per shard: multiple ingress publishers fan into this
        // single consumer, which assigns positions in arrival order.
        let mut drains = Vec::new();
        for (shard, mut rx) in receivers.into_iter().enumerate() {
            let exec = exec.clone();
            let receipt_bus = mock.receipt_bus.clone();
            drains.push(tokio::spawn(async move {
                let mut term: i32 = 0;
                while let Some(env) = rx.recv().await {
                    term += 1;
                    let pos = BPosition {
                        term_id: shard as i32,
                        term_offset: term,
                    };
                    if let Some(receipt) = exec.observe(shard, &env, pos) {
                        let _ = receipt_bus.send(receipt);
                    }
                }
            }));
        }

        let proxies = (0..replicas)
            .map(|id| {
                let cfg = IngressConfig {
                    partition_count_m: shards,
                    ingress_id: id,
                    // OnOffer: release as soon as the receipt arrives — keeps
                    // the harness receipt-driven (no watermark gating).
                    ack_policy: AckPolicy::OnOffer,
                    pending_receipt_timeout: Duration::from_secs(10),
                    ..IngressConfig::default()
                };
                Arc::new(IngressProxy::new(
                    cfg,
                    mock.clone(),
                    mock.clone(),
                    Arc::new(InMemoryStateDb::new()),
                ))
            })
            .collect();

        Self {
            proxies,
            exec,
            _drains: drains,
        }
    }

    async fn submit(&self, replica: usize, raw: alloy_primitives::Bytes) -> Receipt {
        self.proxies[replica]
            .submit_raw("127.0.0.1".parse().unwrap(), raw)
            .await
            .expect("submit resolves")
            .receipt
    }
}

/// Bounded poll on observable state — succeeds fast, fails loudly. Not a fixed
/// sleep: it asserts a state was reached, not that time passed.
async fn poll_until<F: Fn() -> bool>(what: &str, cond: F) {
    for _ in 0..2000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("condition never met: {what}");
}

/// A signer whose address routes to `target_shard` under `m` shards.
fn signer_for_shard(target_shard: u32, m: u32) -> PrivateKeySigner {
    loop {
        let s = PrivateKeySigner::random();
        if partition_for(s.address(), m) == target_shard {
            return s;
        }
    }
}

/// D2: `correlation_id`s are globally unique across replicas and carry the
/// originating replica id in their high bits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn correlation_id_unique_and_namespaced_across_replicas() {
    const K: u16 = 4;
    const N: usize = 10;
    let cluster = Cluster::start(K, 4);

    for replica in 0..K as usize {
        let mut futs = Vec::new();
        for _ in 0..N {
            let raw = common::sign_legacy(&PrivateKeySigner::random(), 0);
            futs.push(cluster.submit(replica, raw));
        }
        for r in futures::future::join_all(futs).await {
            assert!(r.status);
        }
    }

    let ids = cluster.exec.correlation_ids();
    assert_eq!(ids.len(), K as usize * N, "every submit was published once");
    assert_eq!(
        ids.iter().copied().collect::<HashSet<_>>().len(),
        K as usize * N,
        "all correlation_ids are globally unique"
    );
    // Exactly N ids per replica, and the high bits identify that replica.
    for replica in 0..K {
        let count = ids.iter().filter(|c| ingress_id_of(**c) == replica).count();
        assert_eq!(count, N, "replica {replica} owns exactly N correlation_ids");
    }
}

/// D1: every replica routes a sender to the same shard, and the envelope lands
/// there regardless of which replica accepted it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tx_routes_to_correct_shard_from_any_replica() {
    let m = 4u32;
    let cluster = Cluster::start(3, m);
    let signer = PrivateKeySigner::random();
    let expected = partition_for(signer.address(), m);

    // All replicas agree on the shard (pure function of sender + M).
    for p in &cluster.proxies {
        assert_eq!(p.partition_for(signer.address()), expected);
    }

    // Submit nonces 0,1,2 — one via each replica (distinct tx_hashes execute).
    for (replica, nonce) in [(0usize, 0u64), (1, 1), (2, 2)] {
        let r = cluster
            .submit(replica, common::sign_legacy(&signer, nonce))
            .await;
        assert!(r.status);
    }

    let per_shard = cluster.exec.per_shard();
    assert_eq!(
        per_shard[expected as usize], 3,
        "all of this sender's txs hit its shard"
    );
    assert_eq!(
        per_shard.iter().sum::<usize>(),
        3,
        "and nothing landed elsewhere"
    );
}

/// D4: a receipt fans out to every replica's cache (the basis for failover).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receipt_caches_on_all_replicas() {
    let cluster = Cluster::start(3, 2);
    let signer = PrivateKeySigner::random();
    let receipt = cluster.submit(0, common::sign_legacy(&signer, 0)).await;
    let h = receipt.tx_hash;

    for (i, p) in cluster.proxies.iter().enumerate() {
        let p = p.clone();
        poll_until(&format!("replica {i} caches receipt"), || {
            p.lookup_receipt_by_hash(h).is_some()
        })
        .await;
        assert_eq!(p.lookup_receipt_by_hash(h), Some(receipt.clone()));
    }
}

/// D4: a client that fails over to another replica after its tx executed is
/// served from that replica's cache — with NO re-publish to the sequencers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_retry_served_from_cache_no_republish() {
    let cluster = Cluster::start(2, 2);
    let signer = PrivateKeySigner::random();

    let first = cluster.submit(0, common::sign_legacy(&signer, 0)).await;
    // Wait until replica 1 has cached the receipt (the failover precondition).
    let p1 = cluster.proxies[1].clone();
    let h = first.tx_hash;
    poll_until("replica 1 caches receipt", || {
        p1.lookup_receipt_by_hash(h).is_some()
    })
    .await;

    let before = cluster.exec.seen_count();
    // Failover: the same (sender, nonce) submitted to replica 1.
    let again = cluster.submit(1, common::sign_legacy(&signer, 0)).await;

    assert_eq!(again.tx_hash, first.tx_hash, "served the same receipt");
    assert_eq!(
        cluster.exec.seen_count(),
        before,
        "cache hit must not re-publish to the sequencer"
    );
}

/// D1: multiple replicas concurrently publishing to the SAME shard all reach
/// the single consumer, and every submit resolves. (The in-process consumer
/// assigns positions by arrival order, so concurrent publishers are safe here —
/// the real-Aeron concurrent-publisher path is covered by Phase A's session id.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishers_one_shard_all_delivered() {
    let m = 4u32;
    let cluster = Cluster::start(3, m);
    // Three distinct senders, all routing to shard 0, one per replica.
    let signers: Vec<_> = (0..3).map(|_| signer_for_shard(0, m)).collect();

    let futs: Vec<_> = signers
        .iter()
        .enumerate()
        .map(|(replica, s)| cluster.submit(replica, common::sign_legacy(s, 0)))
        .collect();
    for r in futures::future::join_all(futs).await {
        assert!(r.status);
    }

    let per_shard = cluster.exec.per_shard();
    assert_eq!(
        per_shard[0], 3,
        "all three concurrent publishers reached shard 0"
    );
    assert_eq!(cluster.exec.seen_count(), 3);
}
