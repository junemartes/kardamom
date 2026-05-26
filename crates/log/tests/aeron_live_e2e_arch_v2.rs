//! End-to-end test of the **split-architecture** log tier against a
//! real Aeron Media Driver running in Docker.
//!
//! Topology validated:
//!
//! - M=2 sequencers each open a `TxDataPublisher` (exclusive) and publish
//!   N tx envelopes apiece.
//! - M=2 executors-side `TxDataSubscriber`s drain those envelopes into a
//!   per-A buffer indexed by `(sequencer_id, BPosition)`.
//! - One `TxOrderingPublisher` (concurrent) publishes `TxRef` records into
//!   tx_ordering in an interleaved canonical order.
//! - One `TxOrderingSubscriber` walks tx_ordering in canonical order, joining
//!   each ref against the A-buffer and asserting the recovered sequence
//!   matches the canonical order Aeron produced.
//!
//! Gated on the `docker-e2e` + `aeron-live` features AND on Docker
//! availability at runtime: if `docker info` fails (e.g. unprivileged CI
//! runner), the test prints "skipping" and returns 0. The crate's default
//! `cargo test` path does not enable these features, so this file is
//! excluded from baseline test runs.
//!
//! To run locally:
//!
//! ```bash
//! cargo test -p kardamom-log --features 'docker-e2e aeron-live' \
//!     --test aeron_live_e2e_arch_v2 -- --nocapture
//! ```

#![cfg(all(feature = "docker-e2e", feature = "aeron-live"))]

use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use log::config::LogConfig;
use log::publisher::{TxDataPublisher, TxOrderingPublisher};
use log::subscriber::Subscribers;
use log::testing::AeronTestCluster;
use types::{BPosition, TxEnvelope, TxOrderingMessage, TxRef};

const M: u8 = 2;
const TXS_PER_SEQ: u64 = 25;

async fn docker_available() -> bool {
    use tokio::process::Command;
    Command::new("docker")
        .arg("info")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn env(seq: u8, n: u64) -> TxEnvelope {
    TxEnvelope {
        correlation_id: (seq as u64) * 1000 + n,
        raw_tx: Bytes::from(vec![seq ^ (n as u8); 64]),
        sender: Address::repeat_byte(seq),
        tx_hash: B256::repeat_byte((seq ^ (n as u8)).wrapping_add(0x40)),
    }
}

// Single-thread runtime: `rusteron_client::Aeron` is `!Send + !Sync` (the
// C client is thread-confined). With `flavor = "multi_thread"` tokio could
// move this task across worker threads at await points, which is UB for
// the `Rc<Aeron>` held below. The whole test runs on one OS thread.
#[tokio::test(flavor = "current_thread")]
async fn split_architecture_m_plus_one_e2e() {
    if !docker_available().await {
        eprintln!("skipping: docker not available");
        return;
    }

    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container started");

    let endpoint = cluster.archive_control_endpoint(0).await;
    eprintln!("aeron archive control: {endpoint}");

    // Per-A streams are IPC-aliased per sequencer; tx_ordering is the UDP
    // endpoint the container exposes. Stream-id arithmetic matches the
    // ChannelsConfig defaults.
    let mut cfg = LogConfig::default();
    cfg.channels.tx_ordering_channel = format!("aeron:udp?endpoint={endpoint}|alias=b");
    cfg.channels.tx_ordering_stream_id = 1001;
    // TxData stays on IPC inside the container's media driver (this test
    // runs the client *against* the container, so the per-A streams need to
    // be reachable from outside too — use UDP with distinct endpoints).
    cfg.channels.tx_data_channel_template =
        format!("aeron:udp?endpoint={endpoint}|alias=a-{{sid}}");
    cfg.channels.tx_data_stream_id_base = 2000;

    let ctx = rusteron_client::AeronContext::new().expect("aeron context");
    let aeron = Rc::new(rusteron_client::Aeron::new(&ctx).expect("aeron connect to container"));
    aeron.start().expect("aeron start");

    // Per-sequencer tx_data publishers + per-A subscribers.
    let mut a_pubs: Vec<TxDataPublisher> = Vec::with_capacity(M as usize);
    let subs = Subscribers {
        aeron: aeron.clone(),
        ch: cfg.channels.clone(),
    };
    let mut a_subs = Vec::with_capacity(M as usize);
    for sid in 0..M {
        a_pubs.push(TxDataPublisher::open(&aeron, &cfg.channels, sid).unwrap());
        a_subs.push(subs.tx_data(sid).unwrap());
    }

    let b_pub = TxOrderingPublisher::open(&aeron, &cfg.channels).unwrap();
    let mut b_sub = subs.tx_ordering().unwrap();

    // Publish TXS_PER_SEQ envelopes per sequencer; remember each
    // (sequencer_id, tx_data_position, expected correlation_id).
    let mut publish_plan: Vec<(u8, BPosition, u64)> = Vec::new();
    for n in 0..TXS_PER_SEQ {
        for sid in 0..M {
            let env = env(sid, n);
            let pos_a = a_pubs[sid as usize].publish(&env).unwrap();
            publish_plan.push((sid, pos_a, env.correlation_id));
        }
    }

    // Interleave TxRefs onto tx_ordering in a non-trivial order so we exercise
    // the per-A-buffer / B-canonical-order join.
    for (sid, pos_a, _corr) in &publish_plan {
        b_pub
            .publish_ref(&TxRef {
                tx_hash: alloy_primitives::B256::ZERO,
                shard_id: *sid,
                tx_data_position: *pos_a,
            })
            .unwrap();
    }

    // Drain tx_data subscriptions into per-A buffers (executor model:
    // M+1 reader threads — here, polled sequentially for test determinism).
    let mut a_buffer: HashMap<(u8, BPosition), TxEnvelope> = HashMap::new();
    let total_a = (TXS_PER_SEQ as usize) * (M as usize);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while a_buffer.len() < total_a && std::time::Instant::now() < deadline {
        for (i, sub) in a_subs.iter_mut().enumerate() {
            let sid = i as u8;
            sub.poll(
                |env, pos| {
                    a_buffer.insert((sid, pos), env);
                },
                256,
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        a_buffer.len(),
        total_a,
        "expected {total_a} tx_data envelopes, got {}",
        a_buffer.len()
    );

    // Walk tx_ordering in canonical order. For each TxRef, join against the
    // A-buffer; recover the canonical sequence of correlation_ids and
    // compare to publish_plan.
    let mut canonical: Vec<u64> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while canonical.len() < publish_plan.len() && std::time::Instant::now() < deadline {
        b_sub.poll(
            |m, _b_pos| {
                if let TxOrderingMessage::TxRef(r) = m {
                    let env = a_buffer
                        .remove(&(r.shard_id, r.tx_data_position))
                        .expect("TxRef must hit A-buffer");
                    canonical.push(env.correlation_id);
                }
            },
            256,
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(canonical.len(), publish_plan.len());

    let expected: Vec<u64> = publish_plan.iter().map(|(_, _, c)| *c).collect();
    assert_eq!(
        canonical, expected,
        "B-canonical order must match the publish_ref order"
    );
    assert!(
        a_buffer.is_empty(),
        "executor must have evicted every referenced envelope"
    );

    drop(cluster);
}
