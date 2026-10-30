//! The DA-lag halt as the ingress sees it on the cluster egress. The real
//! egress observer decodes the status frames of the sealer, in the layout
//! that the Java sealer writes and the shared golden frames pin. The real
//! ingress server then follows them: a halted status pauses the ingress on
//! the sealer's `da_lag` root, refuses a submit with the typed error, and
//! raises the halt gauge on the metrics page. A sealed head that grows
//! while halted shows on the page too. A status after the batcher posts
//! again clears the halt, lowers the gauge, and the next submit goes out.

use std::time::Duration;

use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;
use kardamom_cluster_adapter::gateway::fakes::FakeEgress;
use kardamom_cluster_adapter::wire::encode_status;
use kardamom_ingress::cluster::{ClusterWatermarkObserver, Observed};
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::CHAIN_HALTED_CODE;
use kardamom_ingress::test_support::{http_client, sign_legacy, start_test_server};
use kardamom_obs::lifecycle::process;
use kardamom_obs::testkit::{free_port, poll_until, scrape};
use kardamom_types::ClusterStatus;
use tokio::sync::watch;

/// The egress tap of the ingress binary: every status frame goes to the
/// status channel of the proxy.
fn spawn_tap(egress: FakeEgress, bus: watch::Sender<ClusterStatus>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut observer = ClusterWatermarkObserver::new(egress);
        std::iter::from_fn(|| observer.next_event())
            .filter_map(|event| match event {
                Observed::Status(status) => Some(status),
                Observed::Progress(_) => None,
            })
            .for_each(|status| {
                bus.send_replace(status);
            });
    })
}

/// The value of the first line of `body` that starts with `name` and holds
/// `label`.
fn sample(body: &str, name: &str, label: &str) -> Option<f64> {
    body.lines()
        .filter(|line| line.starts_with(name) && line.contains(label))
        .find_map(|line| line.rsplit(' ').next()?.parse().ok())
}

/// Wait until the metrics page at `url` shows `value` for `name`.
async fn await_sample(url: &str, name: &str, label: &str, value: f64) {
    poll_until(
        &format!("{name}{{{label}}} reaches {value}"),
        Duration::from_secs(5),
        Duration::from_millis(20),
        async || {
            let body = scrape(url).await;
            Ok((sample(&body, name, label) == Some(value)).then_some(()))
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn sealer_status_frames_halt_and_resume_the_ingress_and_its_halt_gauge() {
    let metrics_addr = free_port();
    kardamom_obs::init("ingress", metrics_addr, "local", "test", "test")
        .await
        .unwrap();
    let page = format!("http://{metrics_addr}/metrics");
    let server = start_test_server(IngressConfig::default()).await;
    let client = http_client(server.addr);
    let egress = FakeEgress::new();
    let tap = spawn_tap(egress.clone(), server.mock.cluster_status_bus.clone());

    let running = ClusterStatus {
        posted_head: 5,
        sealed_head: 9,
        budget_blocks: 10,
        halted: false,
        retained_frames: 40,
        floor_index: 0,
        floor_block: 1,
        ..Default::default()
    };
    egress.push(encode_status(&running));
    await_sample(&page, "kardamom_ingress_cluster_posted_head", "", 5.0).await;
    await_sample(
        &page,
        "kardamom_ingress_cluster_da_lag_budget_blocks",
        "",
        10.0,
    )
    .await;

    // The batcher stops posting: the sealer halts past the budget, and the
    // boundaries go on.
    egress.push(encode_status(&ClusterStatus {
        sealed_head: 16,
        halted: true,
        ..running
    }));
    await_sample(&page, "kardamom_halt", "cause=\"da_lag\"", 1.0).await;
    assert!(
        process().slots().halt.is_none(),
        "the ingress pauses, it does not halt"
    );
    assert!(process().slots().pause.is_some());
    egress.push(encode_status(&ClusterStatus {
        sealed_head: 20,
        halted: true,
        ..running
    }));
    await_sample(&page, "kardamom_ingress_cluster_sealed_head", "", 20.0).await;
    let signer = PrivateKeySigner::random();
    let refusal = client
        .request::<alloy_primitives::B256, _>(
            "eth_sendRawTransaction",
            rpc_params![sign_legacy(&signer, 0)],
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(
        refusal.contains(&CHAIN_HALTED_CODE.to_string()),
        "{refusal}"
    );
    assert!(refusal.contains("/halt"), "{refusal}");

    // The batcher posts past the frozen head: the halt ends.
    egress.push(encode_status(&ClusterStatus {
        posted_head: 20,
        sealed_head: 20,
        halted: false,
        ..running
    }));
    await_sample(&page, "kardamom_halt", "cause=\"da_lag\"", 0.0).await;
    await_sample(&page, "kardamom_ingress_cluster_posted_head", "", 20.0).await;
    poll_until(
        "the ingress resumes",
        Duration::from_secs(5),
        Duration::from_millis(20),
        async || Ok(process().slots().pause.is_none().then_some(())),
    )
    .await
    .unwrap();
    let hash: alloy_primitives::B256 = client
        .request(
            "kardamom_sendRawTransactionAsync",
            rpc_params![sign_legacy(&signer, 0)],
        )
        .await
        .expect("the submit goes out once the sealer accepts again");
    assert_ne!(hash, alloy_primitives::B256::ZERO);

    egress.close();
    tap.join().unwrap();
    server.handle.stop().unwrap();
}
