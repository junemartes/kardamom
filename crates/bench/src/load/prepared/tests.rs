use std::num::NonZeroU32;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use alloy_primitives::{hex, keccak256};
use jsonrpsee::server::{RpcModule, Server, ServerHandle};

use crate::load::{LoadConfig, NonceStart, SenderRange, prepare};

struct Ingress {
    url: String,
    handle: ServerHandle,
}

impl Ingress {
    async fn start() -> Self {
        Self::with_release(None, 0).await
    }

    async fn with_release(
        release: Option<tokio::sync::watch::Receiver<bool>>,
        accepted_prefix: usize,
    ) -> Self {
        let mut module = RpcModule::new(AtomicUsize::new(0));
        module
            .register_method("eth_chainId", |_, _, _| "0x1")
            .unwrap();
        module
            .register_async_method("eth_sendRawTransaction", move |params, calls, _| {
                let release = release.clone();
                let held = calls.fetch_add(1, Ordering::Relaxed) >= accepted_prefix;
                async move {
                    if held {
                        Self::await_release(release).await;
                    }
                    let raw = params.one::<String>().unwrap();
                    format!("{:#x}", keccak256(hex::decode(raw).unwrap()))
                }
            })
            .unwrap();
        module
            .register_method("eth_getTransactionReceipt", |_, _, _| {
                serde_json::json!({
                    "status":"0x1", "gasUsed":"0x5208", "blockNumber":"0x1",
                    "from":"0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
                })
            })
            .unwrap();
        let server = Server::builder().build("127.0.0.1:0").await.unwrap();
        Self {
            url: format!("http://{}", server.local_addr().unwrap()),
            handle: server.start(module),
        }
    }

    async fn await_release(release: Option<tokio::sync::watch::Receiver<bool>>) {
        if let Some(mut release) = release {
            release.wait_for(|released| *released).await.unwrap();
        }
    }

    fn config(&self, duration: Duration, output: &Path) -> LoadConfig {
        LoadConfig {
            rpc: self.url.clone(),
            chain_id: Some(1),
            duration,
            target_tps: NonZeroU32::new(100).unwrap(),
            sender_range: SenderRange::new(0, NonZeroU32::MIN).unwrap(),
            nonce_start: NonceStart::Fixed(0),
            max_in_flight: NonZeroU32::new(8).unwrap(),
            chaos_mode: true,
            fixed_rate: true,
            subscribe: false,
            scrape: Vec::new(),
            output: Some(output.to_path_buf()),
            drain_timeout: Duration::from_secs(1),
            ..LoadConfig::default()
        }
    }
}

impl Drop for Ingress {
    fn drop(&mut self) {
        let _ = self.handle.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_external_stop_drains_accepted_transactions_and_reports_actual_duration() {
    let ingress = Ingress::start().await;
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("report.json");
    let prepared = prepare(ingress.config(Duration::from_secs(5), &output))
        .await
        .unwrap();
    let progress = prepared.progress();
    let task = tokio::spawn(prepared.run());
    tokio::time::timeout(Duration::from_secs(3), async {
        while progress.accepted() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(progress.is_flowing());
    progress.finish_submissions();
    assert!(task.await.unwrap().unwrap());
    assert!(!progress.is_submitting());
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
    assert!(report["duration_secs"].as_f64().unwrap() < 1.0);
    assert!(report["verdict"]["accepted"].as_u64().unwrap() >= 3);
    assert_eq!(
        report["verdict"]["accepted"],
        report["verdict"]["receipted"]
    );
    assert_eq!(report["verdict"]["missing"], 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_without_external_control_reports_its_measured_window() {
    let ingress = Ingress::start().await;
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("report.json");
    assert!(
        prepare(ingress.config(Duration::from_millis(250), &output))
            .await
            .unwrap()
            .run()
            .await
            .unwrap()
    );
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
    assert!((0.20..2.0).contains(&report["duration_secs"].as_f64().unwrap()));
    assert!(report["verdict"]["accepted"].as_u64().unwrap() > 0);
    assert_eq!(report["verdict"]["missing"], 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_inflight_queue_cannot_extend_the_submission_window() {
    let (release, held) = tokio::sync::watch::channel(false);
    let ingress = Ingress::with_release(Some(held), 0).await;
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("report.json");
    let mut config = ingress.config(Duration::from_millis(100), &output);
    config.max_in_flight = NonZeroU32::MIN;
    let prepared = prepare(config).await.unwrap();
    let progress = prepared.progress();
    let task = tokio::spawn(prepared.run());
    tokio::time::timeout(Duration::from_secs(3), async {
        while progress.tracker.counts().offered == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(2), async {
        while progress.is_submitting() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    release.send_replace(true);
    assert!(task.await.unwrap().unwrap());
    assert!(ended, "the paced window stays open behind the held submit");
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
    assert_eq!(
        report["verdict"]["offered"], 1,
        "no new submit after the deadline"
    );
    assert_eq!(report["verdict"]["accepted"], 1);
    assert_eq!(
        report["verdict"]["receipted"], 1,
        "already offered work still drains"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unfinished_submit_cannot_produce_a_passing_measurement() {
    let (release, held) = tokio::sync::watch::channel(false);
    let ingress = Ingress::with_release(Some(held), 1).await;
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("report.json");
    let mut config = ingress.config(Duration::from_millis(250), &output);
    config.chaos_mode = false;
    config.max_in_flight = NonZeroU32::MIN;
    config.drain_timeout = Duration::from_millis(100);
    let prepared = prepare(config).await.unwrap();
    let progress = prepared.progress();
    let result = tokio::time::timeout(Duration::from_secs(8), prepared.run())
        .await
        .unwrap();
    release.send_replace(true);
    let counts = progress.tracker.counts();
    assert_eq!(
        (counts.offered, counts.accepted, counts.receipted),
        (2, 1, 1)
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("unfinished submit")
    );
    assert!(
        !output.exists(),
        "an unknown outcome cannot publish a passing report"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_prepared_run_stops_its_background_receipt_readers() {
    let (release, held) = tokio::sync::watch::channel(false);
    let ingress = Ingress::with_release(Some(held), 0).await;
    let directory = tempfile::tempdir().unwrap();
    let config = ingress.config(
        Duration::from_secs(5),
        &directory.path().join("report.json"),
    );
    let prepared = prepare(config).await.unwrap();
    let progress = prepared.progress();
    let task = tokio::spawn(prepared.run());
    tokio::time::timeout(Duration::from_secs(2), async {
        while progress.tracker.counts().offered == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let stopped = tokio::time::timeout(Duration::from_secs(1), async {
        while std::sync::Arc::strong_count(&progress.tracker) != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    release.send_replace(true);
    assert!(
        stopped,
        "a detached background reader still owns the delivery tracker"
    );
    assert!(!progress.is_submitting());
}
