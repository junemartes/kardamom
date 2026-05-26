//! Multi-process end-to-end test.
//!
//! Brings up real Aeron in Docker, then spawns each kardamom service
//! (sealer, sequencer, executor, ingress) as its own `kardamom-<svc>`
//! subprocess pointed at the container's bind-mounted `aeron.dir`. Submits
//! signed transfers via the proxy's JSON-RPC server using `jsonrpsee`'s
//! HTTP client, and asserts each submission returns a receipt with the
//! expected `tx_hash` and `status=true`.
//!
//! This is the most realistic deployment shape: ansible / nomad would
//! launch each `kardamom-*` binary the same way this test does.
//!
//! Gated on `feature = "full-pipeline-e2e"` + `#[ignore]`. To run locally:
//!
//! ```bash
//! cargo test -p e2e --features full-pipeline-e2e \
//!   --test multiprocess_e2e -- --ignored --nocapture
//! ```

#![cfg(feature = "full-pipeline-e2e")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, U256};
use alloy_rlp::{Decodable, Encodable};
use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::rpc_params;
use kardamom_log::testing::AeronTestCluster;
use serde_json::Value;
use tempfile::TempDir;

const CHAIN_ID: u64 = 1;
const JSONRPC_PORT: u16 = 18545;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(3);

async fn docker_available() -> bool {
    use tokio::process::Command;
    Command::new("docker")
        .arg("info")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + cargo-built kardamom-* bins; run with `cargo test -p e2e --features full-pipeline-e2e --test multiprocess_e2e -- --ignored --nocapture`"]
async fn multiprocess_e2e_signed_transfer_round_trip() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_test_writer()
        .try_init();

    if !docker_available().await {
        eprintln!("skipping: docker not available");
        return;
    }

    // ----- Build the service binaries up front so the spawn step below
    // ----- doesn't race the `cargo build` from `--features full-pipeline-e2e`.
    build_service_bins();

    // ----- Bring up Aeron in a container, with the host bind-mount path
    // ----- we'll point each service at via `--aeron-dir`.
    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container up");
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();
    tracing::info!(aeron_dir = %aeron_dir.display(), "aeron container running");

    // ----- Per-service TOML configs in a tempdir. The sealer + sequencer
    // ----- bins enforce `deny_unknown_fields` so the schema must match.
    let cfg_dir = TempDir::new().expect("cfg tempdir");
    let sealer_cfg = write_sealer_config(cfg_dir.path());
    let sequencer_cfg = write_sequencer_config(cfg_dir.path());
    let executor_cfg = write_executor_config(cfg_dir.path());
    let ingress_cfg = write_ingress_config(cfg_dir.path());

    let target_bin = workspace_target_bin();

    // ----- Spawn the four services. SIGTERM order on teardown is reverse:
    // ----- ingress (stops new submissions) → executor → sequencer → sealer.
    let sealer = ChildGuard::spawn(
        "kardamom-sealer",
        Command::new(target_bin.join("kardamom-sealer"))
            .arg("--config")
            .arg(&sealer_cfg)
            .arg("--aeron-dir")
            .arg(&aeron_dir)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit()),
    );
    let sequencer = ChildGuard::spawn(
        "kardamom-sequencer",
        Command::new(target_bin.join("kardamom-sequencer"))
            .arg("--config")
            .arg(&sequencer_cfg)
            .arg("--aeron-dir")
            .arg(&aeron_dir)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit()),
    );
    let executor = ChildGuard::spawn(
        "kardamom-executor",
        Command::new(target_bin.join("kardamom-executor"))
            .arg("--config")
            .arg(&executor_cfg)
            .arg("--aeron-dir")
            .arg(&aeron_dir)
            .arg("--shards")
            .arg("1")
            .arg("--chain-id")
            .arg(CHAIN_ID.to_string())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit()),
    );
    let ingress = ChildGuard::spawn(
        "kardamom-ingress",
        Command::new(target_bin.join("kardamom-ingress"))
            .arg("--config")
            .arg(&ingress_cfg)
            .arg("--aeron-dir")
            .arg(&aeron_dir)
            .arg("--shards")
            .arg("1")
            .arg("--jsonrpc-bind")
            .arg(format!("127.0.0.1:{JSONRPC_PORT}"))
            // No quorum-watermark publisher in this test setup; release
            // submit_raw waiters as soon as the receipt arrives.
            .arg("--ack-policy")
            .arg("on-offer")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit()),
    );

    // ----- Wait for the ingress to bind. Each AeronRuntime takes a couple
    // ----- of seconds to come up because the C client + Media Driver
    // ----- handshake is slow. 30 s is a generous cap.
    let rpc_url = format!("http://127.0.0.1:{JSONRPC_PORT}");
    wait_for_jsonrpc(&rpc_url, Duration::from_secs(30))
        .await
        .expect("ingress JSON-RPC ready");

    // ----- Submit N signed transfers via eth_sendRawTransaction. The
    // ----- proxy waits for the receipt via its in-memory tx_receipts cache
    // ----- (post-PR-#29) and returns the receipt body in the RPC response.
    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(60))
        .build(&rpc_url)
        .expect("http client");

    let alice = signer_from_seed(0x11);
    let bob_addr = Address::repeat_byte(0x22);
    const N: u64 = 3;

    for nonce in 0..N {
        let raw = signed_transfer(&alice, bob_addr, U256::from(1u64), nonce);
        let raw_hex = format!("0x{}", hex::encode(&raw));
        let resp: Value = client
            .request("eth_sendRawTransaction", rpc_params![raw_hex])
            .await
            .expect("eth_sendRawTransaction");
        // The proxy returns a serialized TransactionReceipt; assert the
        // tx_hash matches what we signed.
        let env = ConsensusEnvelope::decode(&mut raw.as_slice()).expect("decode my tx");
        let expected_hash = *env.tx_hash();
        let received_hash = resp
            .get("transactionHash")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<B256>().ok())
            .unwrap_or_else(|| panic!("missing transactionHash in response: {resp}"));
        assert_eq!(received_hash, expected_hash, "tx {nonce}: tx_hash mismatch");
        let status = resp
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        // alloy serialises status as "0x1" / "0x0".
        assert_eq!(
            status, "0x1",
            "tx {nonce}: receipt status not success ({resp})"
        );
        tracing::info!(nonce, tx_hash = ?expected_hash, "received receipt");
    }

    tracing::info!("all {N} receipts validated; tearing down");

    // ----- Teardown. Drop in reverse order so the channels everyone
    // ----- subscribes to remain alive while consumers tear down.
    drop(ingress);
    drop(executor);
    drop(sequencer);
    drop(sealer);
    drop(cluster);
}

// ============================================================================
// Helpers
// ============================================================================

/// Compile every service bin once at the start of the test so the spawn
/// loop below doesn't have to race a partial cargo build. `--bin <name>`
/// on its own resolves only against the current package's bin targets
/// (the `e2e` test crate has none); pass `-p <pkg>` for each so cargo
/// looks up the right workspace member.
fn build_service_bins() {
    let st = Command::new(env!("CARGO"))
        .args([
            "build",
            "-p",
            "kardamom-sealer",
            "-p",
            "kardamom-sequencer",
            "-p",
            "kardamom-executor",
            "-p",
            "kardamom-ingress",
            "--bins",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("cargo build status");
    assert!(st.success(), "failed to build kardamom service bins");
}

/// Resolve `<workspace>/target/<profile>` where the service bins live.
/// `CARGO_MANIFEST_DIR` points at `crates/e2e`; walk two levels up.
fn workspace_target_bin() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.parent().unwrap().parent().unwrap();
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    workspace.join("target").join(profile)
}

fn write_sealer_config(dir: &Path) -> PathBuf {
    let p = dir.join("sealer.toml");
    let body = r#"
host_id = 0
channel_b_uri = "aeron:udp?endpoint=224.0.1.1:40001"
channel_b_tx_stream_id = 1
channel_b_boundary_stream_id = 1001
tick_interval_ms = 250
"#;
    std::fs::write(&p, body).unwrap();
    p
}

fn write_sequencer_config(dir: &Path) -> PathBuf {
    let p = dir.join("sequencer.toml");
    let body = r#"
partition_count = 1
partition_index = 0
sequencer_id = 0
max_pending_per_sender = 16
backpressure_policy = "return_immediately"
"#;
    std::fs::write(&p, body).unwrap();
    p
}

fn write_executor_config(dir: &Path) -> PathBuf {
    // The kardamom-executor bin only checks the config file is present —
    // runtime tuning is via CLI flags. Write an empty TOML.
    let p = dir.join("executor.toml");
    std::fs::write(&p, "").unwrap();
    p
}

fn write_ingress_config(dir: &Path) -> PathBuf {
    // Same as executor — the kardamom-ingress bin uses CLI flags for
    // runtime tuning today. Write an empty TOML so the presence check passes.
    let p = dir.join("ingress.toml");
    std::fs::write(&p, "").unwrap();
    p
}

fn signer_from_seed(byte: u8) -> PrivateKeySigner {
    let mut bytes = [0u8; 32];
    bytes[31] = byte;
    PrivateKeySigner::from_bytes(&bytes.into()).expect("derive signer")
}

fn signed_transfer(signer: &PrivateKeySigner, to: Address, value: U256, nonce: u64) -> Vec<u8> {
    let mut tx = TxLegacy {
        chain_id: Some(CHAIN_ID),
        nonce,
        gas_price: 1,
        gas_limit: 21_000,
        to: to.into(),
        value,
        input: Default::default(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).expect("sign");
    let env: ConsensusEnvelope = tx.into_signed(sig).into();
    let mut out = Vec::with_capacity(256);
    env.encode(&mut out);
    out
}

async fn wait_for_jsonrpc(url: &str, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(2))
        .build(url)
        .map_err(|e| format!("http client: {e}"))?;
    while tokio::time::Instant::now() < deadline {
        let res: Result<Value, _> = client.request("eth_chainId", rpc_params![]).await;
        if res.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err("timed out waiting for ingress JSON-RPC".into())
}

// ============================================================================
// Child-process RAII
// ============================================================================

/// Wraps a `Child` so its `Drop` impl sends SIGTERM and waits a short grace
/// period for the binary to exit cleanly. Without this, a test panic leaves
/// orphaned `kardamom-*` processes behind that can hold ports / shm files.
struct ChildGuard {
    name: &'static str,
    child: Option<Child>,
}

impl ChildGuard {
    fn spawn(name: &'static str, cmd: &mut Command) -> Self {
        let child = cmd.spawn().unwrap_or_else(|e| panic!("spawn {name}: {e}"));
        tracing::info!(name, pid = child.id(), "spawned");
        Self {
            name,
            child: Some(child),
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            tracing::info!(name = self.name, pid = child.id(), "stopping");
            // Best-effort SIGTERM.
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                let pid = child.id() as i32;
                unsafe {
                    libc::kill(pid, libc::SIGTERM);
                }
                let deadline = std::time::Instant::now() + SHUTDOWN_GRACE;
                while std::time::Instant::now() < deadline {
                    match child.try_wait() {
                        Ok(Some(s)) => {
                            tracing::info!(
                                name = self.name,
                                code = s.code(),
                                signal = s.signal(),
                                "exited"
                            );
                            return;
                        }
                        Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                        Err(_) => break,
                    }
                }
            }
            tracing::warn!(name = self.name, "service did not exit cleanly; SIGKILL");
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
