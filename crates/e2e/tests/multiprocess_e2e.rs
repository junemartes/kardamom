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
use alloy_primitives::{Address, B256, TxKind, U256, address};
use alloy_rlp::{Decodable, Encodable};
use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::rpc_params;
use kardamom_log::aeron_live::{
    AeronRuntime, TxDepositsPublisherHandle, TxReceiptsSubscriberHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::testing::AeronTestCluster;
use kardamom_types::Deposit;
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

    // Genesis allocation: fund Alice so the executor's revm has a balance
    // to debit during the test's transfers. Bob's address is the recipient.
    let alice = signer_from_seed(0x11);
    let alice_addr = alice.address();
    let bob_addr = Address::repeat_byte(0x22);
    let genesis_path = write_genesis_toml(cfg_dir.path(), CHAIN_ID, alice_addr);

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
            .arg("--chain")
            .arg(&genesis_path)
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

    // Bootstrap gate: ingress only flips kardamom_isReady to true once all of
    // its bootstrapped subscriptions report caught-up. See #31.
    //
    // Known caveat (follow-up): the service binaries connect to the Aeron
    // Archive via AeronArchiveContext::new() which defaults to localhost:8010.
    // In this test cluster the archive's control port is mapped to a random
    // host port and the binaries are not (yet) configured to use it, so the
    // archive client connect fails and all bootstrapped subs degrade to the
    // Connected policy. wait_for_ready() still exercises the readiness gate
    // (Connected signals caught-up after the first live frame), but the
    // archive-Replay path is NOT exercised end-to-end here. The dedicated
    // archive_bootstrap_e2e test (or a configured docker harness) should
    // cover the Replay path before this work is considered complete.
    wait_for_ready(&rpc_url, Duration::from_secs(60))
        .await
        .expect("ingress bootstrap did not complete");

    // ----- Submit N signed transfers via eth_sendRawTransaction. The
    // ----- proxy waits for the receipt via its in-memory tx_receipts cache
    // ----- (post-PR-#29) and returns the receipt body in the RPC response.
    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(60))
        .build(&rpc_url)
        .expect("http client");

    const N: u64 = 3;
    let _ = alice_addr;

    for nonce in 0..N {
        let raw = signed_transfer(&alice, bob_addr, U256::from(1u64), nonce);
        let raw_hex = format!("0x{}", hex::encode(&raw));
        // `eth_sendRawTransaction` returns just the tx_hash. The proxy
        // waits for the receipt internally and only resolves the request
        // once the receipt has arrived on tx_receipts.
        let returned_hash: B256 = client
            .request("eth_sendRawTransaction", rpc_params![&raw_hex])
            .await
            .expect("eth_sendRawTransaction");

        // Verify it's the hash we signed.
        let env = ConsensusEnvelope::decode(&mut raw.as_slice()).expect("decode my tx");
        let expected_hash = *env.tx_hash();
        assert_eq!(returned_hash, expected_hash, "tx {nonce}: tx_hash mismatch");

        // Fetch the receipt body via eth_getTransactionReceipt and assert
        // status=success.
        let receipt: Value = client
            .request("eth_getTransactionReceipt", rpc_params![returned_hash])
            .await
            .expect("eth_getTransactionReceipt");
        let status = receipt
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert_eq!(
            status, "0x1",
            "tx {nonce}: receipt status not success ({receipt})"
        );
        tracing::info!(nonce, tx_hash = ?expected_hash, "receipt verified");
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

/// L1 deposit → L2 mint round-trip.
///
/// Bypasses the L1 + DA watcher: opens a `TxDepositsPublisherHandle` in the
/// test process and offers a synthetic `Deposit` directly onto `tx_deposits`,
/// then asserts the executor produces a matching success `Receipt` (joined
/// via `source_hash`) on `tx_receipts`. Exercises the new bin wiring:
/// sequencer subscribes tx_deposits + emits `DepositRef`, executor joins +
/// executes via the deposit code path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker / host-native MD + cargo-built kardamom-* bins; run with `cargo test -p e2e --features full-pipeline-e2e --test multiprocess_e2e -- --ignored --nocapture multiprocess_e2e_deposit_round_trip`"]
async fn multiprocess_e2e_deposit_round_trip() {
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

    build_service_bins();

    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container up");
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();
    tracing::info!(aeron_dir = %aeron_dir.display(), "aeron container running");

    let cfg_dir = TempDir::new().expect("cfg tempdir");
    let sealer_cfg = write_sealer_config(cfg_dir.path());
    let sequencer_cfg = write_sequencer_config(cfg_dir.path());
    let executor_cfg = write_executor_config(cfg_dir.path());
    let ingress_cfg = write_ingress_config(cfg_dir.path());

    // Genesis: no funded EOAs needed — the deposit mints from L1.
    let genesis_path = write_genesis_toml(cfg_dir.path(), CHAIN_ID, Address::ZERO);

    let target_bin = workspace_target_bin();

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
            .arg("--chain")
            .arg(&genesis_path)
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
            .arg("--ack-policy")
            .arg("on-offer")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit()),
    );

    // Use the ingress's JSON-RPC bind as a readiness proxy: when it's
    // listening, every bin is at least partway through Aeron setup.
    let rpc_url = format!("http://127.0.0.1:{JSONRPC_PORT}");
    wait_for_jsonrpc(&rpc_url, Duration::from_secs(30))
        .await
        .expect("ingress JSON-RPC ready");

    // Open the test-side Aeron handles against the same MD: a publisher
    // on tx_deposits (impersonates the DA watcher) and a subscriber on
    // tx_receipts (so we can match the executor's deposit Receipt).
    let aeron_rt = AeronRuntime::spawn_with_dir(&aeron_dir).expect("aeron runtime");
    let channels = LogConfig::default().channels;
    let deposits_pub =
        TxDepositsPublisherHandle::open(&aeron_rt, &channels).expect("deposits pub");
    let mut receipts_sub =
        TxReceiptsSubscriberHandle::open(&aeron_rt, &channels).expect("receipts sub");

    // Bootstrap gate: ingress only flips kardamom_isReady to true once all of
    // its bootstrapped subscriptions report caught-up. See #31.
    //
    // Known caveat (follow-up): the service binaries connect to the Aeron
    // Archive via AeronArchiveContext::new() which defaults to localhost:8010.
    // In this test cluster the archive's control port is mapped to a random
    // host port and the binaries are not (yet) configured to use it, so the
    // archive client connect fails and all bootstrapped subs degrade to the
    // Connected policy. wait_for_ready() still exercises the readiness gate
    // (Connected signals caught-up after the first live frame), but the
    // archive-Replay path is NOT exercised end-to-end here. The dedicated
    // archive_bootstrap_e2e test (or a configured docker harness) should
    // cover the Replay path before this work is considered complete.
    wait_for_ready(&rpc_url, Duration::from_secs(60))
        .await
        .expect("ingress bootstrap did not complete");

    // Synthesise a Deposit. `from` is the aliased L1 sender (any address
    // works for this test — there's no signature on the deposit envelope);
    // mint credits `from`, then the inner call (here a zero-value no-op
    // call from `from` to `to`) runs against the credited balance. A
    // unique `source_hash` makes the executor's dedup happy.
    let aliased_from = Address::repeat_byte(0x11);
    let target = Address::repeat_byte(0xCC);
    const MINT_WEI: u128 = 100_000_000_000_000_000_000; // 100 ETH
    let source_hash = B256::repeat_byte(0xDA);
    let deposit = Deposit {
        source_hash,
        from: aliased_from,
        to: Some(target),
        mint: MINT_WEI,
        value: U256::ZERO,
        gas_limit: 100_000,
        is_system_transaction: false,
        input: bytes::Bytes::new(),
    };

    deposits_pub.publish(&deposit).expect("publish deposit");
    tracing::info!(?source_hash, "synthetic deposit published onto tx_deposits");

    // Wait for the matching Receipt on tx_receipts. The deposit reader
    // joins on source_hash inside the executor, then runs the deposit
    // pre-credit + inner call, then publishes the Receipt. The executor
    // sets `Receipt.tx_hash = deposit.source_hash` (deposits don't have
    // a 2718 keccak), so we match on that.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut matched: Option<kardamom_types::Receipt> = None;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(250), receipts_sub.recv()).await {
            Ok(Some((_pos, r))) if r.tx_hash == source_hash => {
                matched = Some(r);
                break;
            }
            Ok(Some(_)) => continue, // unrelated receipt (sealer's empty blocks)
            Ok(None) => panic!("tx_receipts subscription closed unexpectedly"),
            Err(_) => continue, // 250 ms tick — keep polling until deadline
        }
    }

    let r = matched.expect("no matching deposit receipt arrived within 30 s");
    assert!(r.status, "deposit receipt status was false: {r:?}");
    assert_eq!(r.from, aliased_from, "receipt.from mismatch");
    assert_eq!(r.to, Some(target), "receipt.to mismatch");
    assert_eq!(r.tx_hash, source_hash, "receipt.tx_hash != source_hash");
    assert_eq!(
        r.effective_gas_price, 0,
        "deposits pay no fee — effective_gas_price should be 0"
    );
    tracing::info!(
        block_number = r.block_number,
        gas_used = r.gas_used,
        "deposit receipt verified; tearing down"
    );

    // Tear down in reverse: drop the test-side Aeron handles first so the
    // bin processes don't see spurious subscription-closed errors on the
    // tx_receipts stream while they're still publishing.
    drop(receipts_sub);
    drop(deposits_pub);
    drop(aeron_rt);
    drop(ingress);
    drop(executor);
    drop(sequencer);
    drop(sealer);
    drop(cluster);
}

// ============================================================================
// Phase 2: anvil L1 → da-watcher → L2 round-trip
// ============================================================================

use alloy_node_bindings::Anvil;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::sol;
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_arg};

sol! {
    #[sol(rpc)]
    contract ETHLockbox {
        function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable;
    }
}

/// Factory owner used by the kardamom-deployer ERC-7955 factory bootstrap.
/// Pinned by `kardamom_deployer::tests` so dev/test deployments are
/// deterministic.
const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
/// Arbitrary L1 EOA that calls `depositETH` — distinct from the factory
/// owner to demonstrate the lockbox is permissionless.
const DEPOSITOR: Address = address!("000000000000000000000000000000000000C0FE");

/// Full L1 → L2 round-trip:
///
/// 1. Spin up anvil as the L1.
/// 2. Inject the ERC-7955 factory bytecode at the canonical address.
/// 3. Deploy `ETHLockbox` via `kardamom_deployer` (proxy + impl, all on L1).
/// 4. Spin up the five kardamom services (sealer, sequencer, executor,
///    ingress, da-watcher) — da-watcher pointed at anvil's RPC + the
///    deployed lockbox.
/// 5. Call `depositETH(target, …)` on L1 as DEPOSITOR.
/// 6. Wait for the L2 deposit `Receipt` on tx_receipts (matched by
///    `source_hash = Receipt.tx_hash`). The full path exercised:
///    L1 event → da-watcher derives `Deposit` → publishes on tx_deposits
///    → sequencer republishes `DepositRef` on tx_ordering → executor
///    joins, mints, publishes Receipt.
/// 7. Execute three L2 transfers from Alice (funded via genesis alloc).
/// 8. Deploy a tiny contract on L2 from Alice; assert
///    `contractAddress != 0x0` and `status = 0x1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires anvil (forge) + Docker / host-native MD + cargo-built kardamom-* bins; run with `cargo test -p e2e --features full-pipeline-e2e --test multiprocess_e2e -- --ignored --nocapture anvil_pipeline_e2e_l1_deposit_and_l2_round_trip`"]
async fn anvil_pipeline_e2e_l1_deposit_and_l2_round_trip() {
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

    // -------- L1: anvil + ERC-7955 factory + ETHLockbox -----------------
    // `--slots-in-an-epoch 1` makes anvil's `finalized` tag advance after
    // each new block (default value lags `latest` by ~64 blocks, mirroring
    // mainnet finality). The da-watcher reads only finalized blocks, so
    // without this the watcher would never see the deposit.
    let anvil = match Anvil::new()
        .arg("--slots-in-an-epoch")
        .arg("1")
        .try_spawn()
    {
        Ok(a) => a,
        Err(e) => {
            eprintln!("skipping: anvil unavailable: {e}");
            return;
        }
    };
    let l1_url = anvil.endpoint();
    tracing::info!(l1_url = %l1_url, "anvil L1 up");

    let l1_provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(l1_url.parse().expect("parse anvil endpoint"));

    // Inject the ERC-7955 factory runtime at its canonical address so the
    // deployer can drive it without us having to deploy the factory's
    // CREATE2 deployer ourselves.
    let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
    let _: serde_json::Value = l1_provider
        .raw_request("anvil_setCode".into(), (ERC7955_FACTORY, bytes_hex))
        .await
        .expect("anvil_setCode");

    // Fund + impersonate the deployer owner and the depositor. The owner
    // pays for factory bootstrap + the EthLockbox deploy; the depositor
    // pays for `depositETH` later.
    for who in [DEV_OWNER, DEPOSITOR] {
        let _: serde_json::Value = l1_provider
            .raw_request(
                "anvil_setBalance".into(),
                (who, U256::from(1_000_000_000_000_000_000_000u128)),
            )
            .await
            .expect("anvil_setBalance");
        let _: serde_json::Value = l1_provider
            .raw_request("anvil_impersonateAccount".into(), (who,))
            .await
            .expect("anvil_impersonateAccount");
    }

    // Deploy ETHLockbox via the deployer. `l2_minter` is an opaque param
    // baked into the lockbox at init — we don't exercise the L2-side mint
    // authority here (the executor mints unconditionally from a Deposit).
    let l2_minter = Address::from([0xBE; 20]);
    let deployer = Deployer::new(l1_provider.clone(), DEV_OWNER);
    deployer
        .ensure_factory(DEV_OWNER)
        .await
        .expect("ensure_factory");
    deployer
        .apply(
            &[Op::Deploy {
                l2_chain_id: CHAIN_ID,
                id: ContractId::EthLockbox,
                init_args: encode_address_arg(l2_minter),
            }],
            DEV_OWNER,
        )
        .await
        .expect("deploy ETHLockbox");

    let entries = deployer
        .addresses(Some(CHAIN_ID))
        .await
        .expect("addresses");
    let lockbox_addr = entries
        .iter()
        .find(|e| e.id == ContractId::EthLockbox.id())
        .expect("ETHLockbox registered")
        .proxy;
    tracing::info!(?lockbox_addr, "ETHLockbox deployed on L1");

    // -------- Kardamom: 5 services, including da-watcher ----------------
    build_service_bins_with_da_watcher();

    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container up");
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();
    tracing::info!(aeron_dir = %aeron_dir.display(), "aeron container running");

    let cfg_dir = TempDir::new().expect("cfg tempdir");
    let sealer_cfg = write_sealer_config(cfg_dir.path());
    let sequencer_cfg = write_sequencer_config(cfg_dir.path());
    let executor_cfg = write_executor_config(cfg_dir.path());
    let ingress_cfg = write_ingress_config(cfg_dir.path());

    // Alice is funded via genesis (for the L2 transfer + contract deploy
    // steps); Bob receives transfers; `l2_target` is the deposit's L2
    // recipient (the inner-call target).
    let alice = signer_from_seed(0x11);
    let alice_addr = alice.address();
    let bob_addr = Address::repeat_byte(0x22);
    let l2_target = Address::repeat_byte(0x44);
    let genesis_path = write_genesis_toml(cfg_dir.path(), CHAIN_ID, alice_addr);

    let target_bin = workspace_target_bin();

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
            .arg("--chain")
            .arg(&genesis_path)
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
            .arg("--ack-policy")
            .arg("on-offer")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit()),
    );
    let da_watcher = ChildGuard::spawn(
        "kardamom-da-watcher",
        Command::new(target_bin.join("kardamom-da-watcher"))
            .arg("--l1-rpc")
            .arg(&l1_url)
            .arg("--lockbox")
            .arg(format!("{lockbox_addr:#x}"))
            .arg("--aeron-dir")
            .arg(&aeron_dir)
            // Short poll: anvil produces blocks on demand, so 1 s is plenty.
            .arg("--poll-interval-secs")
            .arg("1")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit()),
    );

    let rpc_url = format!("http://127.0.0.1:{JSONRPC_PORT}");
    wait_for_jsonrpc(&rpc_url, Duration::from_secs(30))
        .await
        .expect("ingress JSON-RPC ready");
    // Bootstrap gate: ingress only flips kardamom_isReady to true once all of
    // its bootstrapped subscriptions report caught-up. See #31.
    //
    // Known caveat (follow-up): the service binaries connect to the Aeron
    // Archive via AeronArchiveContext::new() which defaults to localhost:8010.
    // In this test cluster the archive's control port is mapped to a random
    // host port and the binaries are not (yet) configured to use it, so the
    // archive client connect fails and all bootstrapped subs degrade to the
    // Connected policy. wait_for_ready() still exercises the readiness gate
    // (Connected signals caught-up after the first live frame), but the
    // archive-Replay path is NOT exercised end-to-end here. The dedicated
    // archive_bootstrap_e2e test (or a configured docker harness) should
    // cover the Replay path before this work is considered complete.
    wait_for_ready(&rpc_url, Duration::from_secs(60))
        .await
        .expect("ingress bootstrap did not complete");

    // Test-side: subscribe to tx_receipts so we can wait for the deposit's
    // executor receipt by source_hash.
    let aeron_rt = AeronRuntime::spawn_with_dir(&aeron_dir).expect("aeron runtime");
    let channels = LogConfig::default().channels;
    let mut receipts_sub =
        TxReceiptsSubscriberHandle::open(&aeron_rt, &channels).expect("receipts sub");
    tokio::time::sleep(Duration::from_secs(2)).await; // sub handshake

    // -------- Step 1: deposit on L1 -------------------------------------
    let lockbox = ETHLockbox::new(lockbox_addr, l1_provider.clone());
    const MINT_WEI: u128 = 50_000_000_000_000_000_000; // 50 ETH
    let l1_receipt = lockbox
        .depositETH(l2_target, 200_000, alloy_primitives::Bytes::new())
        .value(U256::from(MINT_WEI))
        .from(DEPOSITOR)
        .send()
        .await
        .expect("send depositETH")
        .get_receipt()
        .await
        .expect("L1 depositETH receipt");

    let log = l1_receipt
        .inner
        .logs()
        .iter()
        .find(|l| l.address() == lockbox_addr)
        .expect("DepositInitiated log on lockbox");
    let l1_block_hash = log.block_hash.expect("block_hash");
    let l1_log_index: u64 = log.log_index.expect("log_index");
    let expected_source_hash = kardamom_da_watcher::source_hash(l1_block_hash, l1_log_index);
    tracing::info!(
        ?expected_source_hash,
        ?l1_block_hash,
        l1_log_index,
        "L1 deposit submitted; waiting for da-watcher → L2"
    );

    // Force-advance the L1 chain a few blocks so the `finalized` tag moves
    // past the deposit's block. With `--slots-in-an-epoch 1`, finalised
    // lags `latest` by ~2 blocks; mining 5 more comfortably covers that
    // even after further L1 activity.
    for _ in 0..5 {
        let _: serde_json::Value = l1_provider
            .raw_request("evm_mine".into(), ())
            .await
            .expect("evm_mine");
    }

    // -------- Step 2: wait for the executor's deposit Receipt ------------
    // The executor stamps `Receipt.tx_hash = deposit.source_hash` for
    // deposits, so we match on source_hash directly. Generous 60 s window
    // covers anvil's finalisation lag + the watcher's 1 s poll + the
    // sequencer/executor processing time.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut deposit_receipt: Option<kardamom_types::Receipt> = None;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), receipts_sub.recv()).await {
            Ok(Some((_pos, r))) if r.tx_hash == expected_source_hash => {
                deposit_receipt = Some(r);
                break;
            }
            Ok(Some(_)) => continue,
            Ok(None) => panic!("tx_receipts subscription closed unexpectedly"),
            Err(_) => continue,
        }
    }
    let dr = deposit_receipt.expect("no L2 deposit receipt arrived within 60 s");
    assert!(dr.status, "deposit receipt status=false: {dr:?}");
    assert_eq!(dr.to, Some(l2_target), "deposit receipt.to mismatch");
    assert_eq!(
        dr.effective_gas_price, 0,
        "deposits pay no fee — effective_gas_price should be 0"
    );
    tracing::info!(
        block_number = dr.block_number,
        gas_used = dr.gas_used,
        "L1 → L2 deposit confirmed"
    );

    // -------- Step 3: three L2 transfers from Alice ---------------------
    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(60))
        .build(&rpc_url)
        .expect("http client");

    for nonce in 0..3 {
        let raw = signed_transfer(&alice, bob_addr, U256::from(1u64), nonce);
        let raw_hex = format!("0x{}", hex::encode(&raw));
        let tx_hash: B256 = client
            .request("eth_sendRawTransaction", rpc_params![&raw_hex])
            .await
            .expect("eth_sendRawTransaction (transfer)");
        let env = ConsensusEnvelope::decode(&mut raw.as_slice()).expect("decode my tx");
        assert_eq!(tx_hash, *env.tx_hash(), "transfer nonce={nonce} hash mismatch");
        let receipt: Value = client
            .request("eth_getTransactionReceipt", rpc_params![tx_hash])
            .await
            .expect("eth_getTransactionReceipt (transfer)");
        let status = receipt
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        assert_eq!(
            status, "0x1",
            "transfer nonce={nonce} status not success ({receipt})"
        );
    }
    tracing::info!("3 L2 transfers confirmed");

    // -------- Step 4: contract-creation on L2 from Alice ----------------
    // Tiny valid creation bytecode that returns a 1-byte runtime (0x01):
    //   60 01 60 0c 60 00 39 60 01 60 00 f3 01
    //   PUSH1 1 PUSH1 12 PUSH1 0 CODECOPY PUSH1 1 PUSH1 0 RETURN <runtime>
    let creation = hex::decode("6001600c60003960016000f301").expect("decode bytecode");
    let raw = signed_create_contract(&alice, &creation, 3);
    let raw_hex = format!("0x{}", hex::encode(&raw));
    let tx_hash: B256 = client
        .request("eth_sendRawTransaction", rpc_params![&raw_hex])
        .await
        .expect("eth_sendRawTransaction (deploy)");
    let receipt: Value = client
        .request("eth_getTransactionReceipt", rpc_params![tx_hash])
        .await
        .expect("eth_getTransactionReceipt (deploy)");
    let status = receipt
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert_eq!(status, "0x1", "deploy status not success ({receipt})");
    let contract_address = receipt
        .get("contractAddress")
        .and_then(|v| v.as_str())
        .expect("contractAddress");
    assert!(
        contract_address.starts_with("0x")
            && contract_address != "0x0000000000000000000000000000000000000000",
        "contractAddress not set ({contract_address})"
    );
    tracing::info!(contract_address, "L2 contract deployed");

    // -------- Teardown --------------------------------------------------
    drop(receipts_sub);
    drop(aeron_rt);
    drop(da_watcher);
    drop(ingress);
    drop(executor);
    drop(sequencer);
    drop(sealer);
    drop(cluster);
    drop(anvil);
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

/// Same as [`build_service_bins`] but also builds `kardamom-da-watcher`.
/// Used by the anvil-rooted Phase 2 test that needs the DA watcher
/// subprocess to read deposits from the L1.
fn build_service_bins_with_da_watcher() {
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
            "-p",
            "kardamom-da-watcher",
            "--bins",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("cargo build status");
    assert!(st.success(), "failed to build kardamom service bins (+ da-watcher)");
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
    // Match `LogConfig::default().channels.tx_ordering_*` so the sealer
    // publishes on the same Aeron stream the executor + sequencer
    // subscribe to. UDP multicast doesn't round-trip through macOS
    // loopback, so we use the IPC defaults for local + CI alike.
    let body = r#"
host_id = 0
channel_b_uri = "aeron:ipc?alias=tx-ordering"
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

/// Write a kardamom `Genesis` TOML that funds `alice` with 1000 ETH. The
/// executor bin loads this via `--chain` and seeds the in-memory state DB.
fn write_genesis_toml(dir: &Path, chain_id: u64, alice: Address) -> PathBuf {
    let p = dir.join("genesis.toml");
    let body = format!(
        r#"chain_id = {chain_id}

[[alloc]]
address = "{alice:#x}"
balance = "1000000000000000000000"
"#
    );
    std::fs::write(&p, body).unwrap();
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

/// Sign a contract-creation tx (legacy, no `to` set, `input` = creation code).
fn signed_create_contract(signer: &PrivateKeySigner, code: &[u8], nonce: u64) -> Vec<u8> {
    let mut tx = TxLegacy {
        chain_id: Some(CHAIN_ID),
        nonce,
        gas_price: 1,
        gas_limit: 500_000,
        to: TxKind::Create,
        value: U256::ZERO,
        input: code.to_vec().into(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).expect("sign");
    let env: ConsensusEnvelope = tx.into_signed(sig).into();
    let mut out = Vec::with_capacity(256 + code.len());
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

/// Polls `kardamom_isReady` on the ingress JSON-RPC endpoint until the
/// response reports `ready == true`, or `timeout` elapses.
///
/// `kardamom_isReady` is served by the ingress's `KardamomAdminApi`; it
/// returns `{"ready": bool, "gated_streams": [...]}`. When `ready` is true
/// every bootstrapped subscriber has caught up with the archive, and the
/// ingress is accepting new transactions. This helper is the correct
/// replacement for the 5 s unconditional sleep used in the original tests
/// (see issue #31).
async fn wait_for_ready(url: &str, timeout: Duration) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(2))
        .build(url)
        .map_err(|e| format!("http client: {e}"))?;
    while tokio::time::Instant::now() < deadline {
        let res: Result<Value, _> = client.request("kardamom_isReady", rpc_params![]).await;
        if let Ok(v) = res {
            if v.get("ready").and_then(|r| r.as_bool()).unwrap_or(false) {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("timed out waiting for kardamom_isReady".into())
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
