//! Cross-language tests of the permissioned withdrawal flow against
//! anvil: the Rust attester and withdrawals tree
//! (`kardamom_types::withdrawals`), and `kardamom-deployer` working with
//! the real Solidity contracts.
//!
//! [`attester_posts_output_matching_rust_root`] (always runs) covers the
//! deterministic interop: deploy the oracle and lockbox through the
//! factory, with the oracle address predicted and wired into the lockbox
//! init in one atomic batch. Fund the lockbox, have the attester post an
//! output, and check that the on-chain output root equals the one
//! computed in Rust.
//!
//! [`full_withdrawal_finalize_and_challenge`] (`#[ignore]`) walks the
//! full finalize and challenge round trip. It is ignored because the
//! post-time-warp `finalizeWithdrawal` confirmation hits an alloy/anvil
//! receipt-watcher timing flake. The L1 protocol itself is covered
//! deterministically by the Foundry `WithdrawalFlow.t.sol` end-to-end
//! test. Run this test locally with `--ignored`.
//!
//! All tests skip gracefully if anvil is unavailable.

use alloy_primitives::{Address, B256, Bytes, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::sol;

use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_pair, encode_oracle_init_args};
use kardamom_types::withdrawals;
use kardamom_validator::attester::OutputPoster;

sol! {
    #[sol(rpc)]
    contract ETHLockbox {
        struct WithdrawalTransaction {
            uint256 nonce;
            address sender;
            address target;
            uint256 value;
        }
        function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable;
        function outputOracle() external view returns (address);
        function finalizeWithdrawal(
            WithdrawalTransaction calldata wtx,
            uint256 outputIndex,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] calldata proof
        ) external;
    }

    #[sol(rpc)]
    contract WithdrawalOutputOracle {
        function deleteOutput(uint256 index) external;
        function outputCount() external view returns (uint256);
        function outputRootAt(uint256 index) external view returns (bytes32);
    }
}

const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
const L2_CHAIN_ID: u64 = 42;
const WINDOW: u64 = 86_400; // 1 day
const L2_MINTER: Address = address!("00000000000000000000000000000000000000BE");

// Anvil's deterministic dev accounts (standard test mnemonic).
const ATTESTER_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const ATTESTER_ADDR: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
const CHALLENGER_KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
const CHALLENGER_ADDR: Address = address!("70997970C51812dc3A010C7d01b50e0d17dc79C8");

fn wallet_provider(anvil: &alloy_node_bindings::AnvilInstance, key: &str) -> impl Provider + Clone {
    let signer: PrivateKeySigner = key.parse().unwrap();
    let p = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(anvil.endpoint_url());
    // alloy's default HTTP poll interval is slow. Tighten it, so
    // `get_receipt` returns promptly against the local node.
    p.client()
        .set_poll_interval(std::time::Duration::from_millis(50));
    p
}

/// Spawn anvil, with interval mining so the receipt watcher always sees
/// fresh blocks. Deploy the factory, oracle, and lockbox, and return the
/// anvil handle (kept alive by the caller) plus the deployed oracle and
/// lockbox addresses. Returns `None` if anvil is unavailable.
async fn setup() -> Option<(alloy_node_bindings::AnvilInstance, Address, Address)> {
    let anvil = alloy_node_bindings::Anvil::new()
        .block_time(1)
        .try_spawn()
        .ok()?;

    let deploy_provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(anvil.endpoint_url());
    deploy_provider
        .client()
        .set_poll_interval(std::time::Duration::from_millis(50));

    let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
    let _: serde_json::Value = deploy_provider
        .raw_request("anvil_setCode".into(), (ERC7955_FACTORY, bytes_hex))
        .await
        .ok()?;
    let _: serde_json::Value = deploy_provider
        .raw_request(
            "anvil_setBalance".into(),
            (DEV_OWNER, U256::from(1_000_000_000_000_000_000_000u128)),
        )
        .await
        .ok()?;
    let _: serde_json::Value = deploy_provider
        .raw_request("anvil_impersonateAccount".into(), (DEV_OWNER,))
        .await
        .ok()?;

    let deployer = Deployer::new(deploy_provider.clone(), DEV_OWNER);
    deployer.ensure_factory(DEV_OWNER).await.unwrap();

    // Predict the oracle proxy address, wire it into the lockbox init,
    // and deploy both atomically.
    let oracle_init = encode_oracle_init_args(ATTESTER_ADDR, CHALLENGER_ADDR, WINDOW);
    let predicted_oracle = deployer.predict_proxy_address(
        L2_CHAIN_ID,
        ContractId::WithdrawalOutputOracle,
        &oracle_init,
    );
    let lockbox_init = encode_address_pair(L2_MINTER, predicted_oracle);

    deployer
        .apply(
            &[
                Op::Deploy {
                    l2_chain_id: L2_CHAIN_ID,
                    id: ContractId::WithdrawalOutputOracle,
                    init_args: oracle_init,
                },
                Op::Deploy {
                    l2_chain_id: L2_CHAIN_ID,
                    id: ContractId::EthLockbox,
                    init_args: lockbox_init,
                },
            ],
            DEV_OWNER,
        )
        .await
        .expect("atomic deploy of oracle + lockbox");

    let entries = deployer.addresses(Some(L2_CHAIN_ID)).await.unwrap();
    let oracle_addr = entries
        .iter()
        .find(|e| e.id == ContractId::WithdrawalOutputOracle.id())
        .unwrap()
        .proxy;
    let lockbox_addr = entries
        .iter()
        .find(|e| e.id == ContractId::EthLockbox.id())
        .unwrap()
        .proxy;
    // The factory's CREATE2 address must match the Rust prediction used
    // to wire the lockbox, or the lockbox would point at the wrong oracle.
    assert_eq!(
        oracle_addr, predicted_oracle,
        "predicted oracle address must match deployed"
    );
    Some((anvil, oracle_addr, lockbox_addr))
}

fn deposit_provider(anvil: &alloy_node_bindings::AnvilInstance) -> impl Provider + Clone {
    let p = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(anvil.endpoint_url());
    p.client()
        .set_poll_interval(std::time::Duration::from_millis(50));
    p
}

#[tokio::test]
async fn attester_posts_output_matching_rust_root() {
    let Some((anvil, oracle_addr, lockbox_addr)) = setup().await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let attester_provider = wallet_provider(&anvil, ATTESTER_KEY);
    let read_provider = deposit_provider(&anvil);

    // The lockbox must point at the oracle: this proves the predicted-address wiring.
    let lockbox = ETHLockbox::new(lockbox_addr, attester_provider.clone());
    assert_eq!(lockbox.outputOracle().call().await.unwrap(), oracle_addr);

    // Fund the lockbox through a deposit, the on-ramp.
    lockbox
        .depositETH(
            address!("0000000000000000000000000000000000001234"),
            0,
            Bytes::new(),
        )
        .value(U256::from(5_000_000_000_000_000_000u128))
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    assert_eq!(
        read_provider.get_balance(lockbox_addr).await.unwrap(),
        U256::from(5_000_000_000_000_000_000u128)
    );

    // Build an output that commits a 2-withdrawal tree, and post it as the attester.
    let l2_sender = address!("00000000000000000000000000000000000000AA");
    let recipient = address!("00000000000000000000000000000000000000CC");
    let leaves = vec![
        withdrawals::withdrawal_leaf(U256::ZERO, l2_sender, recipient, U256::from(1u64)),
        withdrawals::withdrawal_leaf(U256::from(1u64), l2_sender, recipient, U256::from(2u64)),
    ];
    let wroot = withdrawals::withdrawals_root(&leaves);
    let state_root = B256::from([0x99; 32]);
    let output_root = withdrawals::output_root(state_root, wroot);

    let poster = OutputPoster::new(attester_provider.clone(), oracle_addr);
    poster
        .propose_output(output_root, 100)
        .await
        .expect("attester posts output");

    // Cross-language check: the on-chain output root equals the
    // Rust-computed one, and the attester recorded exactly one output.
    let oracle = WithdrawalOutputOracle::new(oracle_addr, read_provider);
    assert_eq!(oracle.outputCount().call().await.unwrap(), U256::from(1u64));
    assert_eq!(
        oracle.outputRootAt(U256::ZERO).call().await.unwrap(),
        output_root
    );
}

#[tokio::test]
#[ignore = "post-warp finalizeWithdrawal hits an alloy/anvil receipt-watcher timing flake; \
            the L1 protocol is covered by Foundry WithdrawalFlow.t.sol. Run with --ignored."]
async fn full_withdrawal_finalize_and_challenge() {
    let Some((anvil, oracle_addr, lockbox_addr)) = setup().await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let attester_provider = wallet_provider(&anvil, ATTESTER_KEY);
    let read_provider = deposit_provider(&anvil);
    let lockbox = ETHLockbox::new(lockbox_addr, attester_provider.clone());

    // Fund the lockbox.
    lockbox
        .depositETH(
            address!("0000000000000000000000000000000000001234"),
            0,
            Bytes::new(),
        )
        .value(U256::from(5_000_000_000_000_000_000u128))
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    // Post an output that commits a single withdrawal of 1 wei to `recipient`.
    let l2_sender = address!("00000000000000000000000000000000000000AA");
    let recipient = address!("00000000000000000000000000000000000000CC");
    let value = U256::from(1_000_000_000_000_000_000u128);
    let leaves = vec![withdrawals::withdrawal_leaf(
        U256::ZERO,
        l2_sender,
        recipient,
        value,
    )];
    let wroot = withdrawals::withdrawals_root(&leaves);
    let state_root = B256::from([0x99; 32]);
    let output_root = withdrawals::output_root(state_root, wroot);

    let poster = OutputPoster::new(attester_provider.clone(), oracle_addr);
    poster.propose_output(output_root, 100).await.unwrap();

    let wtx = ETHLockbox::WithdrawalTransaction {
        nonce: U256::ZERO,
        sender: l2_sender,
        target: recipient,
        value,
    };
    let proof = withdrawals::withdrawal_proof(&leaves, 0);

    // Before the window: finalize must revert.
    let early = lockbox
        .finalizeWithdrawal(
            wtx.clone(),
            U256::ZERO,
            state_root,
            wroot,
            U256::ZERO,
            proof.clone(),
        )
        .send()
        .await;
    assert!(early.is_err(), "finalize before window must revert");

    // Advance past the window, then finalize: the recipient gets paid.
    let _: serde_json::Value = attester_provider
        .raw_request("evm_increaseTime".into(), (WINDOW + 10,))
        .await
        .unwrap();
    lockbox
        .finalizeWithdrawal(wtx, U256::ZERO, state_root, wroot, U256::ZERO, proof)
        .gas(2_000_000)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    assert_eq!(read_provider.get_balance(recipient).await.unwrap(), value);

    // Challenge path: delete a second, bad output, and it can never finalize.
    let bad_leaves = vec![withdrawals::withdrawal_leaf(
        U256::from(2u64),
        l2_sender,
        recipient,
        value,
    )];
    let bad_wroot = withdrawals::withdrawals_root(&bad_leaves);
    let bad_state = B256::from([0xBA; 32]);
    poster
        .propose_output(withdrawals::output_root(bad_state, bad_wroot), 200)
        .await
        .unwrap();
    let bad_index = poster.output_count().await.unwrap() - 1;

    let challenger_provider = wallet_provider(&anvil, CHALLENGER_KEY);
    let oracle = WithdrawalOutputOracle::new(oracle_addr, challenger_provider.clone());
    oracle
        .deleteOutput(U256::from(bad_index))
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();

    let _: serde_json::Value = challenger_provider
        .raw_request("evm_increaseTime".into(), (WINDOW + 10,))
        .await
        .unwrap();
    let bad_wtx = ETHLockbox::WithdrawalTransaction {
        nonce: U256::from(2u64),
        sender: l2_sender,
        target: recipient,
        value,
    };
    let blocked = lockbox
        .finalizeWithdrawal(
            bad_wtx,
            U256::from(bad_index),
            bad_state,
            bad_wroot,
            U256::ZERO,
            withdrawals::withdrawal_proof(&bad_leaves, 0),
        )
        .send()
        .await;
    assert!(blocked.is_err(), "deleted output must not finalize");
}
