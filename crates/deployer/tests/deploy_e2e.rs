//! End-to-end deployer flow against anvil with the SingletonFactory predeployed.
//! Skips gracefully if forge artifacts or anvil are missing.

use alloy_node_bindings::Anvil;
use alloy_primitives::{Bytes, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::sol;

use kardamom_deployer::addresses::SINGLETON_FACTORY;
use kardamom_deployer::artifacts::{creation_bytecode, default_contracts_root};
use kardamom_deployer::{ContractId, Deployer, FactoryStatus, Op, encode_address_arg};

sol! {
    #[sol(rpc)]
    contract ETHLockbox {
        function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable;
        function depositNonce() external view returns (uint64);
        function l2Minter() external view returns (address);
    }
}

const SINGLETON_RUNTIME_HEX: &str =
    "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe03601600081602082378035828234f58015156039578182fd5b8082525050506014600cf3";

#[tokio::test]
async fn full_deploy_and_upgrade_flow() {
    let root = default_contracts_root();
    if creation_bytecode(&root, "KardamomFactoryV1").is_err() {
        eprintln!("SKIP: forge artifacts missing; run forge build in contracts/");
        return;
    }

    let anvil = match Anvil::new().try_spawn() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("SKIP: anvil unavailable: {e}");
            return;
        }
    };

    let wallet = anvil.wallet().expect("anvil wallet");
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(anvil.endpoint_url());
    let operator = anvil.addresses()[0];

    // Inject the Arachnid singleton runtime at its canonical address.
    let bytes_hex = format!("0x{SINGLETON_RUNTIME_HEX}");
    let _: serde_json::Value = provider
        .raw_request("anvil_setCode".into(), (SINGLETON_FACTORY, bytes_hex))
        .await
        .expect("anvil_setCode");

    let deployer = Deployer::new(provider.clone(), operator);

    // Cold bootstrap.
    let status1 = deployer.ensure_factory().await.expect("ensure_factory cold");
    assert!(matches!(status1, FactoryStatus::Deployed));

    // Warm — idempotent.
    let status2 = deployer.ensure_factory().await.expect("ensure_factory warm");
    assert!(matches!(status2, FactoryStatus::AlreadyDeployed));

    // Deploy ETHLockbox.
    let l2_minter = address!("00000000000000000000000000000000000000BE");
    deployer
        .apply(&[Op::Deploy {
            id: ContractId::EthLockbox,
            init_args: encode_address_arg(l2_minter),
        }])
        .await
        .expect("deploy ETHLockbox");

    let entries = deployer.addresses().await.expect("addresses");
    assert_eq!(entries.len(), 1);
    let lockbox_entry = entries[0].clone();
    assert_eq!(lockbox_entry.id, ContractId::EthLockbox.id());
    assert_eq!(lockbox_entry.version, 1);

    let verify = deployer.verify().await.expect("verify");
    assert!(verify.mismatches.is_empty(), "verify mismatches: {:?}", verify.mismatches);

    // Exercise depositETH on the proxy.
    let lockbox = ETHLockbox::new(lockbox_entry.proxy, provider.clone());
    assert_eq!(lockbox.l2Minter().call().await.expect("l2Minter call"), l2_minter);

    let target = address!("0000000000000000000000000000000000000022");
    let _ = lockbox
        .depositETH(target, 100_000, Bytes::new())
        .value(U256::from(1_000_000_000_000_000_000u128))
        .from(operator)
        .send()
        .await
        .expect("send depositETH")
        .get_receipt()
        .await
        .expect("depositETH receipt");
    assert_eq!(
        lockbox.depositNonce().call().await.expect("depositNonce after deposit"),
        1u64
    );

    // Upgrade to version 2 (same source — but different impl_salt → different CREATE2 address).
    deployer
        .apply(&[Op::Upgrade {
            id: ContractId::EthLockbox,
            new_version: 2,
            init_args: Bytes::new(),
        }])
        .await
        .expect("upgrade");

    let entries2 = deployer.addresses().await.expect("addresses after upgrade");
    assert_eq!(entries2.len(), 1);
    assert_eq!(entries2[0].version, 2);
    assert_ne!(entries2[0].current_impl, lockbox_entry.current_impl);
    assert_eq!(entries2[0].proxy, lockbox_entry.proxy);

    // State persists across upgrade.
    assert_eq!(
        lockbox.depositNonce().call().await.expect("depositNonce after upgrade"),
        1u64
    );

    let verify2 = deployer.verify().await.expect("verify after upgrade");
    assert!(verify2.mismatches.is_empty());
}
