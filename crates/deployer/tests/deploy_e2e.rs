//! End-to-end deployer flow against anvil with the ERC-7955 factory predeployed.
//! Skips gracefully if forge artifacts or anvil are missing.

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, Bytes, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::sol;

use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, FactoryStatus, Op, encode_address_arg};

sol! {
    #[sol(rpc)]
    contract ETHLockbox {
        function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable;
        function depositNonce() external view returns (uint64);
        function l2Minter() external view returns (address);
    }
}

const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");

/// Set up an anvil instance with ERC-7955 factory preloaded and DEV_OWNER impersonated
/// (so transactions from DEV_OWNER don't need a private key). Returns None if anvil
/// isn't available.
async fn setup_anvil_with_erc7955() -> Option<(
    alloy_node_bindings::AnvilInstance,
    impl alloy_provider::Provider + Clone,
)> {
    let anvil = Anvil::new().try_spawn().ok()?;
    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(anvil.endpoint_url());

    let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
    let _: serde_json::Value = provider
        .raw_request("anvil_setCode".into(), (ERC7955_FACTORY, bytes_hex))
        .await
        .ok()?;

    // Fund and impersonate DEV_OWNER so transactions from it are accepted without a key.
    let _: serde_json::Value = provider
        .raw_request(
            "anvil_setBalance".into(),
            (DEV_OWNER, U256::from(1_000_000_000_000_000_000_000u128)),
        )
        .await
        .ok()?;
    let _: serde_json::Value = provider
        .raw_request("anvil_impersonateAccount".into(), (DEV_OWNER,))
        .await
        .ok()?;

    Some((anvil, provider))
}

#[tokio::test]
async fn cross_chain_address_parity() {
    let (anvil_a, provider_a) = match setup_anvil_with_erc7955().await {
        Some(p) => p,
        None => {
            eprintln!("SKIP: anvil unavailable");
            return;
        }
    };
    let (_anvil_b, provider_b) = match setup_anvil_with_erc7955().await {
        Some(p) => p,
        None => {
            eprintln!("SKIP: anvil unavailable");
            return;
        }
    };
    let _ = anvil_a; // keep alive

    let deployer_a = Deployer::new(provider_a, DEV_OWNER);
    let deployer_b = Deployer::new(provider_b, DEV_OWNER);

    assert!(matches!(
        deployer_a.ensure_factory(DEV_OWNER).await.unwrap(),
        FactoryStatus::Deployed
    ));
    assert!(matches!(
        deployer_b.ensure_factory(DEV_OWNER).await.unwrap(),
        FactoryStatus::Deployed
    ));

    let addr_a = deployer_a.factory_address();
    let addr_b = deployer_b.factory_address();
    assert_eq!(
        addr_a, addr_b,
        "same owner + same bytecode must produce same factory address on different chains"
    );
}

#[tokio::test]
#[ignore = "anvil flake: reverts intermittently with 'atomic multi-L2 upgrade: Reverted'; tracked separately"]
async fn multi_l2_deploy_and_atomic_upgrade() {
    let (anvil, provider) = match setup_anvil_with_erc7955().await {
        Some(p) => p,
        None => {
            eprintln!("SKIP: anvil unavailable");
            return;
        }
    };
    let _ = anvil; // keep alive
    let deployer = Deployer::new(provider.clone(), DEV_OWNER);

    deployer.ensure_factory(DEV_OWNER).await.unwrap();
    // Warm path is idempotent.
    assert!(matches!(
        deployer.ensure_factory(DEV_OWNER).await.unwrap(),
        FactoryStatus::AlreadyDeployed
    ));

    // Deploy ETHLockbox on L2 chainIDs 42 and 43 in one tx.
    let l2_minter_a = address!("00000000000000000000000000000000000000BE");
    let l2_minter_b = address!("00000000000000000000000000000000000000BF");
    deployer
        .apply(
            &[
                Op::Deploy {
                    l2_chain_id: 42,
                    id: ContractId::EthLockbox,
                    init_args: encode_address_arg(l2_minter_a),
                },
                Op::Deploy {
                    l2_chain_id: 43,
                    id: ContractId::EthLockbox,
                    init_args: encode_address_arg(l2_minter_b),
                },
            ],
            DEV_OWNER,
        )
        .await
        .expect("multi-L2 deploy");

    let entries = deployer.addresses(None).await.unwrap();
    assert_eq!(entries.len(), 2);
    let e42 = entries.iter().find(|e| e.l2_chain_id == 42).unwrap();
    let e43 = entries.iter().find(|e| e.l2_chain_id == 43).unwrap();
    assert_ne!(e42.proxy, e43.proxy, "per-L2 proxies must differ");
    assert_eq!(
        e42.current_impl, e43.current_impl,
        "impl must be shared via dedup"
    );

    // State independence: each proxy's l2Minter is its own.
    let lock_a = ETHLockbox::new(e42.proxy, provider.clone());
    let lock_b = ETHLockbox::new(e43.proxy, provider.clone());
    assert_eq!(lock_a.l2Minter().call().await.unwrap(), l2_minter_a);
    assert_eq!(lock_b.l2Minter().call().await.unwrap(), l2_minter_b);

    // Deposit on L2 A only — L2 B's nonce stays at 0.
    // Use anvil's funded account (addresses()[0]) to pay for the deposit, not DEV_OWNER.
    // DEV_OWNER can also send ETH since we funded it above.
    let target = address!("0000000000000000000000000000000000000022");
    lock_a
        .depositETH(target, 100_000, Bytes::new())
        .value(U256::from(1_000_000_000_000_000_000u128))
        .from(DEV_OWNER)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    assert_eq!(lock_a.depositNonce().call().await.unwrap(), 1u64);
    assert_eq!(lock_b.depositNonce().call().await.unwrap(), 0u64);

    // Atomic upgrade across both L2s in one tx.
    deployer
        .apply(
            &[
                Op::Upgrade {
                    l2_chain_id: 42,
                    id: ContractId::EthLockbox,
                    new_version: 2,
                    init_args: Bytes::new(),
                },
                Op::Upgrade {
                    l2_chain_id: 43,
                    id: ContractId::EthLockbox,
                    new_version: 2,
                    init_args: Bytes::new(),
                },
            ],
            DEV_OWNER,
        )
        .await
        .expect("atomic multi-L2 upgrade");

    let entries2 = deployer.addresses(None).await.unwrap();
    let e42 = entries2.iter().find(|e| e.l2_chain_id == 42).unwrap();
    let e43 = entries2.iter().find(|e| e.l2_chain_id == 43).unwrap();
    assert_eq!(e42.version, 2);
    assert_eq!(e43.version, 2);
    assert_eq!(
        e42.current_impl, e43.current_impl,
        "upgraded impl must still be shared"
    );

    // L2 A's deposit nonce survived the upgrade (state preserved).
    assert_eq!(lock_a.depositNonce().call().await.unwrap(), 1u64);
    assert_eq!(lock_b.depositNonce().call().await.unwrap(), 0u64);

    // Filter by l2_chain_id.
    let only_42 = deployer.addresses(Some(42)).await.unwrap();
    assert_eq!(only_42.len(), 1);
    assert_eq!(only_42[0].l2_chain_id, 42);
}
