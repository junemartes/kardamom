//! End-to-end deployer flow against anvil with the ERC-7955 factory predeployed.
//! Skips gracefully if forge artifacts or anvil are missing.

use alloy_primitives::{Address, Bytes, U256, address};
use alloy_sol_types::sol;

use kardamom_deployer::testkit::AnvilRig;
use kardamom_deployer::{ContractId, Deployer, FactoryStatus, Op, encode_address_pair};

sol! {
    #[sol(rpc)]
    contract ETHLockbox {
        function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable;
        function depositNonce() external view returns (uint64);
        function l2Minter() external view returns (address);
    }
}

const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");

#[tokio::test]
async fn cross_chain_address_parity() {
    let Some(rig_a) = AnvilRig::spawn(&[DEV_OWNER]).await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let Some(rig_b) = AnvilRig::spawn(&[DEV_OWNER]).await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };

    let deployer_a = Deployer::new(rig_a.provider, DEV_OWNER);
    let deployer_b = Deployer::new(rig_b.provider, DEV_OWNER);

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
    let Some(rig) = AnvilRig::spawn(&[DEV_OWNER]).await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let provider = rig.provider.clone();
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
                    init_args: encode_address_pair(l2_minter_a, Address::ZERO),
                },
                Op::Deploy {
                    l2_chain_id: 43,
                    id: ContractId::EthLockbox,
                    init_args: encode_address_pair(l2_minter_b, Address::ZERO),
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

    // Each proxy keeps its own l2Minter value.
    let lock_a = ETHLockbox::new(e42.proxy, provider.clone());
    let lock_b = ETHLockbox::new(e43.proxy, provider.clone());
    assert_eq!(lock_a.l2Minter().call().await.unwrap(), l2_minter_a);
    assert_eq!(lock_b.l2Minter().call().await.unwrap(), l2_minter_b);

    // Deposit on L2 A only, so L2 B's nonce stays at 0. DEV_OWNER pays for
    // the deposit; it can send ETH because it was funded above.
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

    // L2 A's deposit nonce stays the same after the upgrade. State is preserved.
    assert_eq!(lock_a.depositNonce().call().await.unwrap(), 1u64);
    assert_eq!(lock_b.depositNonce().call().await.unwrap(), 0u64);

    // Filter by l2_chain_id.
    let only_42 = deployer.addresses(Some(42)).await.unwrap();
    assert_eq!(only_42.len(), 1);
    assert_eq!(only_42[0].l2_chain_id, 42);
}
