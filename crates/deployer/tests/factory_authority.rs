//! The upgrade authority of an app implementation is the factory of the
//! deployer owner. These tests use an owner that is not the dev owner.

use alloy_primitives::{Address, Bytes, address};
use alloy_provider::RootProvider;

use kardamom_deployer::abi::ETHLockbox;
use kardamom_deployer::dev_keys::{DEV_OWNER, L2_MINTER};
use kardamom_deployer::testkit::{AnvilRig, Funding};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_pair};

/// An owner that is not [`DEV_OWNER`], so its factory has a different address.
const OTHER_OWNER: Address = address!("00000000000000000000000000000000BEEF0002");
const L2_CHAIN_ID: u64 = 42;

/// A deployer for [`OTHER_OWNER`] on a fresh anvil, with its factory and an
/// `ETHLockbox` on [`L2_CHAIN_ID`]. `None` when anvil is not installed.
struct Fixture {
    rig: AnvilRig,
    deployer: Deployer<RootProvider>,
    lockbox: ETHLockbox::ETHLockboxInstance<RootProvider>,
}

impl Fixture {
    async fn spawn() -> Option<Self> {
        let rig = AnvilRig::spawn(
            alloy_node_bindings::Anvil::new(),
            &[(OTHER_OWNER, Funding::FundAndImpersonate)],
        )
        .await?;
        let deployer = Deployer::new(rig.provider.clone(), OTHER_OWNER);
        deployer.ensure_factory(OTHER_OWNER).await.unwrap();
        deployer
            .apply(
                &[Op::Deploy {
                    l2_chain_id: L2_CHAIN_ID,
                    id: ContractId::EthLockbox,
                    init_args: encode_address_pair(L2_MINTER, Address::ZERO),
                }],
                OTHER_OWNER,
            )
            .await
            .expect("deploy ETHLockbox");
        let proxy = deployer.addresses(Some(L2_CHAIN_ID)).await.unwrap()[0].proxy;
        let lockbox = ETHLockbox::new(proxy, rig.provider.clone());
        Some(Self {
            rig,
            deployer,
            lockbox,
        })
    }

    async fn lockbox_factory(&self) -> Address {
        self.lockbox.factory().call().await.unwrap()
    }
}

#[tokio::test]
async fn other_owner_factory_deploys_and_upgrades() {
    let Some(fx) = Fixture::spawn().await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let factory = fx.deployer.factory_address();
    let dev_factory = Deployer::new(fx.rig.provider.clone(), DEV_OWNER).factory_address();
    assert_ne!(factory, dev_factory);
    assert_eq!(fx.lockbox_factory().await, factory);

    fx.deployer
        .apply(
            &[Op::Upgrade {
                l2_chain_id: L2_CHAIN_ID,
                id: ContractId::EthLockbox,
                new_version: 2,
                init_args: Bytes::new(),
            }],
            OTHER_OWNER,
        )
        .await
        .expect("upgrade ETHLockbox through the factory of the owner");

    let entry = &fx.deployer.addresses(Some(L2_CHAIN_ID)).await.unwrap()[0];
    assert_eq!(entry.version, 2);
    assert_eq!(fx.lockbox_factory().await, factory);
    assert_eq!(fx.lockbox.l2Minter().call().await.unwrap(), L2_MINTER);
}

#[tokio::test]
async fn other_owner_can_initiate_upgrade() {
    let Some(fx) = Fixture::spawn().await else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let receipt = fx
        .lockbox
        .initiateUpgrade(alloy_primitives::U256::from(1), 0)
        .from(OTHER_OWNER)
        .send()
        .await
        .expect("initiateUpgrade from the factory owner")
        .get_receipt()
        .await
        .unwrap();
    assert!(receipt.status());
    assert_eq!(fx.lockbox.upgradeNonce().call().await.unwrap(), 1u64);
}
