//! Machine-readable contract selection for Ansible settlement provisioning.
use std::process::Command;

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, address};
use kardamom_deployer::testkit::{AnvilRig, Funding};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_arg};

const OWNER: Address = address!("00000000000000000000000000000000DEAD0001");

#[tokio::test]
async fn json_addresses_selects_contract_and_chain() {
    let Some(rig) = AnvilRig::spawn(Anvil::new(), &[(OWNER, Funding::FundAndImpersonate)]).await
    else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let deployer = Deployer::new(rig.provider, OWNER);
    deployer.ensure_factory(OWNER).await.unwrap();
    let init_args = encode_address_arg(OWNER);
    let expected = deployer.predict_proxy_address(42, ContractId::KardamomL2Settlement, &init_args);
    let ops: Vec<_> = [42, 43]
        .into_iter()
        .map(|l2_chain_id| Op::Deploy {
            l2_chain_id,
            id: ContractId::KardamomL2Settlement,
            init_args: init_args.clone(),
        })
        .collect();
    deployer.apply(&ops, OWNER).await.unwrap();

    let query = |contract: &str| {
        let output = Command::new(env!("CARGO_BIN_EXE_kardamom-deploy"))
            .args([
                "--rpc-url",
                &rig.anvil.endpoint(),
                "--owner",
                &OWNER.to_string(),
                "addresses",
                "--l2-chain-id",
                "42",
                "--contract",
                contract,
                "--json",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()
    };
    let entries = query("KardamomL2Settlement");
    assert_eq!(entries.as_array().unwrap().len(), 1);
    assert_eq!(entries[0]["proxy"], expected.to_string());
    assert_eq!(entries[0]["l2_chain_id"], 42);
    assert_eq!(query("ETHLockbox"), serde_json::json!([]));
}
