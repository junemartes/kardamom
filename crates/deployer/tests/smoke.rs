//! The transfer smoke against anvil, whose genesis accounts derive from
//! the same mnemonic as the accounts of the chain.
use std::num::NonZeroU64;
use std::process::Command;
use std::time::Duration;

use alloy_node_bindings::{Anvil, AnvilInstance};
use kardamom_deployer::smoke::TransferSmoke;

const ANVIL_CHAIN_ID: &str = "31337";

/// Run `kardamom-deploy smoke` against `anvil`. Returns the exit status
/// and the output.
fn smoke_cli(anvil: &AnvilInstance) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_kardamom-deploy"))
        .args([
            "smoke",
            "--rpc-url",
            &anvil.endpoint(),
            "--chain-id",
            ANVIL_CHAIN_ID,
            "--account",
            "3",
        ])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&output.stdout).into_owned()
        + &String::from_utf8_lossy(&output.stderr);
    (output.status.success(), text)
}

#[test]
fn a_transfer_with_a_receipt_passes_and_runs_again() {
    let Ok(anvil) = Anvil::new().try_spawn() else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let (first, output) = smoke_cli(&anvil);
    assert!(first, "{output}");
    assert!(output.contains("account #3 sent 0x"), "{output}");
    // The second smoke reads the next nonce of the account.
    let (second, output) = smoke_cli(&anvil);
    assert!(second, "{output}");
}

#[tokio::test]
async fn a_transfer_without_a_receipt_fails_at_the_budget() {
    let Ok(anvil) = Anvil::new().arg("--no-mining").try_spawn() else {
        eprintln!("SKIP: anvil unavailable");
        return;
    };
    let smoke = TransferSmoke {
        rpc_url: anvil.endpoint(),
        chain_id: NonZeroU64::new(31_337).unwrap(),
        account: 0,
        budget: Duration::from_secs(2),
    };
    let error = smoke.run().await.unwrap_err().to_string();
    assert!(error.starts_with("no receipt for 0x"), "{error}");
}
