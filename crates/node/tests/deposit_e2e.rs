//! End-to-end deposit flow: anvil L1 + ETHLockbox + kardamom Node.
//! Skips gracefully if `forge` artifacts are missing (e.g., no Foundry in CI).

use std::path::PathBuf;

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::sol;

use kardamom_node::Node;
use kardamom_node::deposit::{DEPOSIT_TX_TYPE, DepositTx, alias_l1_address, source_hash};
use kardamom_node::genesis::Genesis;

// ABI-only bindings — no bytecode embedded here so we can skip gracefully at
// runtime when the forge artifact is absent.
sol! {
    #[sol(rpc)]
    #[derive(Debug)]
    contract ETHLockbox {
        error ZeroDeposit();

        event DepositInitiated(
            uint64 indexed depositNonce,
            address indexed from,
            address indexed to,
            uint256 mint,
            uint64 gasLimit,
            bytes data
        );

        constructor(address _l2Minter);
        function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable;
    }
}

fn artifact_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("contracts")
        .join("out")
        .join("ETHLockbox.sol")
        .join("ETHLockbox.json")
}

/// Read the creation bytecode from the forge artifact JSON.
/// Returns `None` if the file is missing or malformed.
fn read_bytecode() -> Option<Bytes> {
    let path = artifact_path();
    let contents = std::fs::read_to_string(&path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&contents).ok()?;
    let hex_str = v["bytecode"]["object"].as_str()?;
    let hex_str = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes = alloy_primitives::hex::decode(hex_str).ok()?;
    Some(Bytes::from(bytes))
}

fn encode_deposit(dep: &DepositTx) -> Vec<u8> {
    let mut raw = vec![DEPOSIT_TX_TYPE];
    dep.rlp_encode(&mut raw);
    raw
}

#[tokio::test]
async fn deposit_e2e_anvil_to_node() {
    // Read bytecode first — if missing, skip gracefully.
    let init_code = match read_bytecode() {
        Some(b) => b,
        None => {
            eprintln!(
                "SKIP: {} not present. Install Foundry (`foundryup`) and run `forge build` in contracts/.",
                artifact_path().display()
            );
            return;
        }
    };

    // 1. Start anvil.
    let anvil = match Anvil::new().try_spawn() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("SKIP: anvil could not start: {e}");
            return;
        }
    };

    let wallet = anvil.wallet().expect("anvil wallet");
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(anvil.endpoint_url());

    // 2. Deploy ETHLockbox by sending the constructor calldata manually.
    //    ABI-encode the constructor argument (address _l2Minter).
    let l2_minter = Address::from([0xBEu8; 20]);
    // ETHLockbox constructor(address): ABI-encode address as 32-byte padded word.
    let mut constructor_calldata = init_code.to_vec();
    constructor_calldata.extend_from_slice(&[0u8; 12]); // left-pad address to 32 bytes
    constructor_calldata.extend_from_slice(l2_minter.as_slice());

    use alloy_network::{ReceiptResponse, TransactionBuilder};
    use alloy_rpc_types_eth::TransactionRequest;

    let deploy_tx =
        TransactionRequest::default().with_deploy_code(Bytes::from(constructor_calldata));

    let pending = provider
        .send_transaction(deploy_tx)
        .await
        .expect("send deploy");
    let deploy_receipt = pending.get_receipt().await.expect("deploy receipt");
    assert!(deploy_receipt.status(), "deploy failed");
    let lockbox_addr = deploy_receipt
        .contract_address()
        .expect("contract address in receipt");

    // Bind the deployed contract.
    let lockbox = ETHLockbox::new(lockbox_addr, &provider);

    // 3. Call depositETH from the funded anvil account 0.
    let caller: Address = anvil.addresses()[0];
    let target = Address::from([0x22u8; 20]);
    let mint_amount: u128 = 1_000_000_000_000_000_000u128; // 1 ETH

    let receipt = lockbox
        .depositETH(target, 100_000, Bytes::new())
        .value(U256::from(mint_amount))
        .from(caller)
        .send()
        .await
        .expect("send depositETH")
        .get_receipt()
        .await
        .expect("depositETH receipt");

    // 4. Pull DepositInitiated log.
    let log = receipt
        .inner
        .logs()
        .iter()
        .find(|l| l.address() == lockbox_addr)
        .expect("DepositInitiated log");
    let l1_block_hash = log.block_hash.expect("block hash");
    let l1_log_index: u64 = log.log_index.expect("log index");

    // 5. Construct the deposit envelope in Rust.
    let dep = DepositTx {
        source_hash: source_hash(l1_block_hash, l1_log_index),
        from: alias_l1_address(caller),
        to: TxKind::Call(target),
        mint: mint_amount,
        value: U256::from(mint_amount),
        gas_limit: 100_000,
        is_system_transaction: false,
        input: Bytes::new(),
    };

    // 6. Encode 2718 (0x7E || RLP) and submit to a fresh kardamom node.
    let raw = encode_deposit(&dep);

    let node = Node::new(&Genesis {
        chain_id: 1,
        alloc: Vec::new(),
    });
    let tx_hash = node
        .submit_deposit_transaction(Bytes::from(raw))
        .await
        .expect("submit ok");

    assert_eq!(node.balance(target).await, U256::from(mint_amount));
    assert!(node.receipt(tx_hash).await.is_some());
}
