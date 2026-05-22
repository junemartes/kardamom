//! End-to-end deposit flow: anvil L1 + kardamom factory + ETHLockbox proxy + kardamom Node.
//! Skips gracefully if `forge` artifacts are missing (e.g., no Foundry in CI).

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, Bytes, TxKind, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::sol;

use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_arg};
use kardamom_node::Node;
use kardamom_node::deposit::{DEPOSIT_TX_TYPE, DepositTx, alias_l1_address, source_hash};
use kardamom_node::genesis::Genesis;

sol! {
    #[sol(rpc)]
    contract ETHLockbox {
        function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable;
    }
}

/// Must match deployer/tests/factory_address_sync.rs and KardamomUUPSBase.FACTORY.
const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
const L2_CHAIN_ID: u64 = 1;

fn encode_deposit(dep: &DepositTx) -> Vec<u8> {
    let mut raw = vec![DEPOSIT_TX_TYPE];
    dep.rlp_encode(&mut raw);
    raw
}

#[tokio::test]
async fn deposit_e2e_anvil_to_node() {
    let anvil = match Anvil::new().try_spawn() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("SKIP: anvil unavailable: {e}");
            return;
        }
    };
    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(anvil.endpoint_url());

    // Inject the ERC-7955 factory runtime at its canonical address.
    let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
    let _: serde_json::Value = provider
        .raw_request("anvil_setCode".into(), (ERC7955_FACTORY, bytes_hex))
        .await
        .expect("anvil_setCode");

    // Fund and impersonate DEV_OWNER so transactions from it are accepted without a key.
    let _: serde_json::Value = provider
        .raw_request(
            "anvil_setBalance".into(),
            (DEV_OWNER, U256::from(1_000_000_000_000_000_000_000u128)),
        )
        .await
        .expect("anvil_setBalance");
    let _: serde_json::Value = provider
        .raw_request("anvil_impersonateAccount".into(), (DEV_OWNER,))
        .await
        .expect("anvil_impersonateAccount");

    let deployer = Deployer::new(provider.clone(), DEV_OWNER);

    // Bootstrap factory and deploy ETHLockbox via the new pipeline.
    let l2_minter = Address::from([0xBE; 20]);
    deployer
        .ensure_factory(DEV_OWNER)
        .await
        .expect("ensure_factory");
    deployer
        .apply(
            &[Op::Deploy {
                l2_chain_id: L2_CHAIN_ID,
                id: ContractId::EthLockbox,
                init_args: encode_address_arg(l2_minter),
            }],
            DEV_OWNER,
        )
        .await
        .expect("deploy ETHLockbox");

    let entries = deployer
        .addresses(DEV_OWNER, Some(L2_CHAIN_ID))
        .await
        .expect("addresses");
    let lockbox_addr = entries
        .iter()
        .find(|e| e.id == ContractId::EthLockbox.id())
        .expect("ETHLockbox registered")
        .proxy;

    // Deposit via the proxy.
    let lockbox = ETHLockbox::new(lockbox_addr, provider.clone());
    let target = address!("0000000000000000000000000000000000000022");
    let mint_amount: u128 = 1_000_000_000_000_000_000u128;

    let receipt = lockbox
        .depositETH(target, 100_000, Bytes::new())
        .value(U256::from(mint_amount))
        .from(DEV_OWNER)
        .send()
        .await
        .expect("send depositETH")
        .get_receipt()
        .await
        .expect("depositETH receipt");

    let log = receipt
        .inner
        .logs()
        .iter()
        .find(|l| l.address() == lockbox_addr)
        .expect("DepositInitiated log");
    let l1_block_hash = log.block_hash.expect("block hash");
    let l1_log_index: u64 = log.log_index.expect("log index");

    let dep = DepositTx {
        source_hash: source_hash(l1_block_hash, l1_log_index),
        from: alias_l1_address(DEV_OWNER),
        to: TxKind::Call(target),
        mint: mint_amount,
        value: U256::from(mint_amount),
        gas_limit: 100_000,
        is_system_transaction: false,
        input: Bytes::new(),
    };

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
