//! This is a real Uniswap workload, without a router. It uses the real
//! v2-core bytecode, factory and pairs, driven at the pair level:
//! `token.transfer(pair, in)`, then `pair.swap(out0, out1, to, "")`.
//! Swap outputs are computed offline against deterministically tracked
//! reserves, with the same x*y=k plus 0.3% fee integer math the
//! contract runs. So every signed transaction succeeds, and every
//! pair's on-chain reserves match the generator's model exactly.
//!
//! Pair addresses are CREATE2-deterministic. The salt is
//! `keccak(token0 ++ token1)`, and the init-code hash comes from the
//! vendored artifact. So the whole stream signs offline before
//! anything executes.

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256, keccak256};

use crate::signers::DerivedSigner;
use kardamom_types::TxEnvelope;

const GAS_PRICE: u128 = 1_000_000_000;
const CALL_GAS: u64 = 300_000;
const CREATE_GAS: u64 = 4_000_000;

fn selector(sig: &str) -> [u8; 4] {
    keccak256(sig.as_bytes())[..4].try_into().unwrap()
}

fn call(sig: &str, words: &[U256]) -> Bytes {
    let mut d = Vec::with_capacity(4 + 32 * words.len());
    d.extend_from_slice(&selector(sig));
    for w in words {
        d.extend_from_slice(&w.to_be_bytes::<32>());
    }
    Bytes::from(d)
}

fn addr_word(a: Address) -> U256 {
    U256::from_be_slice(a.as_slice())
}

/// Load creation bytecode from a foundry artifact.
fn creation_code(path: &str) -> anyhow::Result<Vec<u8>> {
    let j: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    let hexs = j["bytecode"]["object"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("no bytecode.object in {path}"))?;
    Ok(alloy_primitives::hex::decode(
        hexs.trim_start_matches("0x"),
    )?)
}

pub struct UniswapWorkload {
    /// The setup blocks: deploys, pair creation, liquidity, and
    /// sender funding.
    pub setup_blocks: Vec<Vec<TxEnvelope>>,
    /// The flow blocks: swaps and ERC20 transfers.
    pub flow_blocks: Vec<Vec<TxEnvelope>>,
}

struct PairState {
    addr: Address,
    token0: Address,
    token1: Address,
    reserve0: U256,
    reserve1: U256,
}

fn get_amount_out(amount_in: U256, reserve_in: U256, reserve_out: U256) -> U256 {
    let with_fee = amount_in * U256::from(997u64);
    (with_fee * reserve_out) / (reserve_in * U256::from(1000u64) + with_fee)
}

struct Signer<'a> {
    s: &'a DerivedSigner,
    nonce: u64,
}

impl Signer<'_> {
    fn sign(&mut self, chain_id: u64, to: TxKind, gas: u64, input: Bytes) -> TxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(chain_id),
            nonce: self.nonce,
            gas_price: GAS_PRICE,
            gas_limit: gas,
            to,
            value: U256::ZERO,
            input,
        };
        self.nonce += 1;
        let sig = self.s.signer.sign_transaction_sync(&mut tx).unwrap();
        let env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw = env.encoded_2718();
        let tx_hash = keccak256(&raw);
        TxEnvelope {
            correlation_id: 0,
            raw_tx: raw.into(),
            sender: self.s.signer.address(),
            tx_hash,
        }
    }
}

/// Generate the workload.
///
/// - `pairs`: the number of isolated pools, 2 tokens each. This is the
///   contention setting: senders' home pairs spread out in rotation.
/// - `swap_share_pct`: the percentage of flow operations that are
///   swaps. The rest are plain ERC20 transfers between senders, the
///   parallel-friendly class.
/// - `cross_pct`: the percentage of swaps that hit a pair other than
///   the sender's home pair, a cross-domain join.
#[allow(clippy::too_many_arguments)]
pub fn generate(
    repo_root: &str,
    signers: &[DerivedSigner],
    chain_id: u64,
    pairs: usize,
    flow_blocks: usize,
    txs_per_block: usize,
    swap_share_pct: u64,
    cross_pct: u64,
) -> anyhow::Result<UniswapWorkload> {
    anyhow::ensure!(pairs >= 1 && signers.len() >= 2);
    let factory_code = creation_code(&format!(
        "{repo_root}/uniswap-v2-contracts/core/out/UniswapV2Factory.sol/UniswapV2Factory.json"
    ))?;
    let pair_code = creation_code(&format!(
        "{repo_root}/uniswap-v2-contracts/core/out/UniswapV2Pair.sol/UniswapV2Pair.json"
    ))?;
    let erc20_code = creation_code(&format!(
        "{repo_root}/bench-contracts/out/MintERC20.sol/MintERC20.json"
    ))?;
    let pair_init_hash = keccak256(&pair_code);

    let deployer = &signers[0];
    let dep_addr = deployer.signer.address();
    let mut dep = Signer {
        s: deployer,
        nonce: 0,
    };
    let mut setup: Vec<TxEnvelope> = Vec::new();

    // Tokens: 2 per pair. Each address is CREATE(deployer, nonce).
    let mut tokens: Vec<Address> = Vec::new();
    for _ in 0..pairs * 2 {
        let addr = dep_addr.create(dep.nonce);
        setup.push(dep.sign(
            chain_id,
            TxKind::Create,
            CREATE_GAS,
            erc20_code.clone().into(),
        ));
        tokens.push(addr);
    }
    // Factory: the constructor takes `address feeToSetter`.
    let factory = dep_addr.create(dep.nonce);
    let mut fac_init = factory_code.clone();
    fac_init.extend_from_slice(&addr_word(dep_addr).to_be_bytes::<32>());
    setup.push(dep.sign(chain_id, TxKind::Create, CREATE_GAS, fac_init.into()));

    // Pairs are created through createPair. The address is CREATE2-deterministic.
    let mut pair_states: Vec<PairState> = Vec::new();
    for p in 0..pairs {
        let (a, b) = (tokens[p * 2], tokens[p * 2 + 1]);
        let (t0, t1) = if a < b { (a, b) } else { (b, a) };
        let mut salt_buf = [0u8; 40];
        salt_buf[..20].copy_from_slice(t0.as_slice());
        salt_buf[20..].copy_from_slice(t1.as_slice());
        let salt = keccak256(salt_buf);
        let mut c2 = Vec::with_capacity(85);
        c2.push(0xff);
        c2.extend_from_slice(factory.as_slice());
        c2.extend_from_slice(salt.as_slice());
        c2.extend_from_slice(pair_init_hash.as_slice());
        let pair_addr = Address::from_slice(&keccak256(c2)[12..]);
        setup.push(dep.sign(
            chain_id,
            TxKind::Call(factory),
            CREATE_GAS,
            call(
                "createPair(address,address)",
                &[addr_word(t0), addr_word(t1)],
            ),
        ));
        pair_states.push(PairState {
            addr: pair_addr,
            token0: t0,
            token1: t1,
            reserve0: U256::ZERO,
            reserve1: U256::ZERO,
        });
    }

    // Liquidity: the deployer mints to itself, transfers to each pair,
    // then mints LP tokens.
    let liq = U256::from(10u128.pow(24));
    for ps in &mut pair_states {
        for t in [ps.token0, ps.token1] {
            setup.push(dep.sign(
                chain_id,
                TxKind::Call(t),
                CALL_GAS,
                call("mint(address,uint256)", &[addr_word(dep_addr), liq]),
            ));
            setup.push(dep.sign(
                chain_id,
                TxKind::Call(t),
                CALL_GAS,
                call("transfer(address,uint256)", &[addr_word(ps.addr), liq]),
            ));
        }
        setup.push(dep.sign(
            chain_id,
            TxKind::Call(ps.addr),
            CALL_GAS,
            call("mint(address)", &[addr_word(dep_addr)]),
        ));
        ps.reserve0 = liq;
        ps.reserve1 = liq;
    }

    // Sender funding: every sender mints for itself every token it may use.
    let fund = U256::from(10u128.pow(23));
    let mut senders: Vec<Signer> = signers[1..]
        .iter()
        .map(|s| Signer { s, nonce: 0 })
        .collect();
    for sn in senders.iter_mut() {
        let me = sn.s.signer.address();
        for t in &tokens {
            setup.push(sn.sign(
                chain_id,
                TxKind::Call(*t),
                CALL_GAS,
                call("mint(address,uint256)", &[addr_word(me), fund]),
            ));
        }
    }

    // Slice setup into blocks of txs_per_block, for realistic shapes.
    let setup_blocks: Vec<Vec<TxEnvelope>> = setup
        .chunks(txs_per_block.max(1))
        .map(|c| c.to_vec())
        .collect();

    // Flows.
    let mut flows: Vec<Vec<TxEnvelope>> = Vec::with_capacity(flow_blocks);
    let mut h: u64 = 0x243F_6A88_85A3_08D3; // This is the deterministic mixer state.
    let mut mix = |x: u64| {
        h ^= x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= h >> 29;
        h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        h ^= h >> 32;
        h
    };
    let n_send = senders.len();
    for b in 0..flow_blocks {
        let mut block: Vec<TxEnvelope> = Vec::with_capacity(txs_per_block);
        let mut op_i = 0usize;
        while block.len() < txs_per_block {
            let si = (b * 131 + op_i * 7) % n_send;
            let r = mix((b as u64) << 32 | op_i as u64);
            let me = senders[si].s.signer.address();
            let room_for_swap = txs_per_block - block.len() >= 2;
            if r % 100 < swap_share_pct && room_for_swap {
                // Swap on the home pair, or a cross pair: 2 transactions.
                let home = si % pair_states.len();
                let pi = if r / 100 % 100 < cross_pct {
                    (home + 1 + (r as usize / 10_000) % pair_states.len().max(1))
                        % pair_states.len()
                } else {
                    home
                };
                let zero_for_one = r & 1 == 0;
                let amount_in = U256::from(10u128.pow(18) + (r % 1000) as u128 * 10u128.pow(15));
                let ps = &mut pair_states[pi];
                let (tin, out0, out1) = if zero_for_one {
                    let out = get_amount_out(amount_in, ps.reserve0, ps.reserve1);
                    ps.reserve0 += amount_in;
                    ps.reserve1 -= out;
                    (ps.token0, U256::ZERO, out)
                } else {
                    let out = get_amount_out(amount_in, ps.reserve1, ps.reserve0);
                    ps.reserve1 += amount_in;
                    ps.reserve0 -= out;
                    (ps.token1, out, U256::ZERO)
                };
                let pair_addr = ps.addr;
                block.push(senders[si].sign(
                    chain_id,
                    TxKind::Call(tin),
                    CALL_GAS,
                    call(
                        "transfer(address,uint256)",
                        &[addr_word(pair_addr), amount_in],
                    ),
                ));
                block.push(senders[si].sign(
                    chain_id,
                    TxKind::Call(pair_addr),
                    CALL_GAS,
                    call(
                        "swap(uint256,uint256,address,bytes)",
                        &[out0, out1, addr_word(me), U256::from(0x80u64), U256::ZERO],
                    ),
                ));
            } else {
                // A plain ERC20 transfer to another sender: the parallel class.
                let tj = (si + 1 + (r as usize >> 8) % (n_send - 1)) % n_send;
                let dst = senders[tj].s.signer.address();
                let t = tokens[(r as usize >> 16) % tokens.len()];
                let amt = U256::from(10u128.pow(15) + (r % 997) as u128);
                block.push(senders[si].sign(
                    chain_id,
                    TxKind::Call(t),
                    CALL_GAS,
                    call("transfer(address,uint256)", &[addr_word(dst), amt]),
                ));
            }
            op_i += 1;
        }
        flows.push(block);
    }

    Ok(UniswapWorkload {
        setup_blocks,
        flow_blocks: flows,
    })
}
