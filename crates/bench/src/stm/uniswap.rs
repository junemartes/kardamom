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

use std::num::NonZeroUsize;

use alloy_consensus::TxLegacy;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256, keccak256};

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

/// The constant-product swap formula, with a 0.3% fee. `U256`'s `*`/`+`
/// operators wrap on overflow (never panic; `ruint` backs them with
/// `wrapping_mul`/`wrapping_add`), so this is a proven-bound plain
/// computation, not a checked one: every synthetic reserve here starts
/// at `10u128.pow(21)`-scale and grows by `10u128.pow(18)`-scale
/// increments over a CLI-bounded (`--blocks`) number of simulated
/// swaps, staying many orders of magnitude under `U256::MAX` (`~2^256`).
fn get_amount_out(amount_in: U256, reserve_in: U256, reserve_out: U256) -> U256 {
    let with_fee = amount_in * U256::from(997u64);
    (with_fee * reserve_out) / (reserve_in * U256::from(1000u64) + with_fee)
}

struct Signer<'a> {
    s: &'a DerivedSigner,
    nonce: u64,
}

impl Signer<'_> {
    /// Sign, and advance the nonce. Panics only if the underlying k256
    /// signer fails, which does not happen for a valid `PrivateKeySigner`.
    fn sign(&mut self, chain_id: u64, to: TxKind, gas: u64, input: Bytes) -> TxEnvelope {
        let tx = TxLegacy {
            chain_id: Some(chain_id),
            nonce: self.nonce,
            gas_price: GAS_PRICE,
            gas_limit: gas,
            to,
            value: U256::ZERO,
            input,
        };
        self.nonce += 1;
        self.s
            .sign_envelope(tx)
            .expect("k256 signing does not fail")
    }

    /// One `mint(address,uint256)` call per token, funding this sender
    /// with `fund` units of each.
    fn fund_mints(&mut self, chain_id: u64, tokens: &[Address], fund: U256) -> Vec<TxEnvelope> {
        let me = self.s.signer.address();
        tokens
            .iter()
            .map(|t| {
                self.sign(
                    chain_id,
                    TxKind::Call(*t),
                    CALL_GAS,
                    call("mint(address,uint256)", &[addr_word(me), fund]),
                )
            })
            .collect()
    }
}

/// The three vendored contract artifacts this workload deploys.
#[allow(
    clippy::struct_field_names,
    reason = "the shared _code suffix names what each field holds; dropping it would make the three fields read as unrelated names"
)]
struct Artifacts {
    factory_code: Vec<u8>,
    pair_code: Vec<u8>,
    erc20_code: Vec<u8>,
}

/// Load the three artifacts from `repo_root`.
fn load_artifacts(repo_root: &str) -> anyhow::Result<Artifacts> {
    Ok(Artifacts {
        factory_code: creation_code(&format!(
            "{repo_root}/uniswap-v2-contracts/core/out/UniswapV2Factory.sol/UniswapV2Factory.json"
        ))?,
        pair_code: creation_code(&format!(
            "{repo_root}/uniswap-v2-contracts/core/out/UniswapV2Pair.sol/UniswapV2Pair.json"
        ))?,
        erc20_code: creation_code(&format!(
            "{repo_root}/bench-contracts/out/MintERC20.sol/MintERC20.json"
        ))?,
    })
}

/// The result of `deploy_tokens_and_factory`.
struct Deployed {
    setup: Vec<TxEnvelope>,
    tokens: Vec<Address>,
    factory: Address,
}

/// Deploy one ERC20 token at `dep`'s current CREATE nonce. Appends the
/// deploy transaction to `setup` and the predicted token address to
/// `tokens`.
fn deploy_one_token(
    dep: &mut Signer<'_>,
    chain_id: u64,
    artifacts: &Artifacts,
    setup: &mut Vec<TxEnvelope>,
    tokens: &mut Vec<Address>,
) {
    let addr = dep.s.signer.address().create(dep.nonce);
    setup.push(dep.sign(
        chain_id,
        TxKind::Create,
        CREATE_GAS,
        artifacts.erc20_code.clone().into(),
    ));
    tokens.push(addr);
}

/// Deploy `pairs * 2` ERC20 tokens, then the factory. Each token
/// address is `CREATE(deployer, nonce)`. The factory constructor takes
/// `address feeToSetter`.
fn deploy_tokens_and_factory(
    dep: &mut Signer<'_>,
    chain_id: u64,
    pairs: usize,
    artifacts: &Artifacts,
) -> Deployed {
    let dep_addr = dep.s.signer.address();
    let mut setup = Vec::new();
    let mut tokens: Vec<Address> = Vec::new();
    for _ in 0..pairs.saturating_mul(2) {
        deploy_one_token(dep, chain_id, artifacts, &mut setup, &mut tokens);
    }
    let factory = dep_addr.create(dep.nonce);
    let mut fac_init = artifacts.factory_code.clone();
    fac_init.extend_from_slice(&addr_word(dep_addr).to_be_bytes::<32>());
    setup.push(dep.sign(chain_id, TxKind::Create, CREATE_GAS, fac_init.into()));
    Deployed {
        setup,
        tokens,
        factory,
    }
}

/// The result of `create_pairs_and_liquidity`.
struct PairsSetup {
    setup: Vec<TxEnvelope>,
    pair_states: Vec<PairState>,
}

/// The read-only context every pair creation in one workload shares:
/// the chain id, the factory address, and the pair contract's
/// init-code hash (for the CREATE2 pair address).
struct PairFactoryCtx {
    chain_id: u64,
    factory: Address,
    pair_init_hash: B256,
}

/// Create each pair through `createPair` (the address is
/// CREATE2-deterministic), then fund it with liquidity: the deployer
/// mints to itself, transfers to the pair, and mints LP tokens.
fn create_pairs_and_liquidity(
    dep: &mut Signer<'_>,
    chain_id: u64,
    factory: Address,
    pair_init_hash: B256,
    tokens: &[Address],
    pairs: usize,
) -> PairsSetup {
    let ctx = PairFactoryCtx {
        chain_id,
        factory,
        pair_init_hash,
    };
    let mut setup = Vec::new();
    let mut pair_states: Vec<PairState> = Vec::new();
    for p in 0..pairs {
        create_one_pair_into(dep, &ctx, tokens, p, &mut setup, &mut pair_states);
    }

    let liq = U256::from(10u128.pow(24));
    for ps in &mut pair_states {
        setup.extend(seed_pair_liquidity(dep, chain_id, ps, liq));
    }
    PairsSetup { setup, pair_states }
}

/// One pair's `createPair` transaction and starting [`PairState`].
struct CreatedPair {
    tx: TxEnvelope,
    state: PairState,
}

/// Build [`CreatedPair`] for the pair at index `p` into `tokens`.
fn create_one_pair(
    dep: &mut Signer<'_>,
    ctx: &PairFactoryCtx,
    tokens: &[Address],
    p: usize,
) -> CreatedPair {
    let (a, b) = (tokens[p * 2], tokens[p * 2 + 1]);
    let (t0, t1) = if a < b { (a, b) } else { (b, a) };
    let mut salt_buf = [0u8; 40];
    salt_buf[..20].copy_from_slice(t0.as_slice());
    salt_buf[20..].copy_from_slice(t1.as_slice());
    let salt = keccak256(salt_buf);
    let mut c2 = Vec::with_capacity(85);
    c2.push(0xff);
    c2.extend_from_slice(ctx.factory.as_slice());
    c2.extend_from_slice(salt.as_slice());
    c2.extend_from_slice(ctx.pair_init_hash.as_slice());
    let pair_addr = Address::from_slice(&keccak256(c2)[12..]);
    let tx = dep.sign(
        ctx.chain_id,
        TxKind::Call(ctx.factory),
        CREATE_GAS,
        call(
            "createPair(address,address)",
            &[addr_word(t0), addr_word(t1)],
        ),
    );
    let state = PairState {
        addr: pair_addr,
        token0: t0,
        token1: t1,
        reserve0: U256::ZERO,
        reserve1: U256::ZERO,
    };
    CreatedPair { tx, state }
}

/// [`create_one_pair`] for the pair at index `p`, appending its
/// transaction to `setup` and its starting state to `pair_states`.
fn create_one_pair_into(
    dep: &mut Signer<'_>,
    ctx: &PairFactoryCtx,
    tokens: &[Address],
    p: usize,
    setup: &mut Vec<TxEnvelope>,
    pair_states: &mut Vec<PairState>,
) {
    let created = create_one_pair(dep, ctx, tokens, p);
    setup.push(created.tx);
    pair_states.push(created.state);
}

/// Fund one pair with `liq` of each token, mint its LP tokens, and set
/// its starting reserves.
fn seed_pair_liquidity(
    dep: &mut Signer<'_>,
    chain_id: u64,
    ps: &mut PairState,
    liq: U256,
) -> Vec<TxEnvelope> {
    let dep_addr = dep.s.signer.address();
    let mut txs = Vec::with_capacity(5);
    for t in [ps.token0, ps.token1] {
        txs.push(dep.sign(
            chain_id,
            TxKind::Call(t),
            CALL_GAS,
            call("mint(address,uint256)", &[addr_word(dep_addr), liq]),
        ));
        txs.push(dep.sign(
            chain_id,
            TxKind::Call(t),
            CALL_GAS,
            call("transfer(address,uint256)", &[addr_word(ps.addr), liq]),
        ));
    }
    txs.push(dep.sign(
        chain_id,
        TxKind::Call(ps.addr),
        CALL_GAS,
        call("mint(address)", &[addr_word(dep_addr)]),
    ));
    ps.reserve0 = liq;
    ps.reserve1 = liq;
    txs
}

/// The result of `fund_senders`.
struct Funded<'a> {
    senders: Vec<Signer<'a>>,
    setup: Vec<TxEnvelope>,
}

/// Mint every token for every non-deployer sender, so it can spend
/// them in the flow.
fn fund_senders<'a>(signers: &'a [DerivedSigner], chain_id: u64, tokens: &[Address]) -> Funded<'a> {
    let fund = U256::from(10u128.pow(23));
    let mut senders: Vec<Signer<'a>> = signers[1..]
        .iter()
        .map(|s| Signer { s, nonce: 0 })
        .collect();
    let setup = senders
        .iter_mut()
        .flat_map(|sn| sn.fund_mints(chain_id, tokens, fund))
        .collect();
    Funded { senders, setup }
}

/// The tunable knobs for [`generate`].
#[derive(Debug, Clone, Copy)]
pub struct UniswapParams {
    pub chain_id: u64,
    /// The number of isolated pools, 2 tokens each. This is the
    /// contention setting: senders' home pairs spread out in rotation.
    /// Non-zero by construction: a zero pair count would leave
    /// `pair_states` and `tokens` empty, and every flow op divides by
    /// their length.
    pub pairs: NonZeroUsize,
    pub flow_blocks: usize,
    /// Non-zero: `generate`'s setup-block chunking calls
    /// `.chunks(txs_per_block)`, which panics on 0.
    pub txs_per_block: NonZeroUsize,
    /// The percentage of flow operations that are swaps. The rest are
    /// plain ERC20 transfers between senders, the parallel-friendly
    /// class.
    pub swap_share_pct: u64,
    /// The percentage of swaps that hit a pair other than the
    /// sender's home pair, a cross-domain join.
    pub cross_pct: u64,
}

/// Generates the flow blocks over one workload's senders and pair state.
struct FlowGen<'a, 'b> {
    params: UniswapParams,
    senders: &'b mut [Signer<'a>],
    pair_states: &'b mut [PairState],
    tokens: &'b [Address],
}

/// The deterministic mixer state `generate_flow_blocks` uses to pick
/// each operation's shape, pseudo-randomly but reproducibly.
struct Mixer(u64);

impl Mixer {
    fn next(&mut self, x: u64) -> u64 {
        self.0 ^= x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 ^= self.0 >> 29;
        self.0 = self.0.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        self.0 ^= self.0 >> 32;
        self.0
    }
}

impl FlowGen<'_, '_> {
    /// Generate the flow blocks: a deterministic mix of home- and
    /// cross-pair swaps and plain ERC20 transfers, in a reproducible
    /// pseudo-random order. Mutates `senders`' nonces and
    /// `pair_states`' reserves as it walks, since each op's inputs
    /// depend on the state the prior ops left.
    fn generate_flow_blocks(&mut self) -> Vec<Vec<TxEnvelope>> {
        let mut mixer = Mixer(0x243F_6A88_85A3_08D3);
        (0..self.params.flow_blocks)
            .map(|b| self.generate_one_flow_block(b, &mut mixer))
            .collect()
    }

    /// One flow block: `txs_per_block` operations, round-robin over
    /// senders, each a swap or a plain transfer per `mixer`.
    fn generate_one_flow_block(&mut self, b: usize, mixer: &mut Mixer) -> Vec<TxEnvelope> {
        let txs_per_block = self.params.txs_per_block.get();
        let n_send = self.senders.len();
        let mut block: Vec<TxEnvelope> = Vec::with_capacity(txs_per_block);
        let mut op_i = 0usize;
        while block.len() < txs_per_block {
            self.push_one_op(b, op_i, n_send, txs_per_block, mixer, &mut block);
            op_i += 1;
        }
        block
    }

    /// One [`generate_one_flow_block`] operation: a swap or a plain
    /// transfer, chosen by `mixer`, appended to `block`.
    fn push_one_op(
        &mut self,
        b: usize,
        op_i: usize,
        n_send: usize,
        txs_per_block: usize,
        mixer: &mut Mixer,
        block: &mut Vec<TxEnvelope>,
    ) {
        let swap_share_pct = self.params.swap_share_pct;
        // b and op_i are mixer inputs, immediately reduced mod
        // n_send; wrap is fine, since only the reduced result
        // (not the pre-modulo value) has any meaning.
        let si = (b.wrapping_mul(131).wrapping_add(op_i.wrapping_mul(7))) % n_send;
        let r = mixer.next((b as u64) << 32 | op_i as u64);
        let room_for_swap = txs_per_block - block.len() >= 2;
        if r % 100 < swap_share_pct && room_for_swap {
            self.push_swap_op(si, r, block);
        } else {
            self.push_transfer_op(si, n_send, r, block);
        }
    }

    /// Swap on the home pair, or a cross pair: 2 transactions, appended
    /// to `block`.
    ///
    /// `r` is a mixer-hash `u64`; the `% n` reduction below bounds its
    /// use far under `usize::MAX` before it is cast.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "r is a mixer-hash u64; the % n reduction bounds its use far under usize::MAX before it is cast"
    )]
    fn push_swap_op(&mut self, si: usize, r: u64, block: &mut Vec<TxEnvelope>) {
        let chain_id = self.params.chain_id;
        let cross_pct = self.params.cross_pct;
        let me = self.senders[si].s.signer.address();
        let home = si % self.pair_states.len();
        let pi = if r / 100 % 100 < cross_pct {
            // `pair_states.len()` is `p.pairs.get()`, non-zero
            // by construction (see `UniswapParams::pairs`).
            (home + 1 + (r as usize / 10_000) % self.pair_states.len()) % self.pair_states.len()
        } else {
            home
        };
        let zero_for_one = r & 1 == 0;
        let amount_in = U256::from(10u128.pow(18) + u128::from(r % 1000) * 10u128.pow(15));
        let ps = &mut self.pair_states[pi];
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
        block.push(self.senders[si].sign(
            chain_id,
            TxKind::Call(tin),
            CALL_GAS,
            call(
                "transfer(address,uint256)",
                &[addr_word(pair_addr), amount_in],
            ),
        ));
        block.push(self.senders[si].sign(
            chain_id,
            TxKind::Call(pair_addr),
            CALL_GAS,
            call(
                "swap(uint256,uint256,address,bytes)",
                &[out0, out1, addr_word(me), U256::from(0x80u64), U256::ZERO],
            ),
        ));
    }

    /// A plain ERC20 transfer to another sender: the parallel class.
    /// Appended to `block`.
    ///
    /// `r` is a mixer-hash `u64`; the `% n` reductions below bound each
    /// use far under `usize::MAX` before it is cast.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "r is a mixer-hash u64; the % n reductions bound each use far under usize::MAX before it is cast"
    )]
    fn push_transfer_op(&mut self, si: usize, n_send: usize, r: u64, block: &mut Vec<TxEnvelope>) {
        let chain_id = self.params.chain_id;
        // n_send - 1 never underflows: generate()'s
        // `signers.len() >= 2` check guarantees n_send >= 2.
        let tj = (si + 1 + (r as usize >> 8) % (n_send - 1)) % n_send;
        let dst = self.senders[tj].s.signer.address();
        let t = self.tokens[(r as usize >> 16) % self.tokens.len()];
        let amt = U256::from(10u128.pow(15) + u128::from(r % 997));
        block.push(self.senders[si].sign(
            chain_id,
            TxKind::Call(t),
            CALL_GAS,
            call("transfer(address,uint256)", &[addr_word(dst), amt]),
        ));
    }
}

/// Generate the workload.
///
/// # Errors
///
/// Returns an error if fewer than 2 signers are given (a home pair
/// needs a counterparty), or if a vendored contract artifact cannot be
/// loaded from `repo_root`. `p.pairs` is a `NonZeroUsize`, so a zero
/// pair count is a CLI parse error, not a runtime check here.
pub fn generate(
    repo_root: &str,
    signers: &[DerivedSigner],
    p: UniswapParams,
) -> anyhow::Result<UniswapWorkload> {
    anyhow::ensure!(signers.len() >= 2, "uniswap needs at least 2 signers");
    let artifacts = load_artifacts(repo_root)?;
    let pair_init_hash = keccak256(&artifacts.pair_code);

    let deployer = &signers[0];
    let mut dep = Signer {
        s: deployer,
        nonce: 0,
    };
    let deploy_out = deploy_tokens_and_factory(&mut dep, p.chain_id, p.pairs.get(), &artifacts);
    let pairs_setup = create_pairs_and_liquidity(
        &mut dep,
        p.chain_id,
        deploy_out.factory,
        pair_init_hash,
        &deploy_out.tokens,
        p.pairs.get(),
    );
    let mut setup = deploy_out.setup;
    setup.extend(pairs_setup.setup);
    let mut pair_states = pairs_setup.pair_states;
    let funded = fund_senders(signers, p.chain_id, &deploy_out.tokens);
    setup.extend(funded.setup);
    let mut senders = funded.senders;

    // Slice setup into blocks of txs_per_block, for realistic shapes.
    let setup_blocks: Vec<Vec<TxEnvelope>> = setup
        .chunks(p.txs_per_block.get())
        .map(<[TxEnvelope]>::to_vec)
        .collect();

    let flows = FlowGen {
        params: p,
        senders: &mut senders,
        pair_states: &mut pair_states,
        tokens: &deploy_out.tokens,
    }
    .generate_flow_blocks();

    Ok(UniswapWorkload {
        setup_blocks,
        flow_blocks: flows,
    })
}
