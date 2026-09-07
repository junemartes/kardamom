//! Block generation for each `--scenario` value.

use alloy_primitives::U256;
use anyhow::Context;
use kardamom_bench::signers::{DerivedSigner, SignerSet};
use kardamom_bench::stm::uniswap;
use kardamom_types::TxEnvelope;

use super::args::{Args, Scenario};

use kardamom_bench::stm::workload::ScenarioBlocks;

/// All of a scenario's blocks, setup first, plus the setup count.
pub(crate) struct Blocks {
    pub(crate) all: Vec<Vec<TxEnvelope>>,
    pub(crate) n_setup: usize,
}

/// Build the setup and flow blocks for `a.scenario`.
pub(crate) fn build_blocks(a: &Args, signers: &[DerivedSigner]) -> anyhow::Result<Blocks> {
    let blocks = match a.scenario {
        Scenario::Uniswap => uniswap_blocks(a, signers)?,
        Scenario::Defi => defi_blocks(a, signers)?,
        Scenario::Partransfer => partransfer_blocks(a, signers)?,
        Scenario::Parcounter => parcounter_blocks(a, signers)?,
        Scenario::Transfers => transfers_blocks(a, signers)?,
    };
    let n_setup = blocks.setup.len();
    let mut all = blocks.setup;
    all.extend(blocks.flows);
    Ok(Blocks { all, n_setup })
}

fn uniswap_blocks(a: &Args, signers: &[DerivedSigner]) -> anyhow::Result<ScenarioBlocks> {
    let w = uniswap::generate(
        &a.repo_root,
        signers,
        uniswap::UniswapParams {
            chain_id: a.chain_id,
            pairs: a.pairs,
            flow_blocks: a.blocks,
            txs_per_block: std::num::NonZeroUsize::new(a.block_size)
                .context("--block-size must be non-zero for the uniswap scenario")?,
            swap_share_pct: a.swap_share,
            cross_pct: a.cross,
        },
    )?;
    Ok(ScenarioBlocks {
        setup: w.setup_blocks,
        flows: w.flow_blocks,
    })
}

fn defi_blocks(a: &Args, signers: &[DerivedSigner]) -> anyhow::Result<ScenarioBlocks> {
    let signers = SignerSet::new(signers.to_vec())?;
    kardamom_bench::stm::workload::defi_blocks(
        &signers,
        a.chain_id,
        a.blocks,
        a.block_size,
        a.senders,
    )
}

/// These are fully independent plain transfers: sender i, one
/// transaction per block, with senders at least `block_size`, pays 1
/// wei to a fresh address derived from (sender, block) that nothing
/// else ever touches. There are no sender chains, no recipient
/// overlap, and no code: this is the pure 21k-gas rung. The structural
/// question it isolates is how much of a short transaction the
/// engine's serial parts, the feed and the fold, consume. That is the
/// Amdahl ceiling for micro-transactions.
fn partransfer_blocks(a: &Args, signers: &[DerivedSigner]) -> anyhow::Result<ScenarioBlocks> {
    let mut nonces = vec![0u64; signers.len()];
    let flows = (0..a.blocks)
        .map(|bidx| partransfer_block(a, signers, &mut nonces, bidx))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(ScenarioBlocks {
        setup: Vec::new(),
        flows,
    })
}

/// One `partransfer` block: `a.block_size` fully independent transfers,
/// round-robin across `signers`, each to a fresh address derived from
/// `(bidx, i)`. Mutates `nonces` as it signs.
fn partransfer_block(
    a: &Args,
    signers: &[DerivedSigner],
    nonces: &mut [u64],
    bidx: usize,
) -> anyhow::Result<Vec<TxEnvelope>> {
    use alloy_consensus::TxLegacy;
    use alloy_primitives::TxKind;
    (0..a.block_size)
        .map(|i| {
            let si = i % signers.len();
            let mut fresh = [0u8; 20];
            fresh[..8].copy_from_slice(&(bidx as u64).to_be_bytes());
            fresh[8..16].copy_from_slice(&(i as u64).to_be_bytes());
            fresh[19] = 0xEE;
            let tx = TxLegacy {
                chain_id: Some(a.chain_id),
                nonce: nonces[si],
                gas_price: 1_000_000_000,
                gas_limit: 21_000,
                to: TxKind::Call(alloy_primitives::Address::from(fresh)),
                value: U256::from(1u64),
                input: alloy_primitives::Bytes::default(),
            };
            nonces[si] += 1;
            signers[si].sign_envelope(tx)
        })
        .collect()
}

/// These are fully independent contract calls, the bottom rung of the
/// dependency ladder. Each sender deploys its own 10-byte counter, a
/// slot-0 increment, in setup, then calls it once per block. With
/// senders at least `block_size`, no two transactions in a block share
/// any state: distinct sender, distinct contract, distinct slot. Any
/// idle time or sub-linear scaling here is an engine defect by
/// construction, not a workload structure issue. Contract-call weight
/// keeps the serial feed from masking the scaling, which plain
/// transfers cannot do: they cap at a low speedup by Amdahl's law,
/// regardless of the engine.
///
#[allow(
    clippy::cast_possible_truncation,
    reason = "n's hi/lo split into u8 halves is a lossless bit-mask-then-narrow, not a truncating conversion; the fixed 22-byte runtime literal's length also stays far under u8::MAX"
)]
fn parcounter_blocks(a: &Args, signers: &[DerivedSigner]) -> anyhow::Result<ScenarioBlocks> {
    use alloy_consensus::TxLegacy;
    use alloy_primitives::TxKind;
    // The runtime does `call_work` loop iterations of slot0 += 1. A
    // warm sload or sstore is fast in revm, so only a loop can reach
    // contract-scale per-transaction weight:
    //   PUSH2 N; JUMPDEST@3; PUSH1 0 SLOAD; PUSH1 1 ADD;
    //   PUSH1 0 SSTORE; PUSH1 1; SWAP1; SUB; DUP1; PUSH1 3;
    //   JUMPI; STOP
    let n = u16::try_from(a.call_work.get())
        .context("call_work exceeds u16::MAX; PUSH2's immediate cannot hold it")?;
    let runtime: Vec<u8> = vec![
        0x61,
        (n >> 8) as u8,
        (n & 0xff) as u8,
        0x5b,
        0x60,
        0x00,
        0x54,
        0x60,
        0x01,
        0x01,
        0x60,
        0x00,
        0x55,
        0x60,
        0x01,
        0x90,
        0x03,
        0x80,
        0x60,
        0x03,
        0x57,
        0x00,
    ];
    // The init sequence is: PUSH1 len PUSH1 off PUSH1 0 CODECOPY
    // PUSH1 len PUSH1 0 RETURN, followed by the runtime.
    let mut init = vec![
        0x60,
        runtime.len() as u8,
        0x60,
        0x0c,
        0x60,
        0x00,
        0x39,
        0x60,
        runtime.len() as u8,
        0x60,
        0x00,
        0xf3,
    ];
    init.extend_from_slice(&runtime);
    let sel = [0xAA, 0xBB, 0xCC, 0xDDu8];
    let mut nonces = vec![0u64; signers.len()];
    let counters: Vec<alloy_primitives::Address> = signers
        .iter()
        .map(|s| s.signer.address().create(0))
        .collect();
    let mk = |si: usize,
              nonce: u64,
              kind: TxKind,
              input: Vec<u8>,
              gas: u64|
     -> anyhow::Result<TxEnvelope> {
        let tx = TxLegacy {
            chain_id: Some(a.chain_id),
            nonce,
            gas_price: 1_000_000_000,
            gas_limit: gas,
            to: kind,
            value: U256::ZERO,
            input: input.into(),
        };
        signers[si].sign_envelope(tx)
    };
    let setup: Vec<TxEnvelope> = (0..signers.len())
        .map(|si| {
            nonces[si] += 1;
            mk(si, 0, TxKind::Create, init.clone(), 200_000)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let flow_block = |nonces: &mut Vec<u64>| -> anyhow::Result<Vec<TxEnvelope>> {
        (0..a.block_size)
            .map(|i| {
                let si = i % signers.len();
                let tx = mk(
                    si,
                    nonces[si],
                    TxKind::Call(counters[si]),
                    sel.to_vec(),
                    60_000 + u64::from(n) * 400,
                );
                nonces[si] += 1;
                tx
            })
            .collect()
    };
    let flows = (0..a.blocks)
        .map(|_| flow_block(&mut nonces))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(ScenarioBlocks {
        setup: vec![setup],
        flows,
    })
}

fn transfers_blocks(a: &Args, signers: &[DerivedSigner]) -> anyhow::Result<ScenarioBlocks> {
    kardamom_bench::stm::workload::transfers_blocks(signers, a.chain_id, a.blocks, a.block_size)
}
