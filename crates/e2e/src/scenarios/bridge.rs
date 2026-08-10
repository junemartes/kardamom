//! S1/S2 — the L1↔L2 bridge round-trip.
//!
//! S1 ([`deposit_round_trip`]): `depositETH` on L1 → the da-watcher sees the
//! finalized log → the sequencer orders a `DepositRef` → the executor mints
//! and executes → a receipt keyed by the OP-style `source_hash` surfaces on
//! L2. The credit is then proven **behaviourally**: the freshly-minted
//! account spends its funds. (The ingress deliberately does not serve
//! `eth_getBalance`, so "the money arrived" has to be demonstrated by using
//! it — which is a stronger statement anyway.)
//!
//! S2 ([`withdrawal_round_trip`]): `initiateWithdrawal` on the L2 predeploy →
//! the validator's attester posts an output root to the L1 oracle → warp past
//! the finalization window → the test rebuilds the withdrawal leaf set and
//! its Merkle proof (nothing in production builds user proofs today — that
//! gap is exactly what this pins) → `finalizeWithdrawal` pays out on L1, and
//! a replay of the same withdrawal reverts.

use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use anyhow::{Context, Result};

use super::{
    Target, assert_receipt_ok, await_l2_receipt, read_validator_state_root, receipt_field,
};
use crate::harness::l1::{self, ETHLockbox, L1, L2ToL1MessagePasser, WithdrawalOutputOracle};
use crate::harness::l2::{self, DerivedSigner};
use crate::harness::metrics::poll_until;

/// 1 ETH.
const DEPOSIT_WEI: u128 = 1_000_000_000_000_000_000;

pub struct DepositParams {
    /// Dev-mnemonic index of the fresh L2 account the deposit mints to. It
    /// must be otherwise unfunded so the later spend proves the mint landed.
    pub beneficiary: usize,
    /// Dev-mnemonic index that receives the beneficiary's spend.
    pub payee: usize,
}

impl Default for DepositParams {
    fn default() -> Self {
        Self {
            beneficiary: 6,
            payee: 7,
        }
    }
}

pub async fn deposit_round_trip(t: &Target, l1: &L1, p: DepositParams) -> Result<()> {
    let signers: Vec<DerivedSigner> = l2::dev_signers(p.beneficiary.max(p.payee) as u32 + 1)?;
    let beneficiary = &signers[p.beneficiary];
    let payee = signers[p.payee].address;
    let applied_before = t
        .executor_metric(super::EXEC_TX_APPLIED)
        .await
        .unwrap_or(0.0);

    // --- L1: deposit to the beneficiary. ---------------------------------
    let (block_hash, log_index) = l1
        .deposit_eth(beneficiary.address, U256::from(DEPOSIT_WEI))
        .await
        .context("depositETH")?;
    // The da-watcher only reads FINALIZED logs; with --slots-in-an-epoch 1 a
    // few blocks advance the finalized cursor past the deposit.
    l1.mine(6).await?;

    // --- L2: the deposit surfaces as a receipt keyed by source_hash. ------
    let source_hash = kardamom_da_watcher::source_hash(block_hash, log_index);
    let receipt = await_l2_receipt(t, source_hash, "the deposit").await?;
    assert_receipt_ok(&receipt, "the deposit")?;
    anyhow::ensure!(
        receipt_field(&receipt, "effectiveGasPrice") == Some("0x0"),
        "deposit must execute at gas price 0: {receipt}"
    );
    if let Some(to) = receipt_field(&receipt, "to") {
        anyhow::ensure!(
            to.eq_ignore_ascii_case(&beneficiary.address.to_string()),
            "deposit receipt `to` is {to}, expected {}",
            beneficiary.address
        );
    }

    // --- The credit is real: the minted account spends it. ---------------
    // A transfer from an account that started with nothing can only succeed
    // if the mint landed AND the executor's nonce/balance view agrees.
    let spend = l2::sign_transfer(beneficiary, t.chain_id, 0, payee, 1_000)?;
    let out = t.rpc.send_raw(&spend.raw).await;
    let hash = out
        .result
        .map_err(|e| anyhow::anyhow!("spending the deposited funds failed: {e}"))?;
    anyhow::ensure!(hash == spend.hash, "spend hash mismatch");
    let spend_receipt = await_l2_receipt(t, spend.hash, "the spend").await?;
    assert_receipt_ok(&spend_receipt, "the spend")?;

    // The deposit itself is not a regular tx (no nonce, gas price 0), so the
    // applied counter moves by exactly the one spend beyond it.
    t.wait_executor_applied(applied_before + 1.0, Duration::from_secs(15))
        .await
        .context("executor applied the spend")?;
    Ok(())
}

/// Everything needed to finalize an initiated withdrawal on L1. Produced by
/// [`initiate_withdrawal`], consumed by [`finalize_withdrawal`]; between the
/// two the runner must stop the block clock (see [`finalize_withdrawal`]).
pub struct WithdrawalTicket {
    pub nonce: U256,
    pub sender: Address,
    pub target: Address,
    pub value: U256,
    pub withdrawals_root: B256,
    pub proof: Vec<B256>,
}

#[derive(Default)]
pub struct WithdrawalParams {
    /// Dev-mnemonic index of the L2 account initiating the withdrawal. Must
    /// be prefunded by the genesis in use (`chains/dev-withdrawals.toml`
    /// funds index 0 only).
    pub withdrawer: usize,
}

/// L2 half: call `initiateWithdrawal` on the predeploy and rebuild the
/// withdrawal's leaf set + Merkle proof from the emitted event.
///
/// Rebuilding here is not a shortcut — it is the point. Nothing in
/// production serves withdrawal proofs (the attester drops its leaves after
/// posting, and `withdrawal_proof` has no caller outside tests), so a real
/// withdrawer must reconstruct exactly this. The scenario pins that gap.
pub async fn initiate_withdrawal(
    t: &Target,
    l1: &L1,
    p: WithdrawalParams,
) -> Result<WithdrawalTicket> {
    use alloy_sol_types::SolEvent;

    let signers = l2::dev_signers(p.withdrawer as u32 + 1)?;
    let withdrawer = &signers[p.withdrawer];
    // Fresh L1 recipient so the payout is unambiguous.
    let recipient = Address::from([0x5Au8; 20]);
    let value = U256::from(DEPOSIT_WEI / 4);

    // Fund the bridge first. `ETHLockbox` pays withdrawals out of ETH that was
    // actually deposited — it is an escrow, not a mint — so a withdrawal with
    // no backing deposit reverts on `finalizeWithdrawal`. Depositing here
    // makes this a genuine round-trip: L1 → L2 → L1.
    let (block_hash, log_index) = l1
        .deposit_eth(withdrawer.address, U256::from(DEPOSIT_WEI))
        .await
        .context("fund the lockbox with a deposit")?;
    l1.mine(6).await?;
    let source_hash = kardamom_da_watcher::source_hash(block_hash, log_index);
    await_l2_receipt(t, source_hash, "the backing deposit").await?;

    // --- L2: initiateWithdrawal on the predeploy. -------------------------
    let call = L2ToL1MessagePasser::initiateWithdrawalCall { target: recipient };
    let tx = l2::sign_call(
        withdrawer,
        t.chain_id,
        0,
        kardamom_types::withdrawals::MESSAGE_PASSER,
        value,
        &alloy_sol_types::SolCall::abi_encode(&call),
    )?;
    let out = t.rpc.send_raw(&tx.raw).await;
    out.result
        .map_err(|e| anyhow::anyhow!("initiateWithdrawal failed: {e}"))?;
    let receipt = await_l2_receipt(t, tx.hash, "the withdrawal").await?;
    assert_receipt_ok(&receipt, "initiateWithdrawal")?;

    // The MessagePassed log carries the nonce this withdrawal was assigned.
    let logs = receipt
        .get("logs")
        .and_then(|l| l.as_array())
        .context("withdrawal receipt has no logs")?;
    anyhow::ensure!(!logs.is_empty(), "no MessagePassed log: {receipt}");
    let topics: Vec<B256> = logs[0]
        .get("topics")
        .and_then(|t| t.as_array())
        .context("log has no topics")?
        .iter()
        .filter_map(|t| t.as_str())
        .filter_map(|s| s.parse::<B256>().ok())
        .collect();
    anyhow::ensure!(
        topics.first() == Some(&L2ToL1MessagePasser::MessagePassed::SIGNATURE_HASH),
        "first log is not MessagePassed"
    );
    let nonce = U256::from_be_slice(topics.get(1).context("no nonce topic")?.as_slice());

    let leaf =
        kardamom_types::withdrawals::withdrawal_leaf(nonce, withdrawer.address, recipient, value);
    let leaves = vec![leaf];
    Ok(WithdrawalTicket {
        nonce,
        sender: withdrawer.address,
        target: recipient,
        value,
        withdrawals_root: kardamom_types::withdrawals::withdrawals_root(&leaves),
        proof: kardamom_types::withdrawals::withdrawal_proof(&leaves, 0),
    })
}

/// L1 half: match the withdrawal to its attested output, warp past the
/// finalization window, finalize, and check the payout + replay protection.
///
/// `finalizeWithdrawal` needs the state root the attester committed to, and
/// nothing exposes historical roots — so this matches against the
/// validator's CURRENT root. That is sound because the chain quiesces on its
/// own: blocks are committed only when they carry transactions (empty
/// boundaries do not commit), so once the withdrawal's block lands and no
/// further traffic follows, the validator's head root stays put and is
/// exactly the root the attester attested.
///
/// Do NOT stop the block clock before calling. The withdrawal's receipt is
/// published when the tx EXECUTES, but its block is committed only at the
/// next sealer boundary — freezing on receipt strands the tx in an
/// uncommitted block, so it never reaches a state root and the attester
/// never covers it.
pub async fn finalize_withdrawal(
    l1: &L1,
    ticket: WithdrawalTicket,
    validator_state_dir: &std::path::Path,
) -> Result<()> {
    let WithdrawalTicket {
        nonce,
        sender,
        target: recipient,
        value,
        withdrawals_root,
        proof,
    } = ticket;

    let oracle = WithdrawalOutputOracle::new(l1.oracle, l1.provider());

    // The attester carries this withdrawal's leaf in exactly ONE posted
    // output: a post pairs the arriving state root with `leaves_through`,
    // then `mark_attested` DROPS those leaves (and `on_leaves` refuses to
    // re-add them for an already-attested block). So there is a single
    // (state_root, withdrawals_root) pair on chain that can ever match, and
    // its root is whichever one the feeder happened to deliver alongside —
    // not necessarily the root that is still head by the time we look.
    //
    // Sampling only the CURRENT head root therefore raced: once a later
    // block committed, the head moved off the attested root and no future
    // post could match, so the remaining poll budget was dead time.
    // Instead, remember every root the validator is observed at (cheap,
    // 100 ms cadence, off the poll's own cadence so a fast block can't slip
    // between two samples) and test them all. Proving against a historical
    // root is exactly what `finalizeWithdrawal` expects — it takes the
    // `state_root` as an explicit argument.
    let observed: std::sync::Arc<std::sync::Mutex<Vec<B256>>> = Default::default();
    // Keep the most recent read error: a validator that died takes its mdbx
    // env down unsteadily, and the resulting `MDBX_WANNA_RECOVERY` on the
    // read-only open is the signal that separates "the stack fell over" from
    // "the withdrawal was never attested". It used to surface directly
    // because the poll read the root itself; now the sampler owns that read,
    // so it has to carry the error out to the failure message.
    let last_err: std::sync::Arc<std::sync::Mutex<Option<String>>> = Default::default();
    let sampler_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let observed = observed.clone();
        let last_err = last_err.clone();
        let stop = sampler_stop.clone();
        let dir = validator_state_dir.to_path_buf();
        std::thread::Builder::new()
            .name("s2-root-sampler".into())
            .spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    match read_validator_state_root(&dir) {
                        Ok(Some(r)) => {
                            let mut seen = observed.lock().expect("observed roots poisoned");
                            if seen.last() != Some(&r) {
                                seen.push(r);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            *last_err.lock().expect("sampler error poisoned") =
                                Some(format!("{e:?}"));
                        }
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            })
            .context("spawn S2 root sampler")?
    };

    let found = poll_until(
        "an attested output committing to this withdrawal",
        Duration::from_secs(90),
        Duration::from_millis(500),
        || async {
            let count = oracle
                .outputCount()
                .call()
                .await
                .unwrap_or(U256::ZERO)
                .to::<u64>();
            // Newest first: the withdrawal's block is near the head.
            for i in (0..count).rev() {
                let idx = U256::from(i);
                let Ok(posted) = oracle.outputRootAt(idx).call().await else {
                    continue;
                };
                let roots = observed.lock().expect("observed roots poisoned").clone();
                for root in roots.iter().rev() {
                    let expected =
                        kardamom_types::withdrawals::output_root(*root, withdrawals_root);
                    if posted == expected {
                        return Ok(Some((idx, *root)));
                    }
                }
            }
            Ok(None)
        },
    )
    .await;
    sampler_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = sampler.join();
    let (output_index, state_root) = found.with_context(|| {
        let roots = observed.lock().expect("observed roots poisoned").len();
        match last_err.lock().expect("sampler error poisoned").as_deref() {
            // A read error means the stack fell over, not that attestation
            // is broken — say so instead of blaming the attester.
            Some(e) => format!(
                "could not track the validator's state roots ({roots} sampled before the last \
                 failure) — the validator's state DB became unreadable, which is what an unclean \
                 validator exit looks like: {e}"
            ),
            None => format!(
                "no posted output commits to (any observed validator state root, withdrawals \
                 root) — the attester never covered the withdrawal's block ({roots} distinct \
                 roots sampled)"
            ),
        }
    })?;

    // --- L1: finalize after the window. -----------------------------------
    l1.warp_past_window().await?;
    let wallet = l1.wallet(l1::DEPOSITOR_KEY)?;
    let lockbox = ETHLockbox::new(l1.lockbox, &wallet);
    let wtx = ETHLockbox::WithdrawalTransaction {
        nonce,
        sender,
        target: recipient,
        value,
    };
    let before = wallet
        .get_balance(recipient)
        .await
        .context("recipient balance before")?;
    let pending = lockbox
        .finalizeWithdrawal(
            wtx.clone(),
            output_index,
            state_root,
            withdrawals_root,
            U256::ZERO,
            proof.clone(),
        )
        // Explicit gas: skip estimation, which is where the alloy/anvil
        // post-warp flake in `withdrawal_e2e.rs` bites.
        .gas(2_000_000)
        .send()
        .await
        .context("send finalizeWithdrawal")?;
    let tx_hash = *pending.tx_hash();
    // Poll the receipt manually rather than using alloy's watcher (same
    // flake).
    let receipt = poll_until(
        "finalizeWithdrawal receipt",
        Duration::from_secs(30),
        Duration::from_millis(250),
        || async { Ok(wallet.get_transaction_receipt(tx_hash).await.ok().flatten()) },
    )
    .await?;
    anyhow::ensure!(
        receipt.status(),
        "finalizeWithdrawal reverted (tx {tx_hash}) — the lockbox escrows only DEPOSITED ETH, \
         so a withdrawal must be backed by a prior deposit"
    );
    let after = wallet
        .get_balance(recipient)
        .await
        .context("recipient balance after")?;
    anyhow::ensure!(
        after == before + value,
        "recipient balance {before} -> {after}, expected +{value}"
    );

    // Replaying the same withdrawal must fail: the lockbox flags the leaf on
    // finalize (`AlreadyFinalized`). With explicit gas there is no pre-flight
    // estimation, so the revert surfaces in the RECEIPT, not at send time —
    // and the recipient must not be paid twice.
    let replay = lockbox
        .finalizeWithdrawal(
            wtx,
            output_index,
            state_root,
            withdrawals_root,
            U256::ZERO,
            proof,
        )
        .gas(2_000_000)
        .send()
        .await;
    match replay {
        Err(_) => {} // rejected outright — also acceptable
        Ok(pending) => {
            let hash = *pending.tx_hash();
            let receipt = poll_until(
                "replay receipt",
                Duration::from_secs(30),
                Duration::from_millis(250),
                || async { Ok(wallet.get_transaction_receipt(hash).await.ok().flatten()) },
            )
            .await?;
            anyhow::ensure!(
                !receipt.status(),
                "replaying a finalized withdrawal SUCCEEDED — double-spend of the bridge escrow"
            );
        }
    }
    let final_balance = wallet
        .get_balance(recipient)
        .await
        .context("recipient balance after replay")?;
    anyhow::ensure!(
        final_balance == after,
        "replay changed the recipient's balance {after} -> {final_balance}"
    );
    Ok(())
}
