//! The L1-to-L2 bridge round trip.
//!
//! [`deposit_round_trip`](deposit_round_trip): `depositETH` runs on L1. The da-watcher sees
//! the finalized log. The sequencer orders a `DepositRef`. The executor
//! mints funds and executes. A receipt, keyed by the OP-style `source_hash`,
//! appears on L2. The test then proves the credit by behavior: the
//! freshly-minted account spends its funds. (The ingress deliberately does
//! not serve `eth_getBalance`, so the test must show "the money arrived" by
//! using it, which is a stronger proof anyway.)
//!
//! [`withdrawal_round_trip`](withdrawal_round_trip): `initiateWithdrawal` runs on the L2
//! predeploy. The validator's attester posts an output root to the L1
//! oracle. The test warps past the finalization window, then rebuilds the
//! withdrawal leaf set and its Merkle proof (nothing in production builds
//! user proofs today, and this gap is exactly what the test pins down).
//! `finalizeWithdrawal` pays out on L1, and a replay of the same withdrawal
//! reverts.

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
    /// must have no other funds, so the later spend proves the mint landed.
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

/// # Errors
/// Returns an error when the deposit, its L2 receipt, or the beneficiary's
/// follow-up spend fails.
pub async fn deposit_round_trip(t: &Target, l1: &L1, p: DepositParams) -> Result<()> {
    let signers: Vec<DerivedSigner> = l2::dev_signers_through(p.beneficiary.max(p.payee))?;
    let beneficiary = &signers[p.beneficiary];
    let payee = signers[p.payee].address;
    let applied_before = t
        .executor_metric_opt(super::EXEC_TX_APPLIED)
        .await?
        .unwrap_or(0.0);

    // --- L1: deposit to the beneficiary. ---------------------------------
    let (block_hash, log_index) = l1
        .deposit_eth(beneficiary.address, U256::from(DEPOSIT_WEI))
        .await
        .context("depositETH")?;
    // The da-watcher reads only finalized logs. With `--slots-in-an-epoch 1`,
    // a few blocks advance the finalized cursor past the deposit.
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
    // if the mint landed and the executor's nonce and balance view agree.
    let spend = l2::sign_transfer(beneficiary, t.chain_id, 0, payee, 1_000)?;
    let out = t.rpc.send_raw(&spend.raw).await;
    let hash = out
        .result
        .map_err(|e| anyhow::anyhow!("spending the deposited funds failed: {e}"))?;
    anyhow::ensure!(hash == spend.hash, "spend hash mismatch");
    let spend_receipt = await_l2_receipt(t, spend.hash, "the spend").await?;
    assert_receipt_ok(&spend_receipt, "the spend")?;

    // The deposit itself is not a regular transaction (it has no nonce, and
    // gas price 0). So the applied counter moves by exactly one spend
    // beyond it.
    t.wait_executor_applied(applied_before + 1.0, Duration::from_secs(15))
        .await
        .context("executor applied the spend")?;
    Ok(())
}

/// Everything needed to finalize an initiated withdrawal on L1. Produced by
/// [`initiate_withdrawal`], consumed by [`finalize_withdrawal`]. See
/// [`finalize_withdrawal`] for a timing constraint between the two calls.
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
    /// Dev-mnemonic index of the L2 account that starts the withdrawal. The
    /// genesis in use must prefund it (`chains/dev-withdrawals.toml` funds
    /// only index 0).
    pub withdrawer: usize,
}

/// L2 half: call `initiateWithdrawal` on the predeploy, then rebuild the
/// withdrawal's leaf set and Merkle proof from the emitted event.
///
/// Rebuilding the proof here is not a shortcut, it is the point. Nothing
/// in production serves withdrawal proofs: the attester drops its leaves
/// after posting, and `withdrawal_proof` has no caller outside tests. So a
/// real withdrawer must reconstruct exactly this. The scenario pins down
/// that gap.
///
/// # Errors
/// Returns an error when the backing deposit, the `initiateWithdrawal`
/// call, or its receipt fails, or when the receipt carries no
/// `MessagePassed` log.
pub async fn initiate_withdrawal(
    t: &Target,
    l1: &L1,
    p: WithdrawalParams,
) -> Result<WithdrawalTicket> {
    use alloy_sol_types::SolEvent;

    let signers = l2::dev_signers_through(p.withdrawer)?;
    let withdrawer = &signers[p.withdrawer];
    // Fresh L1 recipient so the payout is unambiguous.
    let recipient = Address::from([0x5Au8; 20]);
    let value = U256::from(DEPOSIT_WEI / 4);

    // Fund the bridge first. `ETHLockbox` pays withdrawals out of ETH that
    // was actually deposited. It is an escrow, not a mint, so a
    // withdrawal with no backing deposit reverts on `finalizeWithdrawal`.
    // Depositing here makes this a genuine round trip: L1 to L2 to L1.
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
/// finalization window, finalize, and check the payout and replay
/// protection.
///
/// `finalizeWithdrawal` needs the state root the attester committed to, but
/// nothing exposes historical roots. So this function samples the
/// validator's state root over time and tests every distinct root it saw
/// against every posted output (see the sampler below for why the current
/// root alone is not enough).
///
/// Do not stop the block clock before calling this function. The
/// withdrawal's receipt is published when the transaction executes, but
/// its block commits only at the next sealer boundary. Freezing on receipt
/// strands the transaction in an uncommitted block, so it never reaches a
/// state root, and the attester never covers it.
///
/// # Errors
/// Returns an error when no attested output ever commits to the
/// withdrawal, when `finalizeWithdrawal` fails to send or reverts, when
/// the payout does not match, or when a replay unexpectedly succeeds.
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
    let finder = OutputFinder {
        oracle: &oracle,
        validator_state_dir,
        withdrawals_root,
    };
    let (output_index, state_root) = finder.find_attested_output().await?;

    // --- L1: finalize after the window. -----------------------------------
    l1.warp_past_window().await?;
    let wallet = l1.wallet(l1::DEPOSITOR_KEY)?;
    let lockbox = ETHLockbox::new(l1.lockbox, &wallet);
    let call = FinalizeCall {
        wtx: ETHLockbox::WithdrawalTransaction {
            nonce,
            sender,
            target: recipient,
            value,
        },
        output_index,
        state_root,
        withdrawals_root,
        proof,
    };
    let finalizer = WithdrawalFinalizer {
        wallet: &wallet,
        lockbox: &lockbox,
        call,
        recipient,
    };
    let after = finalizer.finalize_payout(value).await?;
    finalizer.assert_replay_rejected(after).await
}

/// The fields both [`finalize_payout`] and [`assert_replay_rejected`] send
/// to `finalizeWithdrawal`, built once in [`finalize_withdrawal`] so the
/// two calls cannot drift apart.
struct FinalizeCall {
    wtx: ETHLockbox::WithdrawalTransaction,
    output_index: U256,
    state_root: B256,
    withdrawals_root: B256,
    proof: Vec<B256>,
}

/// The most recently observed root (checked newest-first) that, paired
/// with `withdrawals_root`, produces the output root `posted` on chain.
fn matching_observed_root(roots: &[B256], posted: B256, withdrawals_root: B256) -> Option<B256> {
    roots
        .iter()
        .rev()
        .copied()
        .find(|root| kardamom_types::withdrawals::output_root(*root, withdrawals_root) == posted)
}

/// State for finding the one posted output that commits to a withdrawal's
/// leaf. Built once by [`finalize_withdrawal`], so
/// [`Self::find_attested_output`] reads its inputs as state instead of
/// loose parameters.
struct OutputFinder<'a, P: Provider + Clone> {
    oracle: &'a WithdrawalOutputOracle::WithdrawalOutputOracleInstance<P>,
    validator_state_dir: &'a std::path::Path,
    withdrawals_root: B256,
}

impl<P: Provider + Clone> OutputFinder<'_, P> {
    /// Find the one posted output that commits to `withdrawals_root`, and
    /// the validator state root it was attested against.
    ///
    /// The attester carries this withdrawal's leaf in exactly one posted
    /// output. A post pairs the arriving state root with `leaves_through`,
    /// then `mark_attested` drops those leaves (and `on_leaves` refuses to
    /// re-add them for an already-attested block). So exactly one
    /// (`state_root`, `withdrawals_root`) pair on chain can ever match. Its
    /// root is whichever root the feeder happened to deliver alongside it,
    /// which is not necessarily the root that is still head by the time
    /// this code looks.
    ///
    /// Sampling only the current head root would race: once a later block
    /// commits, the head moves off the attested root, and no future post
    /// can match. The remaining poll time would then be wasted. Instead,
    /// this spawns a sampler thread that remembers every root the
    /// validator was observed at (a cheap 100 ms sample, off the poll's
    /// own cadence, so a fast block cannot slip between two samples) and
    /// tests them all against every posted output. Proving against a
    /// historical root is exactly what `finalizeWithdrawal` expects: it
    /// takes `state_root` as an explicit argument.
    ///
    /// # Errors
    /// Returns an error when no posted output commits to
    /// `withdrawals_root` within 90s. The error names how many distinct
    /// roots were sampled, and, when the validator's state DB became
    /// unreadable, the last read error (the signal that the stack fell
    /// over, as opposed to attestation never covering the withdrawal).
    async fn find_attested_output(&self) -> Result<(U256, B256)> {
        let observed: std::sync::Arc<std::sync::Mutex<Vec<B256>>> = std::sync::Arc::default();
        // Keep the most recent read error. A validator that died leaves its
        // mdbx environment in an unsteady state. The resulting
        // `MDBX_WANNA_RECOVERY` error, on the read-only open, is the signal
        // that tells "the stack fell over" apart from "the withdrawal was
        // never attested". The sampler owns this read, so it must carry the
        // error out to the failure message.
        let last_err: std::sync::Arc<std::sync::Mutex<Option<String>>> = std::sync::Arc::default();
        let sampler_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sampler = {
            let observed = observed.clone();
            let last_err = last_err.clone();
            let stop = sampler_stop.clone();
            let dir = self.validator_state_dir.to_path_buf();
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
                let count = self
                    .oracle
                    .outputCount()
                    .call()
                    .await
                    .unwrap_or(U256::ZERO)
                    .to::<u64>();
                // Newest first: the withdrawal's block is near the head.
                for i in (0..count).rev() {
                    let idx = U256::from(i);
                    let Ok(posted) = self.oracle.outputRootAt(idx).call().await else {
                        continue;
                    };
                    let roots = observed.lock().expect("observed roots poisoned").clone();
                    if let Some(root) =
                        matching_observed_root(&roots, posted, self.withdrawals_root)
                    {
                        return Ok(Some((idx, root)));
                    }
                }
                Ok(None)
            },
        )
        .await;
        sampler_stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = sampler.join();
        found.with_context(|| {
            let roots = observed.lock().expect("observed roots poisoned").len();
            match last_err.lock().expect("sampler error poisoned").as_deref() {
                // A read error means the stack fell over. It does not mean
                // attestation is broken, so say so instead of blaming the
                // attester.
                Some(e) => format!(
                    "could not track the validator's state roots ({roots} sampled before the \
                     last failure) — the validator's state DB became unreadable, which is what \
                     an unclean validator exit looks like: {e}"
                ),
                None => format!(
                    "no posted output commits to (any observed validator state root, \
                     withdrawals root) — the attester never covered the withdrawal's block \
                     ({roots} distinct roots sampled)"
                ),
            }
        })
    }
}

/// State for finalizing one withdrawal: the wallet and lockbox it pays
/// out through, the call built once by [`finalize_withdrawal`], and the
/// recipient. [`Self::finalize_payout`] and [`Self::assert_replay_rejected`]
/// read these as state instead of loose parameters, so the two calls
/// cannot drift apart.
struct WithdrawalFinalizer<'a, PW: Provider + Clone, PL: Provider + Clone> {
    wallet: &'a PW,
    lockbox: &'a ETHLockbox::ETHLockboxInstance<PL>,
    call: FinalizeCall,
    recipient: Address,
}

impl<PW: Provider + Clone, PL: Provider + Clone> WithdrawalFinalizer<'_, PW, PL> {
    /// Send `finalizeWithdrawal` and check the payout landed. Returns the
    /// recipient's post-payout balance, for [`Self::assert_replay_rejected`]
    /// to compare against.
    async fn finalize_payout(&self, value: U256) -> Result<U256> {
        let before = self
            .wallet
            .get_balance(self.recipient)
            .await
            .context("recipient balance before")?;
        let pending = self
            .lockbox
            .finalizeWithdrawal(
                self.call.wtx.clone(),
                self.call.output_index,
                self.call.state_root,
                self.call.withdrawals_root,
                U256::ZERO,
                self.call.proof.clone(),
            )
            // Use explicit gas to skip estimation. Gas estimation is where
            // the alloy/anvil post-warp flake in `withdrawal_e2e.rs`
            // bites.
            .gas(2_000_000)
            .send()
            .await
            .context("send finalizeWithdrawal")?;
        let tx_hash = *pending.tx_hash();
        let receipt = l1::await_l1_receipt(self.wallet, tx_hash, "finalizeWithdrawal").await?;
        anyhow::ensure!(
            receipt.status(),
            "finalizeWithdrawal reverted (tx {tx_hash}) — the lockbox escrows only DEPOSITED \
             ETH, so a withdrawal must be backed by a prior deposit"
        );
        let after = self
            .wallet
            .get_balance(self.recipient)
            .await
            .context("recipient balance after")?;
        // `before` and `value` are wallet-balance and withdrawal wire values;
        // an overflowing expectation must fail the check, not panic or wrap.
        let expected = before
            .checked_add(value)
            .context("recipient balance before + withdrawal value overflows U256")?;
        anyhow::ensure!(
            after == expected,
            "recipient balance {before} -> {after}, expected +{value}"
        );
        Ok(after)
    }

    /// Replaying the same withdrawal must fail. The lockbox flags the leaf
    /// on finalize (`AlreadyFinalized`). With explicit gas there is no
    /// pre-flight estimation, so the revert appears in the receipt, not
    /// at send time. The recipient must not be paid twice.
    async fn assert_replay_rejected(&self, after: U256) -> Result<()> {
        let replay = self
            .lockbox
            .finalizeWithdrawal(
                self.call.wtx.clone(),
                self.call.output_index,
                self.call.state_root,
                self.call.withdrawals_root,
                U256::ZERO,
                self.call.proof.clone(),
            )
            .gas(2_000_000)
            .send()
            .await;
        match replay {
            Err(_) => {} // an outright rejection is also acceptable
            Ok(pending) => {
                let hash = *pending.tx_hash();
                let receipt = l1::await_l1_receipt(self.wallet, hash, "replay").await?;
                anyhow::ensure!(
                    !receipt.status(),
                    "replaying a finalized withdrawal SUCCEEDED — double-spend of the bridge \
                     escrow"
                );
            }
        }
        let final_balance = self
            .wallet
            .get_balance(self.recipient)
            .await
            .context("recipient balance after replay")?;
        anyhow::ensure!(
            final_balance == after,
            "replay changed the recipient's balance {after} -> {final_balance}"
        );
        Ok(())
    }
}
