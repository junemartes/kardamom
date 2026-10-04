//! The priority fee scenario (`s18`): the stack runs with priority fees
//! on (the sequencer's fee checks and tip, the sealer's ordering window,
//! the executor's fee schedule from the genesis `[fees]` section).
//!
//! - s18a: a transfer at the standard price lands; its receipt reports
//!   the block's base fee as the price per gas, and the tip rate and
//!   amount apart; the fee RPCs serve the history and a suggestion. A
//!   transfer priced under the base fee is rejected on the feed with the
//!   sequencer's `FeeTooLow`, before it holds a nonce slot: the sender's
//!   next transfer at the same nonce lands.

use alloy_primitives::{Address, U256};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use super::{CODE_TIMEOUT, Target, await_l2_receipt};
use crate::harness::l2::{self, RpcError};

pub struct Params {
    /// The dev-mnemonic index of the sender.
    pub sender: usize,
}

impl Default for Params {
    fn default() -> Self {
        Self { sender: 22 }
    }
}

/// The initial base fee of the deploy's fee schedule, in wei per gas
/// (`deploy/cluster/config/genesis/fees.toml`). The schedule lowers it
/// by one eighth per empty block, so every block's base fee is under it.
const INITIAL_BASE_FEE: u128 = 1_000_000_000;

/// A hex quantity field of a receipt, as a number.
fn quantity(receipt: &Value, key: &str) -> Result<u128> {
    let raw = receipt
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("receipt has no {key}"))?;
    u128::from_str_radix(raw.trim_start_matches("0x"), 16)
        .with_context(|| format!("receipt {key} {raw} is not a hex quantity"))
}

/// The sender's standard transfer lands and pays the base fee plus the
/// full tip; the fee RPCs serve; a transfer under the base fee is
/// rejected and holds no nonce slot.
///
/// # Errors
/// Returns an error when a step does not match the fee rules above.
pub async fn under_base_fee_is_rejected(t: &Target, p: &Params) -> Result<()> {
    let signer = l2::dev_signers_through(p.sender)?
        .pop()
        .context("dev signer")?;
    let payee = Address::repeat_byte(0x18);

    // The standard transfer: the cap (1 gwei) is at or above every base
    // fee of the schedule, so it lands.
    let standard = l2::sign_transfer(&signer, t.chain_id, 0, payee, 1)?;
    t.rpc
        .send_raw(&standard.raw)
        .await
        .result
        .map_err(|e| anyhow::anyhow!("standard transfer: {e}"))?;
    let receipt = await_l2_receipt(t, standard.hash, "standard transfer").await?;
    let base_fee = quantity(&receipt, "effectiveGasPrice")?;
    let tip_rate = quantity(&receipt, "priorityFeePerGas")?;
    let tip_paid = quantity(&receipt, "priorityFeePaid")?;
    if base_fee == 0 || base_fee > INITIAL_BASE_FEE {
        bail!("effectiveGasPrice {base_fee} is not a scheduled base fee");
    }
    // A legacy price bids its part above the base fee, and pays it on
    // the whole gas limit.
    if tip_rate != INITIAL_BASE_FEE - base_fee || tip_paid != tip_rate * 21_000 {
        bail!("tip rate {tip_rate} and tip paid {tip_paid} do not match base fee {base_fee}");
    }

    // The fee RPCs serve the history the receipt came from.
    let history: Value = t
        .rpc
        .call(
            "eth_feeHistory",
            jsonrpsee::rpc_params![json!("0x2"), json!("latest"), json!([50.0])],
        )
        .await
        .result
        .map_err(|e| anyhow::anyhow!("eth_feeHistory: {e}"))?;
    let base_fees = history
        .get("baseFeePerGas")
        .and_then(Value::as_array)
        .context("feeHistory has no baseFeePerGas")?;
    if base_fees.len() != 3 {
        bail!(
            "feeHistory of two blocks has {} base fees, not 3",
            base_fees.len()
        );
    }
    let suggested: U256 = t
        .rpc
        .call("eth_maxPriorityFeePerGas", jsonrpsee::rpc_params![])
        .await
        .result
        .map_err(|e| anyhow::anyhow!("eth_maxPriorityFeePerGas: {e}"))?;
    if suggested > U256::from(INITIAL_BASE_FEE) {
        bail!("suggested tip {suggested} is above the standard price");
    }

    // One wei per gas is under every base fee of the schedule for
    // thousands of blocks. The sequencer refuses the offer on the feed.
    let cheap = l2::sign_priced_transfer(&signer, t.chain_id, 1, payee, 1)?;
    match t.rpc.send_raw(&cheap.raw).await.result {
        Err(RpcError::Call { code, message })
            if code == CODE_TIMEOUT && message.contains("less than block base fee") => {}
        other => bail!("a transfer under the base fee must fail with FeeTooLow, got {other:?}"),
    }

    // The rejected transfer held no nonce slot: the same nonce lands.
    let again = l2::sign_transfer(&signer, t.chain_id, 1, payee, 2)?;
    t.rpc
        .send_raw(&again.raw)
        .await
        .result
        .map_err(|e| anyhow::anyhow!("transfer after the rejection: {e}"))?;
    await_l2_receipt(t, again.hash, "transfer after the rejection").await?;
    Ok(())
}
