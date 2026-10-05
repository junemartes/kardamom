//! The faults, and how each one changes a JSON-RPC reply.

use alloy_primitives::Address;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One lie an L1 endpoint can tell. Each variant asks the same question
/// of a follower: does it notice?
///
/// The JSON form carries the variant in `kind`, with the parameters
/// beside it: `{"kind": "WrongBlockHash", "from_block": 100}`,
/// `{"kind": "SwallowLogs", "address": "0x..."}`, `{"kind": "Down"}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Fault {
    /// Serve L1 faithfully. The baseline: a follower must still work
    /// through an interposed endpoint, or the fault cases prove nothing.
    None,
    /// Corrupt the `hash` of every block at or above `from_block`. The
    /// block's own fields stay true, so a follower that chains blocks by
    /// their parent hashes sees the lie one block late.
    WrongBlockHash { from_block: u64 },
    /// Corrupt only `parentHash`, and leave each block's own hash intact.
    /// Each block looks right on its own. Only chaining consecutive
    /// blocks catches this, which is the reason a follower does it.
    BrokenParentChain { from_block: u64 },
    /// Drop every log of `address` from an `eth_getLogs` reply. The
    /// censorship case, seen from the L1 side: a settlement whose posts
    /// vanish, or a lockbox whose deposits do.
    SwallowLogs { address: Address },
    /// Answer `null` for every receipt in a block at or above
    /// `from_block`. A sender that waits for its own receipt waits
    /// forever.
    NullReceipts { from_block: u64 },
    /// Answer HTTP 429 to every call.
    RateLimit,
    /// Answer HTTP 503 to every call, and close the connection.
    Down,
}

/// The HTTP answer that replaces a forwarded call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    RateLimited,
    Down,
}

/// The active faults. Several lies at once model one bad endpoint: one
/// URL serves a wrong hash, null receipts and swallowed logs together.
/// `Fault::None` in the list is a no-op.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Faults(Vec<Fault>);

impl Faults {
    /// One active fault. `Fault::None` gives the empty list.
    #[must_use]
    pub fn one(fault: Fault) -> Self {
        Self(vec![fault]).without_none()
    }

    /// The list without its no-op entries, so an empty list means
    /// "faithful" in the control endpoint's answer.
    #[must_use]
    pub fn without_none(self) -> Self {
        Self(self.0.into_iter().filter(|f| *f != Fault::None).collect())
    }

    /// Whether the list is empty: the proxy serves L1 faithfully.
    #[must_use]
    pub fn is_faithful(&self) -> bool {
        self.0.is_empty()
    }

    /// The answer that replaces every forwarded call while a `Down` or a
    /// `RateLimit` fault is active. `Down` wins: an endpoint that is
    /// down cannot rate-limit.
    #[must_use]
    pub fn refusal(&self) -> Option<Refusal> {
        if self.0.contains(&Fault::Down) {
            return Some(Refusal::Down);
        }
        self.0
            .contains(&Fault::RateLimit)
            .then_some(Refusal::RateLimited)
    }

    /// Change the `result` of one `method` call as every active fault
    /// says, in list order.
    pub fn apply(&self, method: &str, result: &mut Value) {
        self.0.iter().for_each(|f| f.apply(method, result));
    }
}

/// The corrupted hash a `WrongBlockHash` fault serves.
fn wrong_hash() -> Value {
    Value::String(format!("0x{}", "ee".repeat(32)))
}

/// The corrupted parent a `BrokenParentChain` fault serves.
fn wrong_parent() -> Value {
    Value::String(format!("0x{}", "ab".repeat(32)))
}

impl Fault {
    /// Change the `result` of one `method` call as this fault says.
    fn apply(self, method: &str, result: &mut Value) {
        match self {
            Self::WrongBlockHash { from_block }
                if is_block_call(method) && at_or_after(result, "number", from_block) =>
            {
                result["hash"] = wrong_hash();
            }
            Self::BrokenParentChain { from_block }
                if is_block_call(method) && at_or_after(result, "number", from_block) =>
            {
                result["parentHash"] = wrong_parent();
            }
            Self::SwallowLogs { address } if method == "eth_getLogs" => {
                swallow(result, address);
            }
            Self::NullReceipts { from_block }
                if method == "eth_getTransactionReceipt"
                    && at_or_after(result, "blockNumber", from_block) =>
            {
                *result = Value::Null;
            }
            Self::NullReceipts { from_block }
                if method == "eth_getBlockReceipts" && first_at_or_after(result, from_block) =>
            {
                *result = Value::Null;
            }
            _ => {}
        }
    }
}

/// Whether `method` answers with a block object.
fn is_block_call(method: &str) -> bool {
    matches!(method, "eth_getBlockByNumber" | "eth_getBlockByHash")
}

/// Whether `result[field]`, a hex quantity, is at or after `from_block`.
fn at_or_after(result: &Value, field: &str, from_block: u64) -> bool {
    result
        .get(field)
        .and_then(Value::as_str)
        .and_then(|h| u64::from_str_radix(h.trim_start_matches("0x"), 16).ok())
        .is_some_and(|n| n >= from_block)
}

/// Whether the first receipt of a block-receipts reply is at or after
/// `from_block`. Every receipt of the reply is in the same block.
fn first_at_or_after(result: &Value, from_block: u64) -> bool {
    result
        .as_array()
        .and_then(|receipts| receipts.first())
        .is_some_and(|first| at_or_after(first, "blockNumber", from_block))
}

/// Drop every log of `address` from a logs reply.
fn swallow(result: &mut Value, address: Address) {
    if let Some(logs) = result.as_array_mut() {
        logs.retain(|log| log_address(log) != Some(address));
    }
}

/// The `address` of one log, when it parses.
fn log_address(log: &Value) -> Option<Address> {
    log.get("address")
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok())
}

#[cfg(test)]
mod tests;
