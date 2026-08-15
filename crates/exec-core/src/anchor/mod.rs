//! Witness MPT anchoring (spec: no-std-exec-core, phase 3b).
//!
//! Phase 2's witness is fail-closed but UNANCHORED: `WitnessDb` refuses
//! reads the witness doesn't carry, but nothing ties what it DOES carry to
//! the chain's actual pre-state — a prover could witness a fictional state
//! and prove a fictional, internally consistent block. This module makes
//! the witness self-authenticating against `pre_state_root` and closes the
//! loop with a post-state root, giving the proof its public outputs:
//! `(pre_state_root, post_state_root, bal_commitment, block_number)` — an
//! inductive root chain from genesis.
//!
//! - [`verify_witness_anchored`] runs BEFORE the first EVM step: every
//!   witness account and slot is proven present (with exactly the witnessed
//!   value) or absent under `pre_state_root`, by walking the carried node
//!   set from the root. Reaching the leaf IS the inclusion proof; reaching
//!   a divergence is the exclusion proof. Code needs no proof —
//!   `keccak256(bytes) == code_hash` and the hash sits inside a proven
//!   account leaf.
//! - [`recompute_post_root`] runs AFTER execution: apply the block's merged
//!   delta to the partial trie and re-hash. Correct IFF every node the
//!   delta's writes restructure is present — reads carry their own paths;
//!   deletion-collapse siblings are the capture fixed point's job, and a
//!   gap surfaces as a precise [`AnchorError::MissingNode`].
//!
//! Everything here is `no_std`: the guest runs this exact code.

mod sparse;

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_rlp::Decodable;
use alloy_trie::nodes::{RlpNode, TrieNode};
use alloy_trie::{EMPTY_ROOT_HASH, KECCAK_EMPTY, TrieAccount};
use kardamom_types::{ExecutionWitness, WitnessProofs};

use crate::delta::PendingDelta;
use crate::error::ExecutorError;

pub use sparse::{Lookup, SparseTrie};

/// Why a witness could not be anchored. Converted into
/// [`ExecutorError::WitnessUnanchored`] at the driver boundary;
/// [`MissingNode`](AnchorError::MissingNode) additionally drives the
/// capture-side fixed point, which matches on it BY NAME.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnchorError {
    /// The witness carries no `pre_state_root` to anchor against.
    NoPreStateRoot,
    /// The node set is not in canonical wire form (sorted by hash, unique).
    ProofSetNotCanonical,
    /// A walk needed a node the set does not carry. On the capture side
    /// this is the fixed point's work item — `path` is the node's nibble
    /// position (what a live-trie proof retainer targets; hashes are not
    /// addressable there), and `account` names the storage trie it belongs
    /// to (`None` = the account trie). In the guest it is fatal.
    MissingNode {
        hash: B256,
        path: alloy_trie::Nibbles,
        account: Option<Address>,
    },
    /// A carried node failed to RLP-decode.
    NodeDecode { hash: B256 },
    /// The trie refutes a witness entry (wrong value, or present-vs-absent
    /// disagreement). The witness or the root is lying.
    Refuted { what: String },
    /// Structurally impossible shape for a secure (fixed-width-key) trie —
    /// a malformed node set, not a state disagreement.
    Malformed(&'static str),
    /// The delta empties an account that existed pre-state. Live execution
    /// cannot produce this (selfdestruct is specced out; draining a balance
    /// bumps the sender's nonce), so v0 fails closed instead of carrying
    /// account-trie deletion machinery. Documented in the spec.
    AccountDeleteUnsupported { address: Address },
    /// The delta writes state for an account the witness never read —
    /// impossible under first-touch capture, so the witness is incomplete.
    WriteWithoutRead { address: Address },
}

impl From<AnchorError> for ExecutorError {
    fn from(e: AnchorError) -> Self {
        ExecutorError::WitnessUnanchored(format!("{e:?}"))
    }
}

/// Content-addressed view over a [`WitnessProofs`] node set: every node
/// keyed by `keccak256(node bytes)`. Construction enforces the canonical
/// wire form so there is exactly one valid encoding of a given set.
pub struct NodeStore<'p> {
    nodes: BTreeMap<B256, &'p [u8]>,
}

impl<'p> NodeStore<'p> {
    pub fn new(proofs: &'p WitnessProofs) -> Result<Self, AnchorError> {
        let mut nodes = BTreeMap::new();
        let mut prev: Option<B256> = None;
        for raw in &proofs.nodes {
            let hash = keccak256(raw);
            if let Some(p) = prev
                && p >= hash
            {
                return Err(AnchorError::ProofSetNotCanonical);
            }
            prev = Some(hash);
            nodes.insert(hash, raw.as_ref());
        }
        Ok(Self { nodes })
    }

    /// Expand the node reference at nibble position `at`: inline references
    /// decode in place; hash references are fetched (verified by
    /// construction — the key IS the hash) and decoded. A miss names both
    /// the hash and `at`, the retainer-addressable half.
    pub(crate) fn resolve(
        &self,
        r: &RlpNode,
        at: &alloy_trie::Nibbles,
    ) -> Result<TrieNode, AnchorError> {
        let (bytes, hash) = match r.as_hash() {
            Some(h) => match self.nodes.get(&h) {
                Some(b) => (*b, h),
                None => {
                    return Err(AnchorError::MissingNode {
                        hash: h,
                        path: *at,
                        account: None,
                    });
                }
            },
            None => (r.as_slice(), B256::ZERO),
        };
        let mut slice = bytes;
        TrieNode::decode(&mut slice).map_err(|_| AnchorError::NodeDecode { hash })
    }
}

/// Stamp `account` onto a storage-trie walk's [`AnchorError::MissingNode`]
/// so the capture side knows WHICH trie to target.
fn in_storage_trie(account: Address, e: AnchorError) -> AnchorError {
    match e {
        AnchorError::MissingNode {
            hash,
            path,
            account: None,
        } => AnchorError::MissingNode {
            hash,
            path,
            account: Some(account),
        },
        other => other,
    }
}

/// The pre-state the anchor walk PROVED, per witnessed account: `Some` with
/// the exact trie leaf for included accounts, `None` for proven-absent.
/// Feeds the post-root recompute (pre storage roots, untouched fields).
pub struct ProvenPre {
    pub accounts: BTreeMap<Address, Option<TrieAccount>>,
}

/// Verify every witness entry against `pre_state_root` over the carried
/// node set. Returns the proven pre-state on success. Fail-closed: any
/// missing node, undecodable node, or disagreement aborts.
pub fn verify_witness_anchored(
    witness: &ExecutionWitness,
    proofs: &WitnessProofs,
) -> Result<ProvenPre, AnchorError> {
    let root = witness.pre_state_root.ok_or(AnchorError::NoPreStateRoot)?;
    let store = NodeStore::new(proofs)?;
    let mut accounts_trie = SparseTrie::new(root, &store);
    let mut proven: BTreeMap<Address, Option<TrieAccount>> = BTreeMap::new();

    for acct in &witness.accounts {
        let leaf = accounts_trie.lookup(keccak256(acct.address))?;
        match leaf {
            Lookup::Found(value) => {
                let mut slice = value.as_slice();
                let ta = TrieAccount::decode(&mut slice)
                    .map_err(|_| AnchorError::Malformed("account leaf not a TrieAccount"))?;
                if !acct.exists {
                    return Err(AnchorError::Refuted {
                        what: format!("account {} witnessed absent but included", acct.address),
                    });
                }
                if ta.nonce != acct.nonce
                    || ta.balance != acct.balance
                    || ta.code_hash != acct.code_hash
                {
                    return Err(AnchorError::Refuted {
                        what: format!("account {} fields diverge from trie leaf", acct.address),
                    });
                }
                proven.insert(acct.address, Some(ta));
            }
            Lookup::Absent => {
                if acct.exists {
                    return Err(AnchorError::Refuted {
                        what: format!("account {} witnessed present but excluded", acct.address),
                    });
                }
                proven.insert(acct.address, None);
            }
        }
    }

    // Storage tries are walked per account, from the PROVEN storage root —
    // never from anything the witness claims directly.
    let mut storage_tries: BTreeMap<Address, SparseTrie<'_, '_>> = BTreeMap::new();
    for slot in &witness.storage {
        let Some(pre) = proven.get(&slot.address) else {
            return Err(AnchorError::Refuted {
                what: format!("slot under unwitnessed account {}", slot.address),
            });
        };
        let sroot = pre.map(|ta| ta.storage_root).unwrap_or(EMPTY_ROOT_HASH);
        let trie = storage_tries
            .entry(slot.address)
            .or_insert_with(|| SparseTrie::new(sroot, &store));
        match trie
            .lookup(keccak256(slot.key))
            .map_err(|e| in_storage_trie(slot.address, e))?
        {
            Lookup::Found(value) => {
                let mut slice = value.as_slice();
                let got = U256::decode(&mut slice)
                    .map_err(|_| AnchorError::Malformed("storage leaf not an RLP word"))?;
                if got != slot.value || slot.value.is_zero() {
                    return Err(AnchorError::Refuted {
                        what: format!(
                            "slot {}/{} value diverges from trie leaf",
                            slot.address, slot.key
                        ),
                    });
                }
            }
            Lookup::Absent => {
                if !slot.value.is_zero() {
                    return Err(AnchorError::Refuted {
                        what: format!(
                            "slot {}/{} witnessed non-zero but excluded",
                            slot.address, slot.key
                        ),
                    });
                }
            }
        }
    }

    // Code integrity: recompute every carried blob's hash. Its BINDING to
    // state is the account leaf's code_hash, already proven above.
    for entry in &witness.code {
        if keccak256(&entry.code) != entry.code_hash {
            return Err(AnchorError::Refuted {
                what: format!("code blob does not hash to {}", entry.code_hash),
            });
        }
    }

    Ok(ProvenPre { accounts: proven })
}

/// Apply the block's merged delta to the anchored partial trie and re-hash:
/// the post-state root, computed from `pre_state_root` + the node set + the
/// delta alone.
///
/// Storage first (each touched account's storage trie → new storage root),
/// then the account trie (changed fields from the delta, untouched fields
/// from the PROVEN pre leaf, storage roots from step one). EIP-161 mirror
/// of the live trie: an account whose post-state is empty is absent — a
/// no-op for accounts that were already absent, and fail-closed
/// [`AnchorError::AccountDeleteUnsupported`] for pre-existing ones (live
/// execution cannot empty an existing account; see the error's docs).
pub fn recompute_post_root(
    witness: &ExecutionWitness,
    proofs: &WitnessProofs,
    pre: &ProvenPre,
    delta: &PendingDelta,
) -> Result<B256, AnchorError> {
    let root = witness.pre_state_root.ok_or(AnchorError::NoPreStateRoot)?;
    let store = NodeStore::new(proofs)?;

    // --- storage tries ---
    let mut storage_writes: BTreeMap<Address, Vec<(B256, U256)>> = BTreeMap::new();
    for ((addr, key), value) in &delta.storage {
        storage_writes
            .entry(*addr)
            .or_default()
            .push((*key, *value));
    }
    let mut new_storage_root: BTreeMap<Address, B256> = BTreeMap::new();
    for (addr, writes) in &storage_writes {
        let pre_acct = pre
            .accounts
            .get(addr)
            .ok_or(AnchorError::WriteWithoutRead { address: *addr })?;
        let sroot = pre_acct
            .map(|ta| ta.storage_root)
            .unwrap_or(EMPTY_ROOT_HASH);
        let mut trie = SparseTrie::new(sroot, &store);
        for (key, value) in writes {
            if value.is_zero() {
                trie.remove(keccak256(key))
                    .map_err(|e| in_storage_trie(*addr, e))?;
            } else {
                let mut rlp = Vec::new();
                alloy_rlp::Encodable::encode(value, &mut rlp);
                trie.insert(keccak256(key), rlp)
                    .map_err(|e| in_storage_trie(*addr, e))?;
            }
        }
        new_storage_root.insert(*addr, trie.root());
    }

    // --- account trie ---
    let mut accounts_trie = SparseTrie::new(root, &store);
    let mut touched: Vec<Address> = delta.accounts.keys().copied().collect();
    touched.extend(storage_writes.keys().copied());
    touched.sort_unstable();
    touched.dedup();
    for addr in touched {
        let pre_acct = pre
            .accounts
            .get(&addr)
            .ok_or(AnchorError::WriteWithoutRead { address: addr })?;
        let (nonce, balance, code_hash) = match delta.accounts.get(&addr) {
            Some(v) => *v,
            None => match pre_acct {
                Some(ta) => (ta.nonce, ta.balance, ta.code_hash),
                // Storage write to an account with no pre leaf and no
                // account-field change: the fields are all empty.
                None => (0, U256::ZERO, KECCAK_EMPTY),
            },
        };
        // The delta stores "no code" as ZERO in some write paths; the trie
        // leaf always uses KECCAK_EMPTY (mirrors the live writer's mapping).
        let code_hash = if code_hash == B256::ZERO {
            KECCAK_EMPTY
        } else {
            code_hash
        };
        let storage_root = new_storage_root
            .get(&addr)
            .copied()
            .or_else(|| pre_acct.map(|ta| ta.storage_root))
            .unwrap_or(EMPTY_ROOT_HASH);
        let post = TrieAccount {
            nonce,
            balance,
            storage_root,
            code_hash,
        };
        let empty = post.nonce == 0 && post.balance.is_zero() && post.code_hash == KECCAK_EMPTY;
        if empty && post.storage_root == EMPTY_ROOT_HASH {
            match pre_acct {
                // EIP-161: touched-but-empty never enters the trie.
                None => continue,
                Some(_) => {
                    return Err(AnchorError::AccountDeleteUnsupported { address: addr });
                }
            }
        }
        let mut rlp = Vec::new();
        alloy_rlp::Encodable::encode(&post, &mut rlp);
        accounts_trie.insert(keccak256(addr), rlp)?;
    }

    Ok(accounts_trie.root())
}
