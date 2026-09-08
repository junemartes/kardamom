//! Witness MPT anchoring.
//!
//! `WitnessDb` fails closed: it refuses reads the witness does not carry.
//! But nothing ties what it does carry to the chain's real pre-state. A
//! prover could witness a
//! fictional state and prove a fictional, but internally consistent,
//! block. This module makes the witness self-authenticating against
//! `pre_state_root`, and closes the loop with a post-state root. This
//! gives the proof its public outputs:
//! `(pre_state_root, post_state_root, bal_commitment, block_number)`, an
//! inductive root chain from genesis.
//!
//! - [`verify_witness_anchored`] runs before the first EVM step. It proves
//!   every witness account and slot present (with exactly the witnessed
//!   value) or absent, under `pre_state_root`, by walking the carried node
//!   set from the root. Reaching the leaf is the inclusion proof; reaching
//!   a divergence is the exclusion proof. Code needs no proof: it checks
//!   `keccak256(bytes) == code_hash`, and the hash sits inside a proven
//!   account leaf.
//! - [`recompute_post_root`] runs after execution. It applies the block's
//!   merged delta to the partial trie and re-hashes. This is correct only
//!   if every node the delta's writes restructure is present. Reads carry
//!   their own paths. Deletion-collapse siblings are the capture fixed
//!   point's job, and a gap surfaces as a precise
//!   [`AnchorError::MissingNode`].
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
use kardamom_types::{ExecutionWitness, WitnessAccount, WitnessProofs, WitnessSlot};

use crate::delta::PendingDelta;
use crate::error::ExecutorError;

pub use sparse::{Lookup, SparseTrie};

/// Why a witness could not be anchored. This converts into
/// [`ExecutorError::WitnessUnanchored`] at the driver boundary.
/// [`MissingNode`](AnchorError::MissingNode) also drives the capture-side
/// fixed point, which matches on it by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnchorError {
    /// The witness carries no `pre_state_root` to anchor against.
    NoPreStateRoot,
    /// The node set is not in canonical wire form (sorted by hash, unique).
    ProofSetNotCanonical,
    /// A walk needed a node the set does not carry. On the capture side,
    /// this is the fixed point's work item. `path` is the node's nibble
    /// position (what a live-trie proof retainer targets; hashes are not
    /// addressable there). `account` names the storage trie it belongs
    /// to (`None` means the account trie). In the guest, this is fatal.
    MissingNode {
        hash: B256,
        path: alloy_trie::Nibbles,
        account: Option<Address>,
    },
    /// A carried node failed to RLP-decode.
    NodeDecode { hash: B256 },
    /// The trie refutes a witness entry (a wrong value, or a
    /// present-versus-absent disagreement). The witness or the root is
    /// lying.
    Refuted { what: String },
    /// A shape that is structurally impossible for a secure
    /// (fixed-width-key) trie. This is a malformed node set, not a state
    /// disagreement.
    Malformed(&'static str),
    /// The delta empties an account that existed pre-state. Live
    /// execution cannot produce this (selfdestruct is specced out, and
    /// draining a balance bumps the sender's nonce), so version 0 fails
    /// closed instead of carrying account-trie deletion machinery. This
    /// is documented in the spec.
    AccountDeleteUnsupported { address: Address },
    /// The delta writes state for an account the witness never read.
    /// This is impossible under first-touch capture, so the witness is
    /// incomplete.
    WriteWithoutRead { address: Address },
}

impl From<AnchorError> for ExecutorError {
    fn from(e: AnchorError) -> Self {
        ExecutorError::WitnessUnanchored(format!("{e:?}"))
    }
}

/// A content-addressed view over a [`WitnessProofs`] node set. Every node
/// is keyed by `keccak256(node bytes)`. Construction enforces the
/// canonical wire form, so there is exactly one valid encoding of a
/// given set.
pub struct NodeStore<'p> {
    nodes: BTreeMap<B256, &'p [u8]>,
}

impl<'p> NodeStore<'p> {
    /// # Errors
    ///
    /// Returns [`AnchorError::ProofSetNotCanonical`] when `proofs.nodes` is
    /// not sorted by hash with no duplicate.
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

    /// Expand the node reference at nibble position `at`. An inline
    /// reference decodes in place. A hash reference is fetched (verified
    /// by construction, since the key is the hash) and decoded. A miss
    /// names both the hash and `at`, the retainer-addressable half.
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

/// Stamp `account` onto a storage-trie walk's [`AnchorError::MissingNode`],
/// so the capture side knows which trie to target.
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

/// The pre-state the anchor walk proved, per witnessed account. `Some`
/// carries the exact trie leaf for an included account; `None` means
/// proven absent. This feeds the post-root recompute (pre storage roots,
/// untouched fields). `root` is the `pre_state_root` this was proven
/// against — [`verify_witness_anchored`] already required it to be
/// `Some`, so downstream callers read it here instead of re-deriving it
/// from the witness (and re-handling the `None` case) a second time.
pub struct ProvenPre {
    pub(crate) root: B256,
    pub(crate) accounts: BTreeMap<Address, Option<TrieAccount>>,
}

/// One witness, checked against its own carried node set. Bundles the
/// three things every verification step needs (`witness`, `store`,
/// `root`) so [`WitnessAnchor::verify`] and its steps read them as state
/// instead of re-threading them through every call.
struct WitnessAnchor<'a> {
    witness: &'a ExecutionWitness,
    store: NodeStore<'a>,
    root: B256,
}

impl<'a> WitnessAnchor<'a> {
    fn new(witness: &'a ExecutionWitness, proofs: &'a WitnessProofs) -> Result<Self, AnchorError> {
        let root = witness.pre_state_root.ok_or(AnchorError::NoPreStateRoot)?;
        let store = NodeStore::new(proofs)?;
        Ok(Self {
            witness,
            store,
            root,
        })
    }

    /// Verify every witness entry against `pre_state_root`, over the
    /// carried node set. Returns the proven pre-state on success. This
    /// fails closed: any missing node, undecodable node, or disagreement
    /// aborts.
    fn verify(&self) -> Result<ProvenPre, AnchorError> {
        let mut accounts_trie = SparseTrie::new(self.root, &self.store);
        let accounts = self.prove_accounts(&mut accounts_trie)?;
        self.prove_storage(&accounts)?;
        self.verify_code_blobs()?;
        Ok(ProvenPre {
            root: self.root,
            accounts,
        })
    }

    /// Prove every witness account present (with the exact witnessed
    /// value) or absent, under the account trie's root. Returns the
    /// proven pre-state leaf per account.
    fn prove_accounts(
        &self,
        accounts_trie: &mut SparseTrie<'_, '_>,
    ) -> Result<BTreeMap<Address, Option<TrieAccount>>, AnchorError> {
        let mut proven: BTreeMap<Address, Option<TrieAccount>> = BTreeMap::new();
        for acct in &self.witness.accounts {
            let leaf = accounts_trie.lookup(keccak256(acct.address))?;
            proven.insert(acct.address, Self::prove_one_account(acct, leaf)?);
        }
        Ok(proven)
    }

    /// Prove one witness account against its trie lookup. The loop in
    /// [`Self::prove_accounts`] stays free of a branch.
    fn prove_one_account(
        acct: &WitnessAccount,
        leaf: Lookup,
    ) -> Result<Option<TrieAccount>, AnchorError> {
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
                // The same normalization rule applies at the anchor. The
                // state table stores "no code" as ZERO, but the trie leaf
                // always uses KECCAK_EMPTY, and execution treats the two
                // identically. So the witness (a capture of table reads)
                // compares under the same mapping the recompute already
                // writes with.
                let witness_code_hash = crate::code_hash::to_revm_code_hash(acct.code_hash);
                if ta.nonce != acct.nonce
                    || ta.balance != acct.balance
                    || ta.code_hash != witness_code_hash
                {
                    return Err(AnchorError::Refuted {
                        what: format!("account {} fields diverge from trie leaf", acct.address),
                    });
                }
                Ok(Some(ta))
            }
            Lookup::Absent => {
                // The state table may keep an EIP-161-empty account as a
                // row (a touched, zero-fee coinbase is the live shape),
                // while the trie rightly excludes it. Execution semantics
                // treat empty and absent the same, so the anchor does
                // too: a witnessed-but-empty account is consistent with
                // exclusion. Anything non-empty witnessed as present
                // still gets refuted.
                let empty =
                    crate::code_hash::is_empty_account(acct.nonce, acct.balance, acct.code_hash);
                if acct.exists && !empty {
                    return Err(AnchorError::Refuted {
                        what: format!("account {} witnessed present but excluded", acct.address),
                    });
                }
                Ok(None)
            }
        }
    }

    /// Prove every witness storage slot against its account's proven
    /// storage root. Storage tries are walked per account, from the
    /// proven storage root, never from anything the witness claims
    /// directly.
    fn prove_storage(
        &self,
        proven: &BTreeMap<Address, Option<TrieAccount>>,
    ) -> Result<(), AnchorError> {
        let mut storage_tries: BTreeMap<Address, SparseTrie<'_, '_>> = BTreeMap::new();
        for slot in &self.witness.storage {
            let Some(pre) = proven.get(&slot.address) else {
                return Err(AnchorError::Refuted {
                    what: format!("slot under unwitnessed account {}", slot.address),
                });
            };
            let sroot = pre.map_or(EMPTY_ROOT_HASH, |ta| ta.storage_root);
            let trie = storage_tries
                .entry(slot.address)
                .or_insert_with(|| SparseTrie::new(sroot, &self.store));
            let lookup = trie
                .lookup(keccak256(slot.key))
                .map_err(|e| in_storage_trie(slot.address, e))?;
            Self::check_one_slot(slot, lookup)?;
        }
        Ok(())
    }

    /// Check one witness storage slot against its trie lookup. The loop
    /// in [`Self::prove_storage`] stays free of a branch.
    fn check_one_slot(slot: &WitnessSlot, lookup: Lookup) -> Result<(), AnchorError> {
        match lookup {
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
        Ok(())
    }

    /// Recompute every carried code blob's hash. Its binding to state is
    /// the account leaf's `code_hash`, already proven by
    /// [`WitnessAnchor::prove_accounts`].
    fn verify_code_blobs(&self) -> Result<(), AnchorError> {
        for entry in &self.witness.code {
            Self::check_one_code_blob(entry)?;
        }
        Ok(())
    }

    /// Check one carried code blob's hash. The loop in
    /// [`Self::verify_code_blobs`] stays free of a branch.
    fn check_one_code_blob(entry: &kardamom_types::delta::CodeEntry) -> Result<(), AnchorError> {
        if keccak256(&entry.code) != entry.code_hash {
            return Err(AnchorError::Refuted {
                what: format!("code blob does not hash to {}", entry.code_hash),
            });
        }
        Ok(())
    }
}

/// Verify every witness entry against `pre_state_root`, over the carried
/// node set. Returns the proven pre-state on success. This fails closed:
/// any missing node, undecodable node, or disagreement aborts.
///
/// # Errors
///
/// Returns an [`AnchorError`] when the witness carries no
/// `pre_state_root`, the node set is not canonical, a walk needs a node
/// the set does not carry, a node fails to decode, or a witnessed value
/// disagrees with the trie.
pub fn verify_witness_anchored(
    witness: &ExecutionWitness,
    proofs: &WitnessProofs,
) -> Result<ProvenPre, AnchorError> {
    WitnessAnchor::new(witness, proofs)?.verify()
}

/// Apply the block's merged delta to the anchored partial trie, and
/// re-hash: the post-state root, computed from `pre.root` (the proven
/// pre-state root [`verify_witness_anchored`] already produced), the
/// node set, and the delta alone.
///
/// This processes storage first (each touched account's storage trie
/// becomes a new storage root), then the account trie (changed fields
/// from the delta, untouched fields from the proven pre leaf, storage
/// roots from the first step). This mirrors the live trie's EIP-161
/// rule: an account whose post-state is empty is absent. This is a
/// no-op for accounts that were already absent, and fails closed with
/// [`AnchorError::AccountDeleteUnsupported`] for pre-existing ones (live
/// execution cannot empty an existing account; see that error's docs).
///
/// # Errors
///
/// Returns an [`AnchorError`] when a touched account or slot has no
/// proven pre leaf, a walk needs a node the set does not carry, or the
/// delta empties an account that existed pre-state.
pub fn recompute_post_root(
    proofs: &WitnessProofs,
    pre: &ProvenPre,
    delta: &PendingDelta,
) -> Result<B256, AnchorError> {
    let store = NodeStore::new(proofs)?;

    let storage_writes = ProvenPre::group_storage_writes(delta);
    let new_storage_root = pre.recompute_storage_roots(&store, &storage_writes)?;

    let mut accounts_trie = SparseTrie::new(pre.root, &store);
    let touched: alloc::collections::BTreeSet<Address> = delta
        .accounts
        .keys()
        .chain(storage_writes.keys())
        .copied()
        .collect();
    for addr in touched {
        let Some(post) = pre.post_account_leaf(addr, delta, &new_storage_root)? else {
            // EIP-161: touched-but-empty never enters the trie.
            continue;
        };
        let mut rlp = Vec::new();
        alloy_rlp::Encodable::encode(&post, &mut rlp);
        accounts_trie.insert(keccak256(addr), rlp)?;
    }

    Ok(accounts_trie.root())
}

impl ProvenPre {
    /// Group the block delta's storage writes by account, in preparation
    /// for the per-account storage-trie recompute.
    fn group_storage_writes(delta: &PendingDelta) -> BTreeMap<Address, Vec<(B256, U256)>> {
        delta
            .storage
            .iter()
            .fold(BTreeMap::new(), |mut writes, ((addr, key), value)| {
                writes.entry(*addr).or_default().push((*key, *value));
                writes
            })
    }

    /// Apply each touched account's storage writes to its proven pre
    /// storage trie, and return the new root per account.
    fn recompute_storage_roots(
        &self,
        store: &NodeStore<'_>,
        storage_writes: &BTreeMap<Address, Vec<(B256, U256)>>,
    ) -> Result<BTreeMap<Address, B256>, AnchorError> {
        let mut new_storage_root = BTreeMap::new();
        for (addr, writes) in storage_writes {
            let pre_acct = self
                .accounts
                .get(addr)
                .ok_or(AnchorError::WriteWithoutRead { address: *addr })?;
            let sroot = pre_acct.map_or(EMPTY_ROOT_HASH, |ta| ta.storage_root);
            let mut trie = SparseTrie::new(sroot, store);
            Self::apply_storage_writes(&mut trie, *addr, writes)?;
            new_storage_root.insert(*addr, trie.root());
        }
        Ok(new_storage_root)
    }

    /// Apply one account's storage writes to its trie, in place: zero
    /// clears the slot, nonzero upserts it. The inner loop of
    /// [`ProvenPre::recompute_storage_roots`]'s per-account walk, split
    /// out so that walk stays a single loop over accounts.
    fn apply_storage_writes(
        trie: &mut SparseTrie<'_, '_>,
        addr: Address,
        writes: &[(B256, U256)],
    ) -> Result<(), AnchorError> {
        for (key, value) in writes {
            Self::apply_one_storage_write(trie, addr, *key, *value)?;
        }
        Ok(())
    }

    /// Clear or upsert one storage slot in `trie`. The loop in
    /// [`Self::apply_storage_writes`] stays free of a branch.
    fn apply_one_storage_write(
        trie: &mut SparseTrie<'_, '_>,
        addr: Address,
        key: B256,
        value: U256,
    ) -> Result<(), AnchorError> {
        if value.is_zero() {
            trie.remove(keccak256(key))
                .map_err(|e| in_storage_trie(addr, e))
        } else {
            let mut rlp = Vec::new();
            alloy_rlp::Encodable::encode(&value, &mut rlp);
            trie.insert(keccak256(key), rlp)
                .map_err(|e| in_storage_trie(addr, e))
        }
    }

    /// Build one account's post-state trie leaf from the delta's field
    /// changes (falling back to the proven pre leaf for untouched
    /// fields) and its recomputed storage root (falling back to the
    /// proven pre storage root, or the empty root, when `addr` had no
    /// storage write). Returns `None` when the post-state is
    /// EIP-161-empty and the account did not exist pre-state (it never
    /// enters the trie). An empty post-state for a pre-existing account
    /// fails closed with [`AnchorError::AccountDeleteUnsupported`].
    ///
    /// # Errors
    ///
    /// Returns [`AnchorError::WriteWithoutRead`] when `addr` has no
    /// proven pre leaf, and [`AnchorError::AccountDeleteUnsupported`]
    /// when the delta empties a pre-existing account.
    fn post_account_leaf(
        &self,
        addr: Address,
        delta: &PendingDelta,
        new_storage_root: &BTreeMap<Address, B256>,
    ) -> Result<Option<TrieAccount>, AnchorError> {
        let pre_acct = self
            .accounts
            .get(&addr)
            .ok_or(AnchorError::WriteWithoutRead { address: addr })?;
        let storage_root = new_storage_root
            .get(&addr)
            .copied()
            .or_else(|| pre_acct.map(|ta| ta.storage_root))
            .unwrap_or(EMPTY_ROOT_HASH);
        let crate::delta::AccountFields {
            nonce,
            balance,
            code_hash,
        } = match delta.accounts.get(&addr) {
            Some(v) => *v,
            None => match pre_acct {
                Some(ta) => crate::delta::AccountFields {
                    nonce: ta.nonce,
                    balance: ta.balance,
                    code_hash: ta.code_hash,
                },
                // Storage write to an account with no pre leaf, and no
                // account-field change. All the fields are empty.
                None => crate::delta::AccountFields {
                    nonce: 0,
                    balance: U256::ZERO,
                    code_hash: KECCAK_EMPTY,
                },
            },
        };
        // The delta stores "no code" as ZERO in some write paths. The
        // trie leaf always uses KECCAK_EMPTY, mirroring the live
        // writer's mapping.
        let code_hash = crate::code_hash::to_revm_code_hash(code_hash);
        let post = TrieAccount {
            nonce,
            balance,
            storage_root,
            code_hash,
        };
        let empty = crate::code_hash::is_empty_account(post.nonce, post.balance, post.code_hash);
        if empty && post.storage_root == EMPTY_ROOT_HASH {
            return match pre_acct {
                None => Ok(None),
                Some(_) => Err(AnchorError::AccountDeleteUnsupported { address: addr }),
            };
        }
        Ok(Some(post))
    }
}
