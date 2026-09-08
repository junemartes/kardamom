//! A sparse Merkle-Patricia trie over a content-addressed node set.
//!
//! The trie starts as a single unresolved root hash, and expands nodes on
//! demand from the [`NodeStore`] as keys are read, inserted, or removed.
//! Untouched subtrees stay unresolved. Re-hashing reuses their `RlpNode`
//! verbatim, which makes the post-root recompute cost proportional to the
//! touched paths, not the whole state.
//!
//! Every resolved node is verified by construction. Resolution looks
//! nodes up by the exact hash the parent carries (the store is keyed by
//! `keccak256(node)`), so a malicious node set can only produce
//! [`AnchorError::MissingNode`] or a root mismatch. It can never smuggle
//! state.
//!
//! [`AnchorError::MissingNode`] names both the hash and the nibble
//! position on purpose. The capture-side fixed point resolves it by
//! re-running the live-trie walk with that position as a retainer target
//! (hashes are not addressable there), so this precision is part of the
//! design, not just diagnostics.
//!
//! [`NodeStore`]: super::NodeStore

use alloc::boxed::Box;
use alloc::vec::Vec;

use alloy_primitives::{B256, keccak256};
use alloy_trie::nodes::{BranchNode, ExtensionNode, LeafNode, RlpNode, TrieNode};
use alloy_trie::{EMPTY_ROOT_HASH, Nibbles, TrieMask};

use super::{AnchorError, NodeStore};

/// One sparse-trie node. `Unresolved` is a subtree the walk has not needed
/// yet, represented by the reference its parent carries (hash or inline).
enum Node {
    Unresolved(RlpNode),
    Leaf { key: Nibbles, value: Vec<u8> },
    Extension { key: Nibbles, child: Box<Node> },
    Branch { children: [Option<Box<Node>>; 16] },
}

impl Node {
    /// Expand an [`RlpNode`] reference into a structural node. Children
    /// stay unresolved. An inline reference (under 32 bytes) decodes in
    /// place; a hash reference goes through the store.
    fn resolve(r: &RlpNode, at: &Nibbles, store: &NodeStore<'_>) -> Result<Node, AnchorError> {
        let trie_node = store.resolve(r, at)?;
        Ok(match trie_node {
            // A child reference never points at the empty root. MPT
            // parents omit empty children entirely (the branch mask bit
            // stays unset).
            TrieNode::EmptyRoot => {
                return Err(AnchorError::Malformed("empty-root node as a child"));
            }
            TrieNode::Leaf(LeafNode { key, value }) => Node::Leaf { key, value },
            TrieNode::Extension(ExtensionNode { key, child }) => Node::Extension {
                key,
                child: Box::new(Node::Unresolved(child)),
            },
            TrieNode::Branch(b) => {
                let mut children: [Option<Box<Node>>; 16] = Default::default();
                let mut stack = b.stack.into_iter();
                for (i, slot) in children.iter_mut().enumerate() {
                    #[allow(
                        clippy::cast_possible_truncation,
                        reason = "`i` indexes a 16-element array: always < 16, fits u8"
                    )]
                    if b.state_mask.is_bit_set(i as u8) {
                        let r = stack
                            .next()
                            .ok_or(AnchorError::Malformed("branch mask exceeds stack"))?;
                        *slot = Some(Box::new(Node::Unresolved(r)));
                    }
                }
                Node::Branch { children }
            }
        })
    }

    fn resolved(self, at: &Nibbles, store: &NodeStore<'_>) -> Result<Node, AnchorError> {
        if let Node::Unresolved(ref r) = self {
            Node::resolve(r, at, store)
        } else {
            Ok(self)
        }
    }
}

/// What a key lookup proved. `Absent` is a real exclusion proof: the walk
/// reached the point where the key would live, and found something else.
pub enum Lookup {
    Found(Vec<u8>),
    Absent,
}

/// A sparse MPT rooted at a known hash, expanding through `store` on
/// demand.
///
/// All keys are fixed-width 32-byte words (a secure trie: callers pass
/// `keccak(address)` or `keccak(slot)`), so a path can never terminate at
/// a branch. Branch value slots are structurally empty in state and
/// storage tries, so hitting one means a malformed node set, not a
/// possible state shape.
pub struct SparseTrie<'s, 'p> {
    root: Option<Node>,
    store: &'s NodeStore<'p>,
}

impl<'s, 'p> SparseTrie<'s, 'p> {
    #[must_use]
    pub fn new(root: B256, store: &'s NodeStore<'p>) -> Self {
        let root_node = if root == EMPTY_ROOT_HASH {
            None
        } else {
            Some(Node::Unresolved(RlpNode::word_rlp(&root)))
        };
        Self {
            root: root_node,
            store,
        }
    }

    /// Prove `key` present (and return its value) or absent. This
    /// resolves only the nodes on the key's path.
    ///
    /// # Errors
    ///
    /// Returns [`AnchorError`] when the walk needs a node the store does
    /// not carry, a carried node fails to decode, or the node set is
    /// structurally malformed.
    pub fn lookup(&mut self, key: B256) -> Result<Lookup, AnchorError> {
        let walk = Walk::new(Nibbles::unpack(key), self.store);
        let Some(root) = self.root.as_mut() else {
            return Ok(Lookup::Absent);
        };
        walk.lookup_in(root, 0)
    }

    /// Insert or update `key` with `value` (non-empty RLP).
    ///
    /// # Errors
    ///
    /// Returns [`AnchorError`] when the walk needs a node the store does
    /// not carry, a carried node fails to decode, or the node set is
    /// structurally malformed.
    pub fn insert(&mut self, key: B256, value: Vec<u8>) -> Result<(), AnchorError> {
        let walk = Walk::new(Nibbles::unpack(key), self.store);
        let taken = self.root.take();
        self.root = Some(walk.insert_in(taken, 0, value)?);
        Ok(())
    }

    /// Remove `key`. Removing an absent key is a no-op, the same as
    /// writing zero to a slot that was already absent.
    ///
    /// # Errors
    ///
    /// Returns [`AnchorError`] when the walk needs a node the store does
    /// not carry, a carried node fails to decode, or the node set is
    /// structurally malformed.
    pub fn remove(&mut self, key: B256) -> Result<(), AnchorError> {
        let walk = Walk::new(Nibbles::unpack(key), self.store);
        let Some(root) = self.root.take() else {
            return Ok(());
        };
        self.root = walk.remove_in(root, 0)?;
        Ok(())
    }

    /// Re-hash the trie bottom-up. Untouched (unresolved) subtrees reuse
    /// the reference their parent carried, with no store access and no
    /// recompute.
    #[must_use]
    pub fn root(&self) -> B256 {
        match &self.root {
            None => EMPTY_ROOT_HASH,
            // Nothing was touched at all. The root hash is the reference
            // itself, not keccak of the 33-byte hash-string RLP.
            Some(Node::Unresolved(r)) => match r.as_hash() {
                Some(h) => h,
                // An inline reference is the node encoding (under 32
                // bytes, unreachable for secure tries, but keccak of it
                // is still correct).
                None => keccak256(r.as_slice()),
            },
            Some(node) => {
                let mut rlp = Vec::new();
                node.encode_node(&mut rlp);
                keccak256(&rlp)
            }
        }
    }
}

/// One lookup/insert/remove walk's invariants: the full key path, and the
/// node store to resolve through. Neither changes as the walk descends —
/// only the current node and its depth do — so `SparseTrie::lookup`/
/// `insert`/`remove` each build one of these and read `path`/`store` as
/// state instead of re-threading them through every recursive call.
struct Walk<'s, 'p> {
    path: Nibbles,
    store: &'s NodeStore<'p>,
}

impl<'s, 'p> Walk<'s, 'p> {
    fn new(path: Nibbles, store: &'s NodeStore<'p>) -> Self {
        Self { path, store }
    }

    fn lookup_in(&self, node: &mut Node, depth: usize) -> Result<Lookup, AnchorError> {
        if let Node::Unresolved(r) = node {
            *node = Node::resolve(r, &self.path.slice(..depth), self.store)?;
        }
        let rest = self.path.slice(depth..);
        match node {
            Node::Unresolved(_) => unreachable!("resolved above"),
            Node::Leaf { key, value } => {
                if *key == rest {
                    Ok(Lookup::Found(value.clone()))
                } else {
                    Ok(Lookup::Absent)
                }
            }
            Node::Extension { key, child } => {
                if rest.starts_with(key) {
                    self.lookup_in(child, depth + key.len())
                } else {
                    Ok(Lookup::Absent)
                }
            }
            Node::Branch { children } => {
                let Some(nibble) = rest.first() else {
                    return Err(AnchorError::Malformed("key exhausted at a branch"));
                };
                match children[nibble as usize].as_mut() {
                    Some(child) => self.lookup_in(child, depth + 1),
                    None => Ok(Lookup::Absent),
                }
            }
        }
    }

    fn insert_in(
        &self,
        node: Option<Node>,
        depth: usize,
        value: Vec<u8>,
    ) -> Result<Node, AnchorError> {
        let rest = self.path.slice(depth..);
        let mut node = match node {
            None => return Ok(Node::Leaf { key: rest, value }),
            Some(n) => n.resolved(&self.path.slice(..depth), self.store)?,
        };
        match &mut node {
            Node::Unresolved(_) => unreachable!("resolved above"),
            Node::Leaf {
                key,
                value: existing,
            } => {
                if *key == rest {
                    *existing = value;
                    return Ok(node);
                }
                Ok(Node::split_leaf(
                    key,
                    core::mem::take(existing),
                    &rest,
                    value,
                ))
            }
            Node::Extension { key, child } => {
                if rest.starts_with(key) {
                    let klen = key.len();
                    let taken = core::mem::replace(child.as_mut(), Node::placeholder());
                    **child = self.insert_in(Some(taken), depth + klen, value)?;
                    return Ok(node);
                }
                Ok(Node::split_extension(key, child, &rest, value))
            }
            Node::Branch { children } => {
                self.descend_branch(children, &rest, depth, value)?;
                Ok(node)
            }
        }
    }

    /// The child slot at `rest`'s next nibble already holds a subtree
    /// (or is empty). Insert below it, recursively, in place.
    fn descend_branch(
        &self,
        children: &mut [Option<Box<Node>>; 16],
        rest: &Nibbles,
        depth: usize,
        value: Vec<u8>,
    ) -> Result<(), AnchorError> {
        let Some(nibble) = rest.first() else {
            return Err(AnchorError::Malformed("key exhausted at a branch"));
        };
        let slot = &mut children[nibble as usize];
        let taken = slot.take().map(|b| *b);
        *slot = Some(Box::new(self.insert_in(taken, depth + 1, value)?));
        Ok(())
    }

    /// Remove inside `node`. `None` means the subtree vanished entirely.
    fn remove_in(&self, node: Node, depth: usize) -> Result<Option<Node>, AnchorError> {
        let node = node.resolved(&self.path.slice(..depth), self.store)?;
        let rest = self.path.slice(depth..);
        match node {
            Node::Unresolved(_) => unreachable!("resolved above"),
            Node::Leaf { key, value } => {
                if key == rest {
                    Ok(None)
                } else {
                    // Not this key. Removing an absent key is a no-op.
                    Ok(Some(Node::Leaf { key, value }))
                }
            }
            Node::Extension { key, child } => self.remove_under_extension(key, child, &rest, depth),
            Node::Branch { mut children } => {
                let Some(nibble) = rest.first() else {
                    return Err(AnchorError::Malformed("key exhausted at a branch"));
                };
                let idx = nibble as usize;
                match children[idx].take() {
                    None => {
                        // Removing an absent key: keep the branch untouched.
                        return Ok(Some(Node::Branch { children }));
                    }
                    Some(child) => {
                        if let Some(kept) = self.remove_in(*child, depth + 1)? {
                            children[idx] = Some(Box::new(kept));
                            return Ok(Some(Node::Branch { children }));
                        }
                    }
                }
                // The child vanished. Two or more survivors keep the
                // branch. Exactly one survivor collapses it. This is the
                // deletion shape whose sibling the capture fixed point
                // must have supplied.
                self.collapse_branch(children, depth)
            }
        }
    }

    /// Removal below an extension. A miss (the removed key does not share
    /// the extension's prefix) is a no-op.
    fn remove_under_extension(
        &self,
        key: Nibbles,
        child: Box<Node>,
        rest: &Nibbles,
        depth: usize,
    ) -> Result<Option<Node>, AnchorError> {
        if !rest.starts_with(&key) {
            return Ok(Some(Node::Extension { key, child }));
        }
        let klen = key.len();
        match self.remove_in(*child, depth + klen)? {
            None => Ok(None),
            Some(new_child) => Ok(Some(new_child.merge_extension(key))),
        }
    }

    /// A branch child just vanished. Two or more survivors keep the
    /// branch as-is. Exactly one survivor collapses it (see
    /// [`Self::splice_survivor`]). Zero survivors means the branch itself
    /// vanished.
    fn collapse_branch(
        &self,
        children: [Option<Box<Node>>; 16],
        depth: usize,
    ) -> Result<Option<Node>, AnchorError> {
        let mut survivors = children
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_some())
            .map(|(i, _)| i);
        let first = survivors.next();
        let second = survivors.next();
        match (first, second) {
            (None, _) => Ok(None),
            (Some(_), Some(_)) => Ok(Some(Node::Branch { children })),
            (Some(i), None) => Ok(Some(self.splice_survivor(children, i, depth)?)),
        }
    }

    /// Splice the lone surviving child up through nibble `i`, replacing
    /// its now-collapsed parent branch. Resolving it can demand an
    /// off-path node — this is the `MissingNode` the capture fixed point
    /// exists to feed. The splice itself is [`Node::merge_extension`]:
    /// once resolved, a survivor spliced up through one nibble is the
    /// same shape as a child merged up through an extension's key.
    fn splice_survivor(
        &self,
        mut children: [Option<Box<Node>>; 16],
        i: usize,
        depth: usize,
    ) -> Result<Node, AnchorError> {
        let survivor = children[i].take().expect("survivor indexed");
        let mut nib = Nibbles::new();
        #[allow(
            clippy::cast_possible_truncation,
            reason = "`i` is a nibble index into a 16-element array: always < 16, fits u8"
        )]
        nib.push(i as u8);
        let survivor_at = self.path.slice(..depth).join(&nib);
        let survivor = survivor.resolved(&survivor_at, self.store)?;
        Ok(survivor.merge_extension(nib))
    }
}

impl Node {
    /// Split a leaf whose key diverges from the inserted key's remainder.
    /// The shared prefix becomes an extension (if non-empty) over a
    /// branch with the two diverging remainders. An associated
    /// constructor, not a method on an existing `Node`: it builds the
    /// replacement from the old leaf's own fields (already taken out of
    /// it by the caller), not from a `Node` value it could take `self`
    /// from.
    fn split_leaf(
        key: &Nibbles,
        existing_value: Vec<u8>,
        rest: &Nibbles,
        new_value: Vec<u8>,
    ) -> Node {
        let common = key.common_prefix_length(rest);
        let old_nibble = key.get_unchecked(common);
        let new_nibble = rest.get_unchecked(common);
        let old_leaf = Node::Leaf {
            key: key.slice(common + 1..),
            value: existing_value,
        };
        let new_leaf = Node::Leaf {
            key: rest.slice(common + 1..),
            value: new_value,
        };
        Node::split_branch(
            rest.slice(..common),
            (old_nibble, old_leaf),
            (new_nibble, new_leaf),
        )
    }

    /// Split an extension whose key diverges from the inserted key's
    /// remainder, at the divergence point. An associated constructor for
    /// the same reason as [`Node::split_leaf`]: `child` is a slot the
    /// caller holds (behind `&mut Box<Node>`), not a `Node` value this
    /// could take `self` from.
    fn split_extension(
        key: &Nibbles,
        child: &mut Box<Node>,
        rest: &Nibbles,
        value: Vec<u8>,
    ) -> Node {
        let common = key.common_prefix_length(rest);
        let ext_nibble = key.get_unchecked(common);
        let new_nibble = rest.get_unchecked(common);
        let ext_rest = key.slice(common + 1..);
        let child_taken = core::mem::replace(child.as_mut(), Node::placeholder());
        let old_side = if ext_rest.is_empty() {
            child_taken
        } else {
            Node::Extension {
                key: ext_rest,
                child: Box::new(child_taken),
            }
        };
        let new_leaf = Node::Leaf {
            key: rest.slice(common + 1..),
            value,
        };
        Node::split_branch(
            rest.slice(..common),
            (ext_nibble, old_side),
            (new_nibble, new_leaf),
        )
    }

    /// Build a branch with exactly two children, at nibbles `a.0` and
    /// `b.0`, wrapped in an extension over `prefix` (or left unwrapped
    /// when `prefix` is empty). Both [`Node::split_leaf`] and
    /// [`Node::split_extension`] end this way: a common prefix, two
    /// diverging nibbles, a branch, then the wrap.
    fn split_branch(prefix: Nibbles, a: (u8, Node), b: (u8, Node)) -> Node {
        let mut children: [Option<Box<Node>>; 16] = Default::default();
        children[a.0 as usize] = Some(Box::new(a.1));
        children[b.0 as usize] = Some(Box::new(b.1));
        Node::Branch { children }.wrap_extension(prefix)
    }

    /// The stand-in used while a child is temporarily moved out during an
    /// in-place edit. It is never observable after the edit completes.
    fn placeholder() -> Node {
        Node::Leaf {
            key: Nibbles::new(),
            value: Vec::new(),
        }
    }

    /// Encode this node into `out` as its full RLP. This is used at the
    /// root, where the hash is always keccak of the encoding, no matter
    /// the length. [`SparseTrie::root`] handles unresolved roots.
    /// Unresolved children never reach here; [`Node::rlp_ref`]
    /// short-circuits them.
    fn encode_node(&self, out: &mut Vec<u8>) {
        use alloy_rlp::Encodable;
        match self {
            Node::Unresolved(r) => out.extend_from_slice(r.as_slice()),
            Node::Leaf { key, value } => {
                LeafNode::new(*key, value.clone()).encode(out);
            }
            Node::Extension { key, child } => {
                ExtensionNode::new(*key, child.rlp_ref()).encode(out);
            }
            Node::Branch { children } => {
                let mut stack = Vec::new();
                let mut mask = TrieMask::default();
                for (i, c) in children.iter().enumerate() {
                    if let Some(c) = c {
                        stack.push(c.rlp_ref());
                        #[allow(
                            clippy::cast_possible_truncation,
                            reason = "`i` indexes a 16-element array: always < 16, fits u8"
                        )]
                        mask.set_bit(i as u8);
                    }
                }
                BranchNode::new(stack, mask).encode(out);
            }
        }
    }

    /// This node's reference form, as seen from its parent. Untouched
    /// nodes keep their original reference. Modified nodes re-encode and
    /// re-hash (inline if under 32 bytes, per MPT).
    fn rlp_ref(&self) -> RlpNode {
        if let Node::Unresolved(r) = self {
            r.clone()
        } else {
            let mut rlp = Vec::new();
            self.encode_node(&mut rlp);
            RlpNode::from_rlp(&rlp)
        }
    }

    /// Wrap this node in an extension with `prefix`, or return it
    /// unwrapped when `prefix` is empty.
    fn wrap_extension(self, prefix: Nibbles) -> Node {
        if prefix.is_empty() {
            self
        } else {
            Node::Extension {
                key: prefix,
                child: Box::new(self),
            }
        }
    }

    /// After a removal below an extension keyed by `key`. An extension
    /// may not point at a leaf or another extension, so this merges
    /// keys. Pointing at a branch stays as-is. An unresolved child is
    /// untouched, so it must still be branch-shaped (an extension never
    /// pointed at anything else), and its reference is kept.
    fn merge_extension(self, key: Nibbles) -> Node {
        match self {
            Node::Leaf { key: ck, value } => Node::Leaf {
                key: key.join(&ck),
                value,
            },
            Node::Extension { key: ck, child } => Node::Extension {
                key: key.join(&ck),
                child,
            },
            other => Node::Extension {
                key,
                child: Box::new(other),
            },
        }
    }
}
