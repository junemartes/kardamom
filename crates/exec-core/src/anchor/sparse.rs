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
        match self {
            Node::Unresolved(ref r) => Node::resolve(r, at, store),
            n => Ok(n),
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
    pub fn lookup(&mut self, key: B256) -> Result<Lookup, AnchorError> {
        let path = Nibbles::unpack(key);
        let Some(root) = self.root.as_mut() else {
            return Ok(Lookup::Absent);
        };
        Self::lookup_in(root, &path, 0, self.store)
    }

    fn lookup_in(
        node: &mut Node,
        path: &Nibbles,
        depth: usize,
        store: &NodeStore<'_>,
    ) -> Result<Lookup, AnchorError> {
        if let Node::Unresolved(r) = node {
            *node = Node::resolve(r, &path.slice(..depth), store)?;
        }
        let rest = path.slice(depth..);
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
                    Self::lookup_in(child, path, depth + key.len(), store)
                } else {
                    Ok(Lookup::Absent)
                }
            }
            Node::Branch { children } => {
                let Some(nibble) = rest.first() else {
                    return Err(AnchorError::Malformed("key exhausted at a branch"));
                };
                match children[nibble as usize].as_mut() {
                    Some(child) => Self::lookup_in(child, path, depth + 1, store),
                    None => Ok(Lookup::Absent),
                }
            }
        }
    }

    /// Insert or update `key` with `value` (non-empty RLP).
    pub fn insert(&mut self, key: B256, value: Vec<u8>) -> Result<(), AnchorError> {
        let path = Nibbles::unpack(key);
        let taken = self.root.take();
        self.root = Some(Self::insert_in(taken, &path, 0, value, self.store)?);
        Ok(())
    }

    fn insert_in(
        node: Option<Node>,
        path: &Nibbles,
        depth: usize,
        value: Vec<u8>,
        store: &NodeStore<'_>,
    ) -> Result<Node, AnchorError> {
        let rest = path.slice(depth..);
        let mut node = match node {
            None => return Ok(Node::Leaf { key: rest, value }),
            Some(n) => n.resolved(&path.slice(..depth), store)?,
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
                // Split: a shared prefix becomes an extension (if
                // non-empty) over a branch with the two diverging
                // remainders.
                let common = key.common_prefix_length(&rest);
                let old_nibble = key.get_unchecked(common);
                let new_nibble = rest.get_unchecked(common);
                let old_leaf = Node::Leaf {
                    key: key.slice(common + 1..),
                    value: core::mem::take(existing),
                };
                let new_leaf = Node::Leaf {
                    key: rest.slice(common + 1..),
                    value,
                };
                let mut children: [Option<Box<Node>>; 16] = Default::default();
                children[old_nibble as usize] = Some(Box::new(old_leaf));
                children[new_nibble as usize] = Some(Box::new(new_leaf));
                let branch = Node::Branch { children };
                Ok(wrap_extension(rest.slice(..common), branch))
            }
            Node::Extension { key, child } => {
                if rest.starts_with(key) {
                    let klen = key.len();
                    let taken = core::mem::replace(child.as_mut(), placeholder());
                    **child = Self::insert_in(Some(taken), path, depth + klen, value, store)?;
                    return Ok(node);
                }
                // Split the extension at the divergence point.
                let common = key.common_prefix_length(&rest);
                let ext_nibble = key.get_unchecked(common);
                let new_nibble = rest.get_unchecked(common);
                let ext_rest = key.slice(common + 1..);
                let child_taken = core::mem::replace(child.as_mut(), placeholder());
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
                let mut children: [Option<Box<Node>>; 16] = Default::default();
                children[ext_nibble as usize] = Some(Box::new(old_side));
                children[new_nibble as usize] = Some(Box::new(new_leaf));
                let branch = Node::Branch { children };
                Ok(wrap_extension(rest.slice(..common), branch))
            }
            Node::Branch { children } => {
                let Some(nibble) = rest.first() else {
                    return Err(AnchorError::Malformed("key exhausted at a branch"));
                };
                let slot = &mut children[nibble as usize];
                let taken = slot.take().map(|b| *b);
                *slot = Some(Box::new(Self::insert_in(
                    taken,
                    path,
                    depth + 1,
                    value,
                    store,
                )?));
                Ok(node)
            }
        }
    }

    /// Remove `key`. Removing an absent key is a no-op, the same as
    /// writing zero to a slot that was already absent.
    pub fn remove(&mut self, key: B256) -> Result<(), AnchorError> {
        let path = Nibbles::unpack(key);
        let Some(root) = self.root.take() else {
            return Ok(());
        };
        self.root = Self::remove_in(root, &path, 0, self.store)?;
        Ok(())
    }

    /// Remove inside `node`. `None` means the subtree vanished entirely.
    fn remove_in(
        node: Node,
        path: &Nibbles,
        depth: usize,
        store: &NodeStore<'_>,
    ) -> Result<Option<Node>, AnchorError> {
        let node = node.resolved(&path.slice(..depth), store)?;
        let rest = path.slice(depth..);
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
            Node::Extension { key, child } => {
                if !rest.starts_with(&key) {
                    return Ok(Some(Node::Extension { key, child }));
                }
                let klen = key.len();
                match Self::remove_in(*child, path, depth + klen, store)? {
                    None => Ok(None),
                    Some(new_child) => Ok(Some(merge_extension(key, new_child))),
                }
            }
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
                        if let Some(kept) = Self::remove_in(*child, path, depth + 1, store)? {
                            children[idx] = Some(Box::new(kept));
                            return Ok(Some(Node::Branch { children }));
                        }
                    }
                }
                // The child vanished. Two or more survivors keep the
                // branch. Exactly one survivor collapses it. This is the
                // deletion shape whose sibling the capture fixed point
                // must have supplied.
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
                    (Some(i), None) => {
                        // Splice the lone survivor up through nibble `i`.
                        // Resolving it can demand an off-path node. This
                        // is the MissingNode the fixed point exists to
                        // feed.
                        let survivor = children[i].take().expect("survivor indexed");
                        let mut nib = Nibbles::new();
                        nib.push(i as u8);
                        let survivor_at = path.slice(..depth).join(&nib);
                        let survivor = survivor.resolved(&survivor_at, store)?;
                        Ok(Some(match survivor {
                            Node::Unresolved(_) => unreachable!("resolved above"),
                            Node::Leaf { key, value } => Node::Leaf {
                                key: nib.join(&key),
                                value,
                            },
                            Node::Extension { key, child } => Node::Extension {
                                key: nib.join(&key),
                                child,
                            },
                            b @ Node::Branch { .. } => Node::Extension {
                                key: nib,
                                child: Box::new(b),
                            },
                        }))
                    }
                }
            }
        }
    }

    /// Re-hash the trie bottom-up. Untouched (unresolved) subtrees reuse
    /// the reference their parent carried, with no store access and no
    /// recompute.
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
                encode_node(node, &mut rlp);
                keccak256(&rlp)
            }
        }
    }
}

/// The stand-in used while a child is temporarily moved out during an
/// in-place edit. It is never observable after the edit completes.
fn placeholder() -> Node {
    Node::Leaf {
        key: Nibbles::new(),
        value: Vec::new(),
    }
}

/// Encode `node` into `out` as its full RLP. This is used at the root,
/// where the hash is always keccak of the encoding, no matter the
/// length. [`SparseTrie::root`] handles unresolved roots. Unresolved
/// children never reach here; [`rlp_ref`] short-circuits them.
fn encode_node(node: &Node, out: &mut Vec<u8>) {
    use alloy_rlp::Encodable;
    match node {
        Node::Unresolved(r) => out.extend_from_slice(r.as_slice()),
        Node::Leaf { key, value } => {
            LeafNode::new(*key, value.clone()).encode(out);
        }
        Node::Extension { key, child } => {
            ExtensionNode::new(*key, rlp_ref(child)).encode(out);
        }
        Node::Branch { children } => {
            let mut stack = Vec::new();
            let mut mask = TrieMask::default();
            for (i, c) in children.iter().enumerate() {
                if let Some(c) = c {
                    stack.push(rlp_ref(c));
                    mask.set_bit(i as u8);
                }
            }
            BranchNode::new(stack, mask).encode(out);
        }
    }
}

/// A node's reference form, as seen from its parent. Untouched nodes keep
/// their original reference. Modified nodes re-encode and re-hash
/// (inline if under 32 bytes, per MPT).
fn rlp_ref(node: &Node) -> RlpNode {
    match node {
        Node::Unresolved(r) => r.clone(),
        _ => {
            let mut rlp = Vec::new();
            encode_node(node, &mut rlp);
            RlpNode::from_rlp(&rlp)
        }
    }
}

fn wrap_extension(prefix: Nibbles, node: Node) -> Node {
    if prefix.is_empty() {
        node
    } else {
        Node::Extension {
            key: prefix,
            child: Box::new(node),
        }
    }
}

/// After a removal below an extension. An extension may not point at a
/// leaf or another extension, so this merges keys. Pointing at a branch
/// stays as-is. An unresolved child is untouched, so it must still be
/// branch-shaped (an extension never pointed at anything else), and its
/// reference is kept.
fn merge_extension(key: Nibbles, child: Node) -> Node {
    match child {
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
