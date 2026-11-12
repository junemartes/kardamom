//! The executor nodes of the total wipe.

use super::CTX;
use crate::harness::Harness;
use crate::nomad::Nodes;

/// The executor nodes of the case. The stale one keeps the state of the
/// old chain. The fresh ones resume on the rebuilt image at once.
pub(super) struct Executors {
    /// The container of the stale executor.
    pub(super) stale: String,
    /// The containers of the fresh executors.
    pub(super) fresh: Vec<String>,
    /// The Nomad names of the fresh executor nodes.
    pub(super) fresh_nodes: Nodes,
    /// The count of executor nodes: the fresh ones and the stale one.
    pub(super) count: usize,
}

impl Executors {
    /// The executor nodes of the contract, by index. The last one is the
    /// stale one.
    pub(super) fn of(h: &Harness) -> anyhow::Result<Self> {
        let all = h.contract.of_role("executor");
        let (stale, fresh) = all
            .split_last()
            .ok_or_else(|| crate::chaos_fail!("{CTX}: no executor node"))?;
        let fresh_nodes =
            Nodes::new(fresh.iter().map(|node| node.name.clone())).ok_or_else(|| {
                crate::chaos_fail!("{CTX}: the case needs two executor nodes or more")
            })?;
        Ok(Self {
            stale: stale.container.clone(),
            fresh: fresh.iter().map(|node| node.container.clone()).collect(),
            fresh_nodes,
            count: all.len(),
        })
    }
}
