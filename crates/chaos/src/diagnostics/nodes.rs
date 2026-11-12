//! The Nomad client nodes: the status of each, and its last events. A
//! node that missed its heartbeat goes `down`, and Nomad replaces its
//! allocations. The node events show when.

use super::{Diagnostics, section};

impl Diagnostics {
    /// The status and the last events of every Nomad client node.
    pub(super) async fn nomad_nodes(&self) {
        section("nomad: client nodes (status and last events)");
        let nodes = match self.nomad.nodes().await {
            Ok(nodes) => nodes,
            Err(e) => {
                println!("(node listing failed: {e:#})");
                return;
            }
        };
        for node in &nodes {
            let detail = self.nomad.node(&node.id).await.map_or_else(
                |e| {
                    format!(
                        "  node {}: status={} (detail failed: {e:#})",
                        node.name, node.status
                    )
                },
                |d| d.render(),
            );
            println!("{detail}");
        }
    }
}
