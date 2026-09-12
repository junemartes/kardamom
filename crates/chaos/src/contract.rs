//! The node contract: version 1 of the JSON that `terraform/containers`
//! exports. Every address the suite uses comes from it. Nothing in the
//! suite names an address.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

/// The contract version this suite reads.
const CONTRACT_VERSION: u32 = 1;

/// One node container of the cluster.
#[derive(Debug, Clone, Deserialize)]
pub struct Node {
    /// The bare instance name, `<class>-<i>`.
    pub name: String,
    /// The host-side container name, `kardamom-<class>-<i>`.
    pub container: String,
    /// The node class, which is also the Nomad `meta.role`.
    pub role: String,
    pub tier: String,
    pub index: u32,
    pub control_plane: bool,
    /// The address Docker assigned on the cluster network.
    pub ip: Ipv4Addr,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Network {
    pub name: String,
    pub bridge: String,
    pub subnet: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeContract {
    pub version: u32,
    pub network: Network,
    /// Keyed by the bare instance name.
    pub nodes: BTreeMap<String, Node>,
}

impl NodeContract {
    /// Read and check the contract file `tofu output -json node_contract`
    /// wrote.
    ///
    /// # Errors
    ///
    /// Returns an error if the file is missing, is not JSON, carries
    /// another version, or lists no node.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read node contract {}", path.display()))?;
        let contract: Self = serde_json::from_str(&text)
            .with_context(|| format!("parse node contract {}", path.display()))?;
        anyhow::ensure!(
            contract.version == CONTRACT_VERSION,
            "{}: node contract version {} (expected {CONTRACT_VERSION})",
            path.display(),
            contract.version
        );
        anyhow::ensure!(
            !contract.nodes.is_empty(),
            "{}: node contract lists no node",
            path.display()
        );
        Ok(contract)
    }

    /// The node with the bare name `<class>-<i>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract has no such node.
    pub fn node(&self, name: &str) -> anyhow::Result<&Node> {
        self.nodes
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("node contract has no node {name}"))
    }

    /// The nodes of one class, ordered by index.
    #[must_use]
    pub fn of_role(&self, role: &str) -> Vec<&Node> {
        let mut nodes: Vec<&Node> = self.nodes.values().filter(|n| n.role == role).collect();
        nodes.sort_by_key(|n| n.index);
        nodes
    }

    /// The control node that runs the Nomad server.
    ///
    /// # Errors
    ///
    /// Returns an error if no node carries the control plane.
    pub fn control(&self) -> anyhow::Result<&Node> {
        self.nodes
            .values()
            .find(|n| n.control_plane)
            .ok_or_else(|| anyhow::anyhow!("node contract has no control-plane node"))
    }

    /// The Nomad HTTP address on the control node, as the Docker host
    /// reaches it.
    ///
    /// # Errors
    ///
    /// Returns an error if no node carries the control plane.
    pub fn nomad_addr(&self, port: u16) -> anyhow::Result<String> {
        Ok(format!("http://{}:{port}", self.control()?.ip))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
      "version": 1,
      "network": {"name": "kardamom-net", "bridge": "kardamom-br0", "subnet": "192.168.56.0/24"},
      "image": {"name": "kardamom-node:ci", "id": "sha256:x"},
      "nodes": {
        "control-0": {"name": "control-0", "container": "kardamom-control-0", "role": "control",
                      "tier": "control", "index": 0, "control_plane": true, "ip": "192.168.56.8"},
        "executor-1": {"name": "executor-1", "container": "kardamom-executor-1", "role": "executor",
                       "tier": "worker", "index": 1, "control_plane": false, "ip": "192.168.56.12"},
        "executor-0": {"name": "executor-0", "container": "kardamom-executor-0", "role": "executor",
                       "tier": "worker", "index": 0, "control_plane": false, "ip": "192.168.56.11"}
      }
    }"#;

    #[test]
    fn loads_and_orders_a_class_by_index() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node-contract.json");
        std::fs::write(&path, SAMPLE).unwrap();
        let contract = NodeContract::load(&path).unwrap();
        let names: Vec<&str> = contract
            .of_role("executor")
            .iter()
            .map(|n| n.name.as_str())
            .collect();
        assert_eq!(names, ["executor-0", "executor-1"]);
        assert_eq!(
            contract.nomad_addr(4646).unwrap(),
            "http://192.168.56.8:4646"
        );
        assert_eq!(
            contract.node("executor-1").unwrap().ip.to_string(),
            "192.168.56.12"
        );
        assert!(contract.node("aux-0").is_err());
    }

    #[test]
    fn rejects_another_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node-contract.json");
        std::fs::write(&path, SAMPLE.replace("\"version\": 1", "\"version\": 2")).unwrap();
        assert!(NodeContract::load(&path).is_err());
    }
}
