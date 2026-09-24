//! The cluster lifecycle: the same commands as the `container-up` and
//! `container-down` Make targets. `OpenTofu` creates the node containers
//! and exports the node contract; Ansible provisions the substrate,
//! publishes the prebuilt images, and deploys the workloads.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::Context;
use tokio::process::Command;

use crate::contract::NodeContract;

/// The Nomad HTTP port of the control node.
pub const NOMAD_HTTP_PORT: u16 = 4646;

/// The node whose Docker daemon pushes the images into the registry: the
/// registry name resolves there, not on the host.
const REGISTRY_PUSH_NODE: &str = "control-0";

/// Extra Ansible variables for the convergence playbook, as one JSON
/// object, the same input as the Make target's `CLUSTER_VARS`. A local
/// run points `images_release_dir`, `images_cluster_jar`, and
/// `workloads_deploy_binary` at prebuilt artifacts with it.
const CLUSTER_VARS_ENV: &str = "KARDAMOM_CHAOS_CLUSTER_VARS";

/// How many times the node apply runs before the bring-up fails. The apply
/// builds the node image, and that build asks Docker Hub for the base
/// image, also when the image is in the local cache. One failed request
/// there is a transient fault of the network, not of the cluster.
const APPLY_ATTEMPTS: u32 = 3;

/// The pause between two apply attempts.
const APPLY_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(15);

/// The deploy-time settings a shard passes to the workloads. Ansible
/// reads them from the environment, and a case reads the same values
/// from its knobs, so one setting drives both sides.
#[derive(Debug, Clone, Default)]
pub struct DeployVars {
    /// `-Dkardamom.cluster.snapshotIntervalS` of the sealer.
    pub cluster_snapshot_interval_s: Option<u64>,
    /// `-Dkardamom.cluster.retention` of the sealer, in frames.
    pub cluster_retention: Option<u64>,
}

impl DeployVars {
    fn env(&self) -> Vec<(&'static str, String)> {
        let snapshot = self
            .cluster_snapshot_interval_s
            .map(|v| ("KARDAMOM_CLUSTER_SNAPSHOT_S", v.to_string()));
        let retention = self
            .cluster_retention
            .map(|v| ("KARDAMOM_CLUSTER_RETENTION", v.to_string()));
        snapshot.into_iter().chain(retention).collect()
    }
}

/// The lifecycle of the container cluster under `deploy/cluster`.
#[derive(Debug, Clone)]
pub struct Lifecycle {
    cluster_dir: PathBuf,
}

impl Lifecycle {
    /// The lifecycle of the `deploy/cluster` tree of `repo_root`.
    #[must_use]
    pub fn new(repo_root: &Path) -> Self {
        Self {
            cluster_dir: repo_root.join("deploy").join("cluster"),
        }
    }

    /// A command of `program` in the checkout's `deploy/cluster`, with
    /// every environment pair passed through.
    fn command(&self, program: &str, env: &[(&str, String)]) -> Command {
        let mut cmd = Command::new(program);
        cmd.current_dir(&self.cluster_dir);
        cmd.envs(env.iter().map(|(k, v)| (*k, v.as_str())));
        cmd
    }

    /// The lifecycle of this workspace's own `deploy/cluster` tree.
    #[must_use]
    pub fn in_workspace() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
        Self::new(&root)
    }

    /// The `deploy/cluster` directory.
    #[must_use]
    pub fn cluster_dir(&self) -> &Path {
        &self.cluster_dir
    }

    /// The repository root.
    #[must_use]
    pub fn repo_root(&self) -> PathBuf {
        self.cluster_dir.join("..").join("..")
    }

    fn terraform_dir(&self) -> PathBuf {
        self.cluster_dir.join("terraform").join("containers")
    }

    /// The `-chdir` argument of tofu, relative to `deploy/cluster`.
    fn tofu_chdir() -> &'static str {
        "-chdir=terraform/containers"
    }

    /// The node contract file `OpenTofu` writes.
    #[must_use]
    pub fn contract_path(&self) -> PathBuf {
        self.terraform_dir().join("node-contract.json")
    }

    /// Read the node contract of a cluster that is already up.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract file is missing or invalid.
    pub fn contract(&self) -> anyhow::Result<NodeContract> {
        NodeContract::load(&self.contract_path())
    }

    /// Create the node containers, write the node contract, and run the
    /// convergence playbook. Output streams to this process's stdout and
    /// stderr, so a test run shows the same log as CI.
    ///
    /// # Errors
    ///
    /// Returns an error if any step exits non-zero, or if the contract
    /// the apply wrote is invalid.
    pub async fn up(&self, vars: &DeployVars) -> anyhow::Result<NodeContract> {
        self.tofu(&["init", "-input=false"]).await?;
        self.apply_with_retry().await?;
        self.write_contract().await?;
        let contract = self.contract()?;
        let nomad_addr = contract.nomad_addr(NOMAD_HTTP_PORT)?;
        let mut env = vec![
            ("NOMAD_ADDR", nomad_addr),
            ("REGISTRY_PUSH_NODE", REGISTRY_PUSH_NODE.to_string()),
        ];
        env.extend(vars.env());
        let mut cmd = self.command("ansible-playbook", &env);
        cmd.args(["-i", "localhost,", "ansible/cluster.yml"]);
        if let Ok(extra) = std::env::var(CLUSTER_VARS_ENV) {
            cmd.args(["--extra-vars", &extra]);
        }
        run_inheriting(cmd, "ansible-playbook ansible/cluster.yml").await?;
        Ok(contract)
    }

    /// Replace one node the way a cloud provider replaces a machine: the
    /// container comes back with generation `generation`, on another
    /// address and with empty volumes, and the contract file follows.
    /// The node is unprovisioned after this; see
    /// [`Self::provision_node`].
    ///
    /// # Errors
    ///
    /// Returns an error if a step exits non-zero, or if the contract the
    /// apply wrote is invalid.
    pub async fn replace_container(
        &self,
        name: &str,
        generation: u32,
    ) -> anyhow::Result<NodeContract> {
        let target = format!("-replace=docker_container.node[\"{name}\"]");
        let var = format!("-var=node_generation={{\"{name}\"={generation}}}");
        self.tofu(&["init", "-input=false"]).await?;
        self.tofu(&["apply", "-auto-approve", "-input=false", &target, &var])
            .await?;
        self.write_contract().await?;
        self.contract()
    }

    /// Provision one node with the substrate playbook, limited to that
    /// node: the play a new machine gets. The workloads are not
    /// redeployed; Nomad places the lost allocations on the node once
    /// its client joins.
    ///
    /// # Errors
    ///
    /// Returns an error if the playbook exits non-zero.
    pub async fn provision_node(&self, name: &str) -> anyhow::Result<()> {
        let contract = self.contract()?;
        let nomad_addr = contract.nomad_addr(NOMAD_HTTP_PORT)?;
        let env = vec![("NOMAD_ADDR", nomad_addr)];
        let mut cmd = self.command("ansible-playbook", &env);
        cmd.args(["-i", "localhost,", "ansible/containers.yml", "--limit"])
            .arg(format!("localhost,{name}"));
        if let Ok(extra) = std::env::var(CLUSTER_VARS_ENV) {
            cmd.args(["--extra-vars", &extra]);
        }
        run_inheriting(cmd, "ansible-playbook ansible/containers.yml (one node)").await
    }

    /// Destroy the node containers and their volumes, and remove the
    /// contract file.
    ///
    /// # Errors
    ///
    /// Returns an error if a step exits non-zero.
    pub async fn down(&self) -> anyhow::Result<()> {
        self.tofu(&["init", "-input=false"]).await?;
        self.tofu(&["destroy", "-auto-approve", "-input=false"])
            .await?;
        let path = self.contract_path();
        if path.exists() {
            std::fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
        }
        Ok(())
    }

    /// Run the node apply, again after a failure, up to [`APPLY_ATTEMPTS`]
    /// times. An apply is idempotent: a second run continues from the
    /// state the first one left.
    async fn apply_with_retry(&self) -> anyhow::Result<()> {
        let mut attempt = 1;
        loop {
            let outcome = self.tofu(&["apply", "-auto-approve", "-input=false"]).await;
            let Err(error) = outcome else {
                return Ok(());
            };
            if attempt >= APPLY_ATTEMPTS {
                return Err(error.context(format!("node apply failed {attempt} times")));
            }
            crate::log(format!(
                "node apply attempt {attempt} of {APPLY_ATTEMPTS} failed ({error:#}); retrying in {}s",
                APPLY_RETRY_DELAY.as_secs()
            ));
            tokio::time::sleep(APPLY_RETRY_DELAY).await;
            attempt += 1;
        }
    }

    async fn tofu(&self, args: &[&str]) -> anyhow::Result<()> {
        let mut cmd = self.command("tofu", &[]);
        cmd.arg(Self::tofu_chdir()).args(args);
        run_inheriting(cmd, &format!("tofu {}", args.join(" "))).await
    }

    async fn write_contract(&self) -> anyhow::Result<()> {
        let out = self
            .command("tofu", &[])
            .arg(Self::tofu_chdir())
            .args(["output", "-json", "node_contract"])
            .stderr(Stdio::inherit())
            .output()
            .await
            .context("spawn tofu output")?;
        anyhow::ensure!(
            out.status.success(),
            "tofu output node_contract failed with {}",
            out.status
        );
        let path = self.contract_path();
        std::fs::write(&path, &out.stdout).with_context(|| format!("write {}", path.display()))?;
        Ok(())
    }
}

/// Run a command with inherited stdio, and fail on a non-zero exit.
async fn run_inheriting(mut cmd: Command, what: &str) -> anyhow::Result<()> {
    let status = cmd
        .stdin(Stdio::null())
        .status()
        .await
        .with_context(|| format!("spawn {what}"))?;
    anyhow::ensure!(status.success(), "{what} failed with {status}");
    Ok(())
}
