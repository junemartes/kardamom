//! The cluster lifecycle: the same commands as the `container-up` and
//! `container-down` recipes. `OpenTofu` creates the node containers
//! and exports the node contract; Ansible provisions the substrate,
//! publishes the prebuilt images, and deploys the workloads. On a saved
//! cluster (`just container-save`), the bring-up starts the provisioned
//! nodes again and only deploys the workloads.

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

/// The directory of a saved cluster (`just container-save`). When it is
/// set, the bring-up recreates the saved volumes, starts each node from
/// its provisioned image, and runs `ansible/resume.yml` in place of the
/// convergence playbook.
const PREPARED_ENV: &str = "KARDAMOM_PREPARED_CLUSTER";

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
#[allow(
    clippy::struct_excessive_bools,
    reason = "each bool is one independent deploy variable that Ansible reads on its own"
)]
pub struct DeployVars {
    /// `-Dkardamom.cluster.snapshotIntervalS` of the sealer.
    pub cluster_snapshot_interval_s: Option<u64>,
    /// `-Dkardamom.cluster.retention` of the sealer, in frames.
    pub cluster_retention: Option<u64>,
    /// `-Dkardamom.cluster.daLagBudgetBlocks` of the sealer, in blocks.
    pub da_lag_budget_blocks: Option<u64>,
    /// Fund the public local canary at genesis and deploy its job.
    pub canary_local: Option<bool>,
    /// Deploy the L1 fault proxy in front of the in-cluster anvil, and
    /// point the followers (the batcher, the da-watcher, the indexer) at
    /// it. The indexer is deployed only with it on a container cluster.
    pub l1_fault_proxy: bool,
    /// The indexer's poll cadence, in seconds.
    pub indexer_poll_s: Option<u64>,
    /// How long `l1_blocks` may carry no record before the da-watcher
    /// pauses on the follower, in seconds.
    pub da_watcher_silence_s: Option<u64>,
    /// `--exec-cursor` of the executors: each one sends its recorded
    /// cursor to the sealer.
    pub exec_cursor: bool,
    /// `--checkpoint-log` of the executors: each one keeps the block log
    /// of its checkpoints.
    pub checkpoint_log: bool,
    /// A carry-born cluster (`KARDAMOM_GENESIS_CARRY=on`). The chain starts
    /// with features 3, 4 and 5 active at genesis, and the deploy turns on
    /// every switch of the carry window: the member feed, the executors on
    /// the sealer nodes in carry mode, the egress, the batcher and the
    /// validator on the egress, the v2 loop of the sequencers, and the
    /// version floor 5. The deploy accepts it only for a new cluster, or
    /// for a registered sealer job that has the genesis features.
    pub genesis_carry: bool,
    /// The switches of the carry window. See [`CarrySwitches`].
    pub carry: CarrySwitches,
}

/// The deploy settings of the carry window. With every field at its
/// default, the deploy renders every job as before.
#[derive(Debug, Clone, Default)]
pub struct CarrySwitches {
    /// The member feed and the executors on the sealer nodes
    /// (`KARDAMOM_MEMBER_FEED=on`, `KARDAMOM_EXEC_ON_SEALERS=on`).
    pub exec_on_sealers: bool,
    /// How far the egress goes.
    pub egress: EgressStage,
    /// The v2 loop of the sequencers (`KARDAMOM_SEQUENCER_ADMISSION=on`).
    pub admission: bool,
    /// `KARDAMOM_SEALER_VERSION_FLOOR` of the sequencers.
    pub version_floor: Option<u8>,
    /// The member indexes of the executors in carry mode
    /// (`KARDAMOM_CARRY_EXECUTORS`).
    pub carry_executors: Vec<u32>,
    /// The lockbox that the L1 follower reads (`LOCKBOX_ADDRESS`).
    pub lockbox: Option<String>,
}

/// The egress and its readers. A reader needs the egress, and the egress
/// needs the frames of the executors.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EgressStage {
    /// No egress.
    #[default]
    Off,
    /// The frames of the executors and the egress
    /// (`KARDAMOM_EXEC_FRAMES=on`, `KARDAMOM_EGRESS=1`).
    On,
    /// Also the batcher and the validator on the egress
    /// (`KARDAMOM_BATCHER_EGRESS=on`, `KARDAMOM_VALIDATOR_EGRESS=on`).
    Readers,
}

impl CarrySwitches {
    fn env(&self) -> [Option<(&'static str, String)>; 10] {
        let executors = (!self.carry_executors.is_empty()).then(|| {
            let ids: Vec<String> = self.carry_executors.iter().map(u32::to_string).collect();
            ("KARDAMOM_CARRY_EXECUTORS", ids.join(","))
        });
        let egress = self.egress != EgressStage::Off;
        let readers = self.egress == EgressStage::Readers;
        [
            DeployVars::switch(self.exec_on_sealers, "KARDAMOM_MEMBER_FEED", "on"),
            DeployVars::switch(self.exec_on_sealers, "KARDAMOM_EXEC_ON_SEALERS", "on"),
            DeployVars::switch(egress, "KARDAMOM_EXEC_FRAMES", "on"),
            DeployVars::switch(egress, "KARDAMOM_EGRESS", "1"),
            DeployVars::switch(self.admission, "KARDAMOM_SEQUENCER_ADMISSION", "on"),
            DeployVars::switch(readers, "KARDAMOM_BATCHER_EGRESS", "on"),
            DeployVars::switch(readers, "KARDAMOM_VALIDATOR_EGRESS", "on"),
            self.version_floor
                .map(|v| ("KARDAMOM_SEALER_VERSION_FLOOR", v.to_string())),
            executors,
            self.lockbox
                .as_ref()
                .map(|a| ("LOCKBOX_ADDRESS", a.clone())),
        ]
    }
}

impl DeployVars {
    /// One variable that is `value` when `on`, and unset when not.
    fn switch(on: bool, name: &'static str, value: &str) -> Option<(&'static str, String)> {
        on.then(|| (name, value.to_string()))
    }

    /// The carry from genesis, always set: `on`, or empty for the
    /// reference path. A value in the caller's environment then cannot
    /// make a reference-born shard carry-born.
    fn genesis_carry_env(&self) -> (&'static str, String) {
        let value = if self.genesis_carry { "on" } else { "" };
        ("KARDAMOM_GENESIS_CARRY", value.to_string())
    }

    /// The deploy variables, as Ansible reads them. The knobs of a shard
    /// read the same pairs (see [`crate::Knobs::deployed`]).
    pub(crate) fn env(&self) -> Vec<(&'static str, String)> {
        [
            self.cluster_snapshot_interval_s
                .map(|v| ("KARDAMOM_CLUSTER_SNAPSHOT_S", v.to_string())),
            self.cluster_retention
                .map(|v| ("KARDAMOM_CLUSTER_RETENTION", v.to_string())),
            self.da_lag_budget_blocks
                .map(|v| ("KARDAMOM_DA_LAG_BUDGET_BLOCKS", v.to_string())),
            self.canary_local
                .map(|enabled| ("CANARY_LOCAL", if enabled { "1" } else { "0" }.to_string())),
            Self::switch(self.l1_fault_proxy, "KARDAMOM_L1_FAULT_PROXY", "1"),
            self.indexer_poll_s
                .map(|v| ("L1_INDEXER_POLL_S", v.to_string())),
            self.da_watcher_silence_s
                .map(|v| ("L1_SILENCE_S", v.to_string())),
            Self::switch(self.exec_cursor, "KARDAMOM_EXEC_CURSOR", "on"),
            Self::switch(self.checkpoint_log, "KARDAMOM_CHECKPOINT_LOG", "on"),
            Some(self.genesis_carry_env()),
        ]
        .into_iter()
        .chain(self.carry.env())
        .flatten()
        .collect()
    }
}

/// The lifecycle of the container cluster under `deploy/cluster`, with
/// the deploy settings that the bring-up and each later deploy pass.
#[derive(Debug, Clone)]
pub struct Lifecycle {
    cluster_dir: PathBuf,
    vars: DeployVars,
    prepared: Option<Prepared>,
}

/// A cluster that `just container-save` kept: one provisioned image per
/// node, the node volumes, and the image manifest.
#[derive(Debug, Clone)]
struct Prepared {
    dir: PathBuf,
}

impl Prepared {
    /// The saved cluster that [`PREPARED_ENV`] names, if any. A relative
    /// directory is resolved from the working directory of this process,
    /// because the commands run in `deploy/cluster`.
    fn from_env() -> Option<Self> {
        std::env::var_os(PREPARED_ENV)
            .filter(|dir| !dir.is_empty())
            .and_then(|dir| std::path::absolute(dir).ok())
            .map(|dir| Self { dir })
    }

    /// The `node_images` variable of terraform/containers: the
    /// provisioned image of each node, as a JSON object.
    fn node_images(&self) -> anyhow::Result<String> {
        let path = self.dir.join("node-images.json");
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))
    }
}

/// The last attempt of the environment when a deploy starts. The release
/// gate refuses a deploy over an attempt that is still `started` unless
/// the deploy says that the attempt died.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LastAttempt {
    /// Accepted or rolled back: the gate's default.
    Settled,
    /// Failed, and this deploy replaces it on purpose
    /// (`KARDAMOM_REPLACE_ATTEMPT=1`): the broken-image case heals the
    /// executor job with the real manifest.
    Dead,
}

impl LastAttempt {
    fn env(self) -> Option<(&'static str, String)> {
        match self {
            Self::Settled => None,
            Self::Dead => Some(("KARDAMOM_REPLACE_ATTEMPT", "1".to_string())),
        }
    }
}

impl Lifecycle {
    /// The lifecycle of the `deploy/cluster` tree of `repo_root`.
    #[must_use]
    pub fn new(repo_root: &Path) -> Self {
        Self {
            cluster_dir: repo_root.join("deploy").join("cluster"),
            vars: DeployVars::default(),
            prepared: Prepared::from_env(),
        }
    }

    /// The same lifecycle with the deploy settings `vars`.
    #[must_use]
    pub fn with_vars(self, vars: DeployVars) -> Self {
        Self { vars, ..self }
    }

    /// The deploy settings of the bring-up and of each later deploy.
    #[must_use]
    pub fn vars(&self) -> &DeployVars {
        &self.vars
    }

    /// A command of `program` in the checkout's `deploy/cluster`, with
    /// every environment pair passed through.
    fn command(&self, program: &str, env: &[(&str, String)]) -> Command {
        let mut cmd = Command::new(program);
        cmd.current_dir(&self.cluster_dir);
        cmd.envs(env.iter().map(|(k, v)| (*k, v.as_str())));
        if self.vars.canary_local == Some(true) {
            cmd.env("CANARY_MNEMONIC", "");
            cmd.env("CANARY_L1_KEY", "");
            cmd.env(
                "ALERTMANAGER_CONFIG_FILE",
                self.cluster_dir.join("config/alertmanager-chaos.yml"),
            );
        }
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
    pub async fn up(&self) -> anyhow::Result<NodeContract> {
        self.tofu(&["init", "-input=false"]).await?;
        if let Some(prepared) = &self.prepared {
            self.restore(prepared).await?;
        }
        self.apply_with_retry().await?;
        self.write_contract().await?;
        let contract = self.contract()?;
        let nomad_addr = contract.nomad_addr(NOMAD_HTTP_PORT)?;
        // Bring-up starts a new sealer cluster. The deploy opens the
        // bootstrap only while it registers a job that Nomad does not know,
        // so a bring-up over a running cluster opens none. The waves apply
        // only to a cluster without jobs and without a deploy record.
        let mut env = vec![
            ("NOMAD_ADDR", nomad_addr),
            ("REGISTRY_PUSH_NODE", REGISTRY_PUSH_NODE.to_string()),
            ("KARDAMOM_CLUSTER_BOOTSTRAP", "1".to_string()),
            ("KARDAMOM_DEPLOY_WAVES", "1".to_string()),
        ];
        env.extend(self.vars.env());
        let playbook = self.bring_up_playbook();
        let cmd = self.playbook(playbook, &env);
        run_inheriting(cmd, &format!("ansible-playbook {playbook}")).await?;
        Ok(contract)
    }

    /// Recreate the volumes of the saved cluster `prepared`, empty of
    /// anything a previous bring-up wrote, and put its image manifest in
    /// place.
    async fn restore(&self, prepared: &Prepared) -> anyhow::Result<()> {
        let mut cmd = self.command("just", &[]);
        cmd.arg("container-restore").arg(&prepared.dir);
        run_inheriting(cmd, "just container-restore").await
    }

    /// The playbook of the bring-up: the resume of a saved cluster, or
    /// the full convergence of new nodes.
    fn bring_up_playbook(&self) -> &'static str {
        if self.prepared.is_some() {
            "ansible/resume.yml"
        } else {
            "ansible/cluster.yml"
        }
    }

    /// Deploy the workloads of the cluster at `nomad_addr` from the image
    /// manifest at `manifest`, a path relative to `deploy/cluster`, with
    /// the deploy settings of the bring-up, and report whether the deploy
    /// succeeded. The output streams to this process's stdout and stderr.
    /// A failed deploy is a result here, not an error: the broken-image
    /// case expects one.
    ///
    /// # Errors
    ///
    /// Returns an error if the playbook cannot be spawned.
    pub async fn deploy(
        &self,
        nomad_addr: &str,
        manifest: &str,
        last: LastAttempt,
    ) -> anyhow::Result<bool> {
        let mut env = vec![
            ("NOMAD_ADDR", nomad_addr.to_string()),
            ("DIGEST_MANIFEST", manifest.to_string()),
            ("KARDAMOM_ENV", "chaos".to_string()),
        ];
        env.extend(last.env());
        env.extend(self.vars.env());
        Self::passes(self.playbook("ansible/deploy.yml", &env)).await
    }

    /// Run `just sealer-upgrade-check` of the cluster at `nomad_addr` for
    /// the upgrade transaction of `feature` at `activation_ms`, and report
    /// whether the check passed. The check reads the cluster and changes
    /// nothing. A refusal is a result here, not an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the playbook cannot be spawned.
    pub async fn upgrade_check(
        &self,
        nomad_addr: &str,
        feature: u64,
        activation_ms: u64,
    ) -> anyhow::Result<bool> {
        let mut env = vec![
            ("NOMAD_ADDR", nomad_addr.to_string()),
            ("KARDAMOM_ENV", "chaos".to_string()),
            ("KARDAMOM_UPGRADE_FEATURE", feature.to_string()),
            ("KARDAMOM_UPGRADE_ACTIVATION_MS", activation_ms.to_string()),
        ];
        env.extend(self.vars.env());
        Self::passes(self.playbook("ansible/upgrade-check.yml", &env)).await
    }

    /// `ansible-playbook` of `playbook` on the controller, with every
    /// environment pair and the extra variables of the suite.
    fn playbook(&self, playbook: &str, env: &[(&str, String)]) -> Command {
        let mut cmd = self.command("ansible-playbook", env);
        cmd.args(["-i", "localhost,", playbook]);
        if let Ok(extra) = std::env::var(CLUSTER_VARS_ENV) {
            cmd.args(["--extra-vars", &extra]);
        }
        cmd
    }

    /// Whether `cmd` exits zero. Its output streams to this process.
    async fn passes(mut cmd: Command) -> anyhow::Result<bool> {
        let status = cmd
            .stdin(Stdio::null())
            .status()
            .await
            .context("spawn ansible-playbook")?;
        Ok(status.success())
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
        let mut cmd = self.playbook("ansible/containers.yml", &env);
        cmd.arg("--limit").arg(format!("localhost,{name}"));
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

    /// Run tofu in the container root. On a saved cluster, every run
    /// sees the provisioned node images, so an apply or a replacement
    /// keeps the nodes of generation 0 on them.
    async fn tofu(&self, args: &[&str]) -> anyhow::Result<()> {
        let env = match &self.prepared {
            Some(prepared) => vec![("TF_VAR_node_images", prepared.node_images()?)],
            None => Vec::new(),
        };
        let mut cmd = self.command("tofu", &env);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_vars_set_only_an_empty_genesis_carry() {
        assert_eq!(
            DeployVars::default().env(),
            vec![("KARDAMOM_GENESIS_CARRY", String::new())]
        );
    }

    #[test]
    fn a_local_canary_uses_the_cluster_test_alert_receiver() {
        let vars = DeployVars {
            canary_local: Some(true),
            ..DeployVars::default()
        };
        assert!(vars.env().contains(&("CANARY_LOCAL", "1".to_string())));
        let lifecycle = Lifecycle::new(Path::new("/repo")).with_vars(vars);
        let command = lifecycle.command("ansible-playbook", &[]);
        let setting = command
            .as_std()
            .get_envs()
            .find(|(key, _)| *key == "ALERTMANAGER_CONFIG_FILE");
        assert_eq!(
            setting.unwrap().1,
            Some(std::ffi::OsStr::new(
                "/repo/deploy/cluster/config/alertmanager-chaos.yml"
            ))
        );
        let defaults = Lifecycle::new(Path::new("/repo")).command("ansible-playbook", &[]);
        assert!(
            !defaults
                .as_std()
                .get_envs()
                .any(|(key, _)| key == "ALERTMANAGER_CONFIG_FILE")
        );
    }

    #[test]
    fn the_carry_switches_reach_the_deploy_variables() {
        let vars = DeployVars {
            exec_cursor: true,
            genesis_carry: true,
            carry: CarrySwitches {
                exec_on_sealers: true,
                egress: EgressStage::Readers,
                admission: true,
                version_floor: Some(5),
                carry_executors: vec![0, 1],
                lockbox: Some("0xab".to_string()),
            },
            ..DeployVars::default()
        };
        let env = vars.env();
        let want = [
            ("KARDAMOM_EXEC_CURSOR", "on"),
            ("KARDAMOM_GENESIS_CARRY", "on"),
            ("KARDAMOM_MEMBER_FEED", "on"),
            ("KARDAMOM_EXEC_ON_SEALERS", "on"),
            ("KARDAMOM_EXEC_FRAMES", "on"),
            ("KARDAMOM_EGRESS", "1"),
            ("KARDAMOM_SEQUENCER_ADMISSION", "on"),
            ("KARDAMOM_BATCHER_EGRESS", "on"),
            ("KARDAMOM_VALIDATOR_EGRESS", "on"),
            ("KARDAMOM_SEALER_VERSION_FLOOR", "5"),
            ("KARDAMOM_CARRY_EXECUTORS", "0,1"),
            ("LOCKBOX_ADDRESS", "0xab"),
        ];
        assert_eq!(
            env,
            want.map(|(k, v)| (k, v.to_string())).to_vec(),
            "{env:?}"
        );
    }

    #[test]
    fn a_saved_cluster_resumes_in_place_of_the_convergence() {
        let new = Lifecycle {
            prepared: None,
            ..Lifecycle::new(Path::new("/repo"))
        };
        let saved = Lifecycle {
            prepared: Some(Prepared {
                dir: PathBuf::from("/saved"),
            }),
            ..new.clone()
        };
        assert_eq!(new.bring_up_playbook(), "ansible/cluster.yml");
        assert_eq!(saved.bring_up_playbook(), "ansible/resume.yml");
    }

    #[test]
    fn a_lifecycle_keeps_its_vars() {
        let vars = DeployVars {
            checkpoint_log: true,
            ..DeployVars::default()
        };
        let lifecycle = Lifecycle::new(Path::new("/repo")).with_vars(vars);
        assert!(lifecycle.vars().checkpoint_log);
        assert_eq!(lifecycle.cluster_dir(), Path::new("/repo/deploy/cluster"));
    }
}
