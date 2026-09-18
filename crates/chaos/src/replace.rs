//! Machine replacement. A node comes back through the Terraform root the
//! way a cloud provider replaces a server: on another address, with empty
//! volumes, and with the substrate play a new machine gets. The cases on
//! top of it assert the recovery of the workload the node ran.

use std::net::Ipv4Addr;

use crate::harness::Harness;
use crate::poll::{self, Budget};

/// The addresses of a replaced node, before and after.
#[derive(Debug, Clone, Copy)]
pub struct Replaced {
    pub old: Ipv4Addr,
    pub new: Ipv4Addr,
}

impl Harness {
    /// Replace node `name` with its next generation, and return once the
    /// new machine is provisioned and the Consul catalog lists it. `ctx`
    /// names the case in the log and in a failure.
    ///
    /// # Errors
    ///
    /// Returns an error if the node does not die, if the apply or the
    /// provisioning fails, if the node comes back on its old address, or
    /// if Consul does not list the new address within two minutes.
    pub async fn replace_node(&mut self, name: &str, ctx: &str) -> anyhow::Result<Replaced> {
        let before = self.contract.node(name)?.clone();
        let generation = before.generation.saturating_add(1);
        crate::log(format!(
            "{ctx}: replacing {name} (generation {} -> {generation}, was {})",
            before.generation, before.ip
        ));
        // The machine dies before its replacement exists. Killing it here
        // also takes the slow teardown of a heavy node (Docker inside, a
        // JVM, an archive) out of the provider's stop timeout, which a
        // sealer node exceeded ("tried to kill container, but did not
        // receive an exit event"); kill_nodes waits for a late exit.
        let container = self.container(name)?;
        self.kill_nodes(&[&container]).await?;
        let contract = self.lifecycle.replace_container(name, generation).await?;
        let replaced = Replaced {
            old: before.ip,
            new: contract.node(name)?.ip,
        };
        anyhow::ensure!(
            replaced.new != replaced.old,
            "{}: {ctx}: {name} came back on its old address {}",
            crate::FAIL_PREFIX,
            replaced.old
        );
        crate::log(format!(
            "{ctx}: {name} is a new container on {}; forgetting the old Consul record, then provisioning",
            replaced.new
        ));
        // The replacement has a fresh node id under the old name. Consul
        // treats that as a name conflict while the old record stands, so
        // the control node forgets it first.
        let control = self.container("control-0")?;
        let _ = self
            .nodes
            .exec(&control, &format!("consul force-leave -prune {name}"))
            .await;
        self.lifecycle.provision_node(name).await?;
        self.follow(contract)?;
        self.await_consul_lists(&control, name, replaced.new, ctx)
            .await?;
        Ok(replaced)
    }

    /// Wait until the Consul catalog on `control` lists `name` at
    /// `address`.
    async fn await_consul_lists(
        &self,
        control: &str,
        name: &str,
        address: Ipv4Addr,
        ctx: &str,
    ) -> anyhow::Result<()> {
        let script = format!("curl -sf http://127.0.0.1:8500/v1/catalog/node/{name}");
        let needle = format!("\"Address\":\"{address}\"");
        let outcome = poll::until(Budget::secs(120, 5), |_| async {
            let body = self.nodes.exec(control, &script).await.unwrap_or_default();
            Ok::<_, anyhow::Error>(body.contains(&needle).then_some(()))
        })
        .await?;
        outcome
            .or_fail(|t| {
                crate::chaos_fail!(
                    "{ctx}: Consul still has no record of {name} at {address} after {}s",
                    t.as_secs()
                )
            })
            .map(|_| ())
    }
}
