//! The L1 deposit path, resolved, and its boundary-only session to the
//! sealer cluster: the `[cluster]` section of `--config`, and the connect.

use std::path::Path;

use anyhow::Context;
use kardamom_cluster_adapter::{ClusterConfig, LiveCluster};
use kardamom_da_watcher::{BoundaryFeed, CursorFile, DaWatcherConfig, L1Cursor};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::discovery::StreamPlane;
use serde::Deserialize;
use tokio::sync::watch;

/// The `--config` file. Only the `[cluster]` section is read.
#[derive(Deserialize)]
struct FileConfig {
    cluster: ClusterConfig,
}

/// The sealer cluster the L1 watcher follows.
pub(crate) struct SealerSession {
    cluster: ClusterConfig,
}

impl SealerSession {
    /// Read the `[cluster]` section of `path`. `egress_endpoint` sets the
    /// session's egress channel, the node's own address.
    pub(crate) fn load(path: &Path, egress_endpoint: Option<&str>) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read --config {}", path.display()))?;
        let mut cluster = toml::from_str::<FileConfig>(&text)
            .with_context(|| format!("parse the [cluster] section of {}", path.display()))?
            .cluster;
        cluster.set_egress_endpoint(egress_endpoint);
        Ok(Self { cluster })
    }

    /// Connect the session, with the member endpoints the discovery
    /// catalog lists when it lists them. Returns the session guard and the
    /// sealer's L1 origin. Keep the guard alive while the watcher runs.
    pub(crate) async fn connect(
        self,
        rt: AeronRuntime,
        plane: &StreamPlane,
    ) -> anyhow::Result<(LiveCluster, watch::Receiver<Option<u64>>)> {
        let mut cfg = self.cluster.to_live();
        if let Some(endpoints) = plane
            .cluster_ingress_endpoints()
            .await
            .context("read the cluster members from discovery")?
        {
            cfg.ingress_endpoints = endpoints;
        }
        tracing::info!(
            ingress_endpoints = %cfg.ingress_endpoints,
            egress_channel = %cfg.egress_channel,
            "kardamom-da-watcher: following the sealer's boundaries"
        );
        BoundaryFeed::connect(rt, cfg).context("connect the boundary session to the sealer")
    }
}

/// The L1 deposit path, resolved. Present only with `--l1-blocks`.
pub(crate) struct L1Path {
    pub(crate) cfg: DaWatcherConfig,
    /// The durable cursor, with its lock taken. `None` without
    /// `--l1-cursor-file`.
    pub(crate) cursor_file: Option<CursorFile<L1Cursor>>,
    /// The sealer cluster to follow. `None` without `--config`.
    pub(crate) sealer: Option<SealerSession>,
    /// The sealer's L1 origin, once [`L1Path::follow_sealer`] connected.
    pub(crate) origins: Option<watch::Receiver<Option<u64>>>,
}

impl L1Path {
    /// Connect the boundary session when `--config` names the sealer
    /// cluster. Returns the session guard: keep it alive while the
    /// watcher runs.
    pub(crate) async fn follow_sealer(
        &mut self,
        rt: &AeronRuntime,
        plane: &StreamPlane,
    ) -> anyhow::Result<Option<LiveCluster>> {
        let Some(sealer) = self.sealer.take() else {
            tracing::warn!(
                "no --config: the L1 watcher does not follow the sealer's commit; an epoch lost \
                 between its publish and the commit is not published again"
            );
            return Ok(None);
        };
        let (session, origins) = sealer.connect(rt.clone(), plane).await?;
        self.origins = Some(origins);
        Ok(Some(session))
    }
}
