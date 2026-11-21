//! The source of the canonical stream of one run: the member feed, the
//! cluster egress, or the range read of the egress up to its head.

use anyhow::Result;
use kardamom_engine::ExecutorError;
use kardamom_engine::bin_support;
use kardamom_engine::reader::carried::{CarriedSubscription, NoSource};
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::discovery::StreamPlane;

use crate::Boot;
use crate::wiring::ExecutorOrdering;

/// The source of the canonical stream of one run of an executor that reads
/// the cluster egress. A carry executor reads the member feed in every run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Source {
    /// The references of the cluster egress, joined with `tx_data`.
    Cluster,
    /// The range read of the egress up to its head. The run ends at the
    /// head.
    Egress,
}

impl Source {
    /// Whether the run after a run of this source reads the egress: the
    /// sealer refused the cursor of a cluster run below its window, and
    /// the executor has an egress (`egress`).
    pub(crate) fn egress_next(self, error: Option<&ExecutorError>, egress: bool) -> bool {
        egress
            && self == Self::Cluster
            && matches!(error, Some(ExecutorError::ClusterReplayUnavailable { .. }))
    }

    /// Whether the run reads the bytes of each entry from its source, so it
    /// opens no `tx_data` lane.
    fn carries(self, carry_bytes: bool) -> bool {
        carry_bytes || self == Self::Egress
    }
}

/// What the reader of one run starts from.
pub(crate) struct ReaderStart<'a> {
    /// The runtime of the `tx_data` lanes, and of the live `egress_order`
    /// frames with `--egress`.
    rt: &'a AeronRuntime,
    state: &'a kardamom_executor::state::PreparedState,
    history: Option<kardamom_engine::reader::carried::LocalHistory>,
    stop: &'a tokio_util::sync::CancellationToken,
    source: Source,
}

/// The `tx_data` lanes of one run and their join-miss refetch.
pub(crate) type Lanes = (
    Vec<bin_support::LiveTxDataSub>,
    Option<kardamom_engine::reader::JoinRecoveryFactory>,
);

impl<'a> ReaderStart<'a> {
    pub(crate) fn new(
        rt: &'a AeronRuntime,
        state: &'a kardamom_executor::state::PreparedState,
        history: Option<kardamom_engine::reader::carried::LocalHistory>,
        stop: &'a tokio_util::sync::CancellationToken,
        source: Source,
    ) -> Self {
        Self {
            rt,
            state,
            history,
            stop,
            source,
        }
    }

    /// The `tx_data` lanes and their join-miss refetch. The member feed and
    /// the egress carry the bytes, so a run that reads them opens no lane
    /// and refetches nothing.
    pub(crate) fn open_lanes(
        &self,
        boot: &Boot,
        plane: &mut StreamPlane,
        aeron_cfg: &kardamom_log::config::AeronConfig,
    ) -> Result<Lanes> {
        let args = &boot.args;
        if self.source.carries(args.carry_bytes) {
            return Ok((Vec::new(), None));
        }
        Ok((
            bin_support::open_tx_data_subs(self.rt, plane)?,
            bin_support::archive_join_recovery(
                plane,
                aeron_cfg,
                args.aeron_dir.as_deref(),
                args.archive_control_response_endpoint.as_deref(),
                args.replay_destination_endpoint.as_deref(),
            ),
        ))
    }

    /// The one `tx_ordering` subscription, always the Aeron Cluster (Raft)
    /// egress. The cluster has already deduped and totally ordered the
    /// stream, and exposes a blocking `next()`, so no async-to-sync bridge is
    /// needed. Leader failover and reconnect, including crash-recovery replay
    /// of the canonical stream, are handled inside the cluster client, so the
    /// reader never sees an image rotation. The subscription's canonical-index
    /// cursor drops any reconnect overlap. The cluster-session guard (`LiveCluster`) must outlive the executor loop,
    /// so [`bin_support::LiveStreams`] holds it. It drops only after the
    /// shutdown wait ends. With `--carry-bytes` the executor reads the member
    /// feed and opens no cluster session.
    ///
    /// The member ingress endpoints come from the catalog when discovery
    /// lists them, else from the static `[cluster]` section. The executor is
    /// the chosen emitter of the `kardamom_sealer_*` re-export, on by default
    /// in the shared subscription.
    ///
    /// With `--egress`, the reader of the member feed has the egress as its
    /// second source. A cursor below the replay window of the member then
    /// reads the egress, and only a cursor below the floor of the egress too
    /// ends the run with the refused replay that stages a peer checkpoint.
    /// A run of [`Source::Egress`] reads the egress alone, up to its head,
    /// and opens no cluster session.
    pub(crate) async fn connect(
        self,
        boot: &Boot,
        plane: &mut StreamPlane,
    ) -> Result<(
        Option<kardamom_cluster_adapter::LiveCluster>,
        ExecutorOrdering,
    )> {
        let (args, file_cfg) = (&boot.args, &boot.file_cfg);
        let cursor = bin_support::cluster_replay_cursor(&self.state.start);
        let aeron_dir = args.aeron_dir.as_deref();
        if args.carry_bytes {
            let egress = args
                .egress
                .clone()
                .map(|base| bin_support::open_egress_transport(self.rt, plane, base))
                .transpose()?;
            let sub = bin_support::open_member_feed(bin_support::MemberFeed {
                aeron_dir,
                cursor,
                history: self.history,
                egress,
                stop: self.stop.clone(),
            })?;
            tracing::info!(
                egress = args.egress.as_deref(),
                "kardamom-executor: canonical entries via the member feed"
            );
            return Ok((None, ExecutorOrdering::Carried(Box::new(sub))));
        }
        if let (Source::Egress, Some(base)) = (self.source, args.egress.clone()) {
            let transport = bin_support::open_egress_transport(self.rt, plane, base)?;
            let sub = CarriedSubscription::new(transport, NoSource, cursor).until_head();
            tracing::info!(
                egress = args.egress.as_deref(),
                "kardamom-executor: canonical entries via the range read of the egress, up to its head"
            );
            return Ok((None, ExecutorOrdering::Egress(Box::new(sub))));
        }
        let mut cluster_cfg = file_cfg.cluster.to_live();
        if let Some(endpoints) = plane
            .cluster_ingress_endpoints(self.rt.start_open_limit())
            .await?
        {
            cluster_cfg.ingress_endpoints = endpoints;
        }
        let (guard, sub) = bin_support::connect_cluster_ordering(aeron_dir, cluster_cfg, cursor)?;
        tracing::info!("kardamom-executor: tx_ordering via Aeron Cluster");
        Ok((Some(guard), ExecutorOrdering::Refs(sub)))
    }
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
