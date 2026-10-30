//! The sequencer's background feeds, and the publish-loop spawner.
//!
//! Three loops feed, or drain, the publish path. The cluster-egress
//! watermark task and the `tx_receipts` floor task supply the
//! `ResyncController`: a replica that falls behind its twin past the
//! cluster's dedup window can double-order a transaction, so these feeds
//! detect the lag and drive the sequencer into a receipt-proof-only
//! publish mode until it clears. The nonce lookup task answers the
//! core's requests with an executor's committed nonce, as floor evidence
//! of the same kind as a receipt. `PublishLoops::spawn` runs the
//! canonical `TxRef` loop and the `DepositRef` epoch pump on blocking
//! tasks.
//!
//! Seam rule: the async shell is tokio. Anything that blocks, such as
//! Aeron polls, the crossbeam egress receiver, or the microsecond-backoff
//! publish loop, runs on `spawn_blocking`. It polls `Shutdown::is_signaled`.
//! Async-capable work, such as the receipts fan-in on an existing tokio
//! channel, is a plain task. It uses `select!` on `Shutdown::cancelled`.

mod lookup;
mod origin_pump;
mod receipts;
mod verdicts;

use std::sync::Arc;

use kardamom_log::aeron_live::{
    IdleCap, TxDataSubscriberHandle, TxDepositsSubscriberHandle,
    TxReceiptsBoundarySubscriberHandle, TxRemoteEpochsSubscriberHandle,
};
use kardamom_obs::lifecycle::process;
pub(crate) use kardamom_sequencer::account_lookup::{AccountLookupFeed, LookupSources, Retries};
use kardamom_sequencer::config::SequencerConfig;
pub(crate) use kardamom_sequencer::egress_feed::EgressWatermarkFeed;
use kardamom_sequencer::epoch::EpochPump;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::fees::{FeeGate, LatestBaseFee};
use kardamom_sequencer::inbound::{Inbound, TxDataSubscriber};
use kardamom_sequencer::lookup::LookupRequester;
use kardamom_sequencer::outbound::{SideChannels, TxOrderingRefPublisher};
use kardamom_sequencer::pump::Pump;
use kardamom_sequencer::resync::ResyncController;
use kardamom_sequencer::sealer_rules::SealerVersion;
use kardamom_sequencer::sequencer::{Ports, Sequencer, Shutdown};
pub(crate) use lookup::NonceLookupFeed;
use origin_pump::OriginPump;
pub(crate) use receipts::{ReceiptFloorFeed, ReceiptSink};
pub(crate) use verdicts::VerdictFeed;

/// One `tx_data` lane subscription: the lane index the sequencer stamps
/// into every ref off it, and the handle that reads it.
pub(crate) struct LaneSub {
    pub(crate) lane: u8,
    pub(crate) handle: TxDataSubscriberHandle,
}

/// The live `tx_data` subscription set: one handle per lane the sequencer
/// reads. The own lane comes first. A resize adds the old lanes. The poll
/// walks the lanes in rotation, so a busy old lane cannot starve the own
/// lane. Never empty: the constructor takes the own lane apart from the
/// old ones.
pub(crate) struct LaneSubscriptions {
    lanes: Vec<LaneSub>,
    /// The lane the next poll tries first.
    next: usize,
}

impl LaneSubscriptions {
    pub(crate) fn new(own: LaneSub, old: Vec<LaneSub>) -> Self {
        let lanes = std::iter::once(own).chain(old).collect();
        Self { lanes, next: 0 }
    }
}

impl TxDataSubscriber for LaneSubscriptions {
    fn poll(&mut self) -> Result<Option<Inbound>, SequencerError> {
        // try_recv is non-blocking. The Sequencer's run loop handles
        // backoff when poll returns None.
        let n = self.lanes.len();
        let lanes = &mut self.lanes;
        let found = (0..n)
            .map(|i| self.next.saturating_add(i) % n)
            .find_map(|idx| {
                let sub = &mut lanes[idx];
                sub.handle.try_recv_positioned().map(|data| {
                    let inbound = Inbound {
                        lane: sub.lane,
                        loc: data.loc,
                        stream_position: data.stream_position,
                        envelope: data.envelope,
                    };
                    (idx, inbound)
                })
            });
        Ok(found.map(|(idx, inbound)| {
            self.next = idx.saturating_add(1) % n;
            inbound
        }))
    }

    fn queued(&self) -> usize {
        self.lanes.iter().map(|sub| sub.handle.len()).sum()
    }
}

/// The executor-boundary feed into the base fee view: every closed
/// block's base fee and gas used imply the next block's base fee, which
/// the fee gate checks caps against. A plain async task, like the
/// receipts feed: the handle fans in over a tokio channel.
pub(crate) struct BaseFeeFeed {
    view: Arc<LatestBaseFee>,
}

impl BaseFeeFeed {
    pub(crate) fn new(view: Arc<LatestBaseFee>) -> Self {
        Self { view }
    }

    pub(crate) fn spawn(
        self,
        mut sub: TxReceiptsBoundarySubscriberHandle,
        shutdown: Shutdown,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let boundary = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    msg = sub.recv() => match msg {
                        Some((_pos, boundary)) => boundary,
                        None => return,
                    },
                };
                self.view.on_boundary(&boundary);
            }
        })
    }
}

pub(crate) type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;

/// Argument group for [`PublishLoops::spawn`]: the config, the `tx_data`
/// lane subscriptions, the three `tx_ordering` publishers, the side
/// channels, the two origin subscriptions, the resync controller, the
/// nonce lookup requester, and one shutdown token. Each spawned loop
/// clones the token itself.
pub(crate) struct PublishLoops<P> {
    pub(crate) cfg: SequencerConfig,
    pub(crate) tx_data: LaneSubscriptions,
    /// The canonical `TxRef` loop's publisher.
    pub(crate) main_pub: P,
    /// The L1 epoch pump's publisher.
    pub(crate) epoch_pub: P,
    /// The interop remote-epoch pump's publisher.
    pub(crate) remote_epoch_pub: P,
    pub(crate) side: SideChannels,
    /// The fee admission gate for the canonical loop.
    pub(crate) fees: FeeGate,
    pub(crate) epochs: TxDepositsSubscriberHandle,
    /// The epoch lane, whose signal sender the egress feed holds.
    pub(crate) epoch_lane: EpochPump,
    pub(crate) remote_epochs: TxRemoteEpochsSubscriberHandle,
    pub(crate) resync: Option<ResyncController>,
    /// `None` when the binary has no executor endpoints.
    pub(crate) lookup: Option<LookupRequester>,
    /// The decision version that the egress feed reads off the sealer.
    pub(crate) sealer_version: SealerVersion,
    /// The inputs of the v2 loop, when `[admission]` turns it on.
    pub(crate) v2: Option<V2Wiring>,
    pub(crate) shutdown: Shutdown,
    /// The idle cap of the process, for the main sequencer loop.
    pub(crate) idle_cap: IdleCap,
}

/// The inputs of the v2 loop: the account events of the receipts feed and
/// of the lookup task, and the lookup queue. With `verdicts`, the loop
/// sends the receipt of each forwarded transaction to the verdict task.
pub(crate) struct V2Wiring {
    pub(crate) accounts: crossbeam_channel::Receiver<kardamom_sequencer::v2::AccountEvent>,
    pub(crate) lookups: tokio::sync::mpsc::Sender<alloy_primitives::Address>,
    pub(crate) verdicts: Option<tokio::sync::mpsc::Sender<kardamom_sequencer::v2::VerdictOut>>,
}

impl V2Wiring {
    /// Run the v2 loop in place of the v1 loop.
    fn run<P: kardamom_sequencer::sequencer::SequencerPorts>(
        self,
        cfg: &SequencerConfig,
        fees: FeeGate,
        sealer_version: SealerVersion,
        ports: &mut P,
        shutdown: &Shutdown,
        idle_cap: IdleCap,
    ) -> Result<(), SequencerError> {
        let vslots = cfg.vslot_set()?;
        let mut admission = kardamom_sequencer::v2::AdmissionLoop::new(
            cfg.admission.loop_config(cfg.tx_ttl(), cfg.partition_index),
            vslots,
            self.accounts,
            self.lookups,
        );
        admission.enable_fees(fees);
        admission.enable_positioned_refs(sealer_version);
        admission.enable_pause(process().subscribe());
        if let Some(verdicts) = self.verdicts {
            admission.enable_verdicts(verdicts);
        }
        tracing::info!(
            partition = cfg.partition_index,
            "sequencer v2: the admission loop runs"
        );
        admission.run(ports, shutdown, idle_cap)
    }
}

impl<P> PublishLoops<P>
where
    P: TxOrderingRefPublisher + Send + 'static,
{
    /// Spawn the main sequencer loop and the two origin pumps, over three
    /// `TxOrderingRefPublisher`s. This is generic over the publisher
    /// type, so the Aeron and cluster branches share one implementation.
    /// Both branches supply concrete publishers that implement the
    /// trait.
    ///
    /// The two origin pumps run as separate loops, not one merged poll,
    /// because the origins are independent: a peer whose feed has
    /// stalled must not delay L1 deposits, and an L1 RPC outage must not
    /// stall cross-chain delivery.
    pub(crate) fn spawn(self) -> (LoopHandle, LoopHandle, LoopHandle) {
        let Self {
            cfg,
            mut tx_data,
            mut main_pub,
            epoch_pub,
            remote_epoch_pub,
            mut side,
            fees,
            epochs: epoch_subscription,
            epoch_lane,
            remote_epochs: remote_epoch_subscription,
            resync,
            lookup,
            sealer_version,
            v2,
            shutdown,
            idle_cap,
        } = self;

        // The sequencer main loop is sync (it uses std::thread and
        // std::thread::sleep for backoff). Hand it to spawn_blocking, so
        // the async runtime stays responsive for shutdown handling.
        let shutdown_for_main = shutdown.clone();
        let join_main = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
            let mut ports = Ports {
                tx_data: &mut tx_data,
                refs: &mut main_pub,
                errors: &mut side,
            };
            if let Some(v2) = v2 {
                return v2.run(
                    &cfg,
                    fees,
                    sealer_version,
                    &mut ports,
                    &shutdown_for_main,
                    idle_cap,
                );
            }
            let mut sequencer = Sequencer::new(cfg)?;
            if let Some(controller) = resync {
                sequencer.enable_resync(controller);
            }
            if let Some(requester) = lookup {
                sequencer.enable_nonce_lookup(requester);
            }
            sequencer.enable_pause(process().subscribe());
            sequencer.enable_fees(fees);
            sequencer.enable_positioned_refs(sealer_version);
            sequencer.run(&mut ports, &shutdown_for_main, idle_cap)
        });

        // Independent pump for tx_deposits to epoch on tx_ordering. The
        // epoch path is not nonce-gated. It is a simple poll-and-publish
        // loop that runs alongside the canonical TxData-to-TxRef path. It
        // stays on spawn_blocking. The epoch lane does a sync Aeron poll
        // and a sync cluster offer. So the loop polls `is_signaled`
        // between backoff sleeps. The lane keeps every epoch until a
        // boundary confirms it, and offers the epochs again on an
        // origin-gap reject, so an epoch the cluster lost is never
        // dropped.
        let shutdown_for_deposits = shutdown.clone();
        let join_deposits = tokio::task::spawn_blocking(move || {
            OriginPump::new(
                shutdown_for_deposits,
                epoch_subscription,
                epoch_pub,
                epoch_lane,
            )
            .run()
        });

        // Independent pump for tx_remote_epochs to a remote-origin record
        // on tx_ordering, on the same terms as the deposit pump above,
        // with a one-slot retry.
        let shutdown_for_remote_epochs = shutdown.clone();
        let join_remote_epochs = tokio::task::spawn_blocking(move || {
            OriginPump::new(
                shutdown_for_remote_epochs,
                remote_epoch_subscription,
                remote_epoch_pub,
                Pump::<kardamom_types::xchain::RemoteEpochRecord>::default(),
            )
            .run()
        });

        (join_main, join_deposits, join_remote_epochs)
    }
}
