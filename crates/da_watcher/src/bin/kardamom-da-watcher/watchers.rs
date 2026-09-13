//! The spawned origin watchers' lifecycle: spawn, wait for shutdown or a
//! fail-stop, and shut each one down. Split out of the main binary file to
//! keep it under the file line bound (`docs/STYLE.md` R3).

use std::ops::ControlFlow;
use std::time::Duration;

use alloy_provider::ProviderBuilder;

use kardamom_da_watcher::interop::{
    CursorReconcile, WsRemoteChainSource, spawn as spawn_interop_watcher,
};
use kardamom_da_watcher::{RpcL1Source, WatcherHandle, spawn as spawn_watcher};
use kardamom_log::aeron_live::{TxDepositsPublisherHandle, TxRemoteEpochsPublisherHandle};
use kardamom_obs::bin::wait_for_shutdown;

use super::publishers::{LiveRemoteEpochsPublisher, LiveTxDepositsPublisher};
use super::{InteropPath, L1Path};

/// Which origin path a watcher handle belongs to. The halt loop keys on
/// this instead of a string, so the interop check cannot silently miss a
/// misspelled name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatcherKind {
    L1,
    Interop,
}

impl WatcherKind {
    /// The label used in watcher log lines and error messages.
    fn label(self) -> &'static str {
        match self {
            Self::L1 => "l1",
            Self::Interop => "interop",
        }
    }
}

/// The spawned origin watchers, plus whether an interop fail-stop alone
/// should end the process. Bundles the watcher lifecycle as methods here,
/// instead of five standalone functions each re-threading the same
/// `watchers: &[(WatcherKind, WatcherHandle)]` and `interop_fault_exits`
/// parameters.
pub(crate) struct Watchers {
    handles: Vec<(WatcherKind, WatcherHandle)>,
    interop_fault_exits: bool,
}

impl Watchers {
    /// Spawn one watcher per fully-configured path.
    pub(crate) async fn spawn(
        l1: Option<L1Path>,
        interop: Option<InteropPath>,
        tx_deposits_pub: Option<TxDepositsPublisherHandle>,
        tx_remote_epochs_pub: Option<TxRemoteEpochsPublisherHandle>,
        interop_fault_exits: bool,
    ) -> anyhow::Result<Self> {
        let mut handles: Vec<(WatcherKind, WatcherHandle)> = Vec::new();

        if let (Some(l1), Some(tx_deposits_pub)) = (l1, tx_deposits_pub) {
            let provider = ProviderBuilder::new()
                .connect(&l1.rpc)
                .await
                .map_err(|e| anyhow::anyhow!("failed to connect to L1 RPC {}: {e}", l1.rpc))?;
            tracing::info!(
                l1_rpc = %l1.rpc,
                lockbox = ?l1.cfg.lockbox,
                poll_interval = ?l1.cfg.poll_interval,
                "kardamom-da-watcher: publishing L1 epochs onto tx_deposits"
            );
            handles.push((
                WatcherKind::L1,
                spawn_watcher(
                    LiveTxDepositsPublisher::new(tx_deposits_pub),
                    RpcL1Source::new(provider),
                    l1.cfg,
                ),
            ));
        }

        if let (Some(mut interop), Some(tx_remote_epochs_pub)) = (interop, tx_remote_epochs_pub) {
            interop.reconcile().await?;
            tracing::info!(
                feed_url = %interop.feed_url,
                origin = interop.peer_chain_id,
                self_chain_id = interop.cfg.self_chain_id,
                start_seq = interop.cfg.start_seq,
                cursor_file = %interop.cursor_file.path().display(),
                dest_rpc = match &interop.cursor_reconcile {
                    CursorReconcile::Rpc(url) => url.as_str(),
                    CursorReconcile::Skip => "<skipped>",
                },
                "kardamom-da-watcher: publishing remote epochs onto tx_remote_epochs"
            );
            let source = WsRemoteChainSource::new(
                interop.peer_chain_id,
                interop.cfg.self_chain_id,
                interop.feed_url,
            );
            handles.push((
                WatcherKind::Interop,
                spawn_interop_watcher(
                    LiveRemoteEpochsPublisher::new(tx_remote_epochs_pub),
                    source,
                    interop.cfg,
                    Some(interop.cursor_file),
                ),
            ));
        }

        Ok(Self {
            handles,
            interop_fault_exits,
        })
    }

    /// Wait for SIGTERM (an orchestrator stop), Ctrl-C, or every watcher to
    /// fail-stop on its own, then ask each remaining watcher to exit.
    ///
    /// A watcher that finishes without being asked has fail-stopped: an
    /// interop derivation fault, a feed lag, or a closed publisher. Once
    /// the last watcher has fail-stopped, nothing is left to watch.
    /// Staying up as a healthy-looking husk would hide the halt from the
    /// orchestrator, so the process exits nonzero. This lets the
    /// supervisor, or an e2e harness, see the fail-stop as a process
    /// outcome, not just a log line.
    ///
    /// With `interop_fault_exits` set (the default) an interop fail-stop
    /// exits the process even while the L1 path still runs. The fault
    /// domain is still the pair on the chain: the L1 path restarts with
    /// the process. Without the flag, the L1 path keeps the process up
    /// and the halt shows only in the log and the metric.
    pub(crate) async fn await_shutdown_or_fail_stop(self) -> anyhow::Result<()> {
        let fail_stopped: Option<&'static str> = tokio::select! {
            () = wait_for_shutdown() => None,
            halt = self.watch_for_halt() => Some(halt),
        };
        for (kind, handle) in self.handles {
            Self::shutdown_one(kind, handle).await?;
        }
        if let Some(who) = fail_stopped {
            tracing::error!(
                "{who} fail-stopped (no shutdown was requested); exiting nonzero so the halt is \
                 a process outcome"
            );
            anyhow::bail!(
                "{who} fail-stopped (no shutdown was requested); exiting nonzero so the halt is \
                 a process outcome"
            );
        }
        Ok(())
    }

    /// Poll every watcher every 200 ms until one of the two halt
    /// conditions [`Self::check_halt`] names is true, and return which
    /// one fired.
    async fn watch_for_halt(&self) -> &'static str {
        loop {
            match self.poll_halt_step().await {
                ControlFlow::Break(reason) => return reason,
                ControlFlow::Continue(()) => {}
            }
        }
    }

    /// One [`Self::watch_for_halt`] poll: check the halt conditions, then
    /// wait 200 ms before the caller polls again.
    async fn poll_halt_step(&self) -> ControlFlow<&'static str> {
        let result = self.check_halt();
        if result.is_continue() {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        result
    }

    /// The two conditions [`Self::watch_for_halt`] polls for: every
    /// watcher finished on its own, or (with `interop_fault_exits`) the
    /// interop watcher alone finished. Neither means a shutdown was
    /// requested — see [`Self::await_shutdown_or_fail_stop`].
    fn check_halt(&self) -> ControlFlow<&'static str> {
        if self.handles.iter().all(|(_, h)| h.task.is_finished()) {
            return ControlFlow::Break("every configured watcher");
        }
        if self.interop_fault_exits
            && self
                .handles
                .iter()
                .any(|(kind, h)| *kind == WatcherKind::Interop && h.task.is_finished())
        {
            return ControlFlow::Break("the interop watcher");
        }
        ControlFlow::Continue(())
    }

    /// One watcher's shutdown: log if it exited on its own (a fail-stop,
    /// not a requested shutdown), ask it to stop, then join it.
    async fn shutdown_one(kind: WatcherKind, handle: WatcherHandle) -> anyhow::Result<()> {
        let name = kind.label();
        if handle.task.is_finished() {
            tracing::error!(watcher = name, "watcher exited without a shutdown request");
        }
        let _ = handle.shutdown.send(());
        handle
            .task
            .await
            .map_err(|e| anyhow::anyhow!("{name} watcher task panicked: {e}"))?;
        Ok(())
    }
}
