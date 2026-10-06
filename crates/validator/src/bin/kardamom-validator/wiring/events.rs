//! The validator's place on the `events` stream, for the whole process.
//! Each revolution opens and closes its own stream plane, and a held
//! divergence runs between revolutions, so the events have a runtime and
//! a plane of their own: the chain status shows the validator halted,
//! not gone, while it holds.
//!
//! It publishes the process's lifecycle and, when the attester is on,
//! the attester's. It keeps the board of every service's state, and
//! pauses the attester while any live validator stands halted on a
//! divergence.

use std::ops::ControlFlow;
use std::path::Path;

use anyhow::{Context, Result};
use kardamom_log::aeron_live::{
    AeronRuntime, ServiceEventsPublisherHandle, ServiceEventsSubscriberHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;
use kardamom_obs::events::{Beacon, BoardView, Identity};
use kardamom_obs::lifecycle::{Slots, process};
use kardamom_validator::attester::AttesterGate;
use tokio::sync::watch;

/// What [`ValidatorEvents::open`] takes: the transport config, this
/// host, and whether the attester runs.
pub(crate) struct EventsConfig<'a> {
    pub(crate) log_cfg: &'a LogConfig,
    pub(crate) aeron_dir: Option<&'a Path>,
    pub(crate) host_id: &'a str,
    pub(crate) attester_on: bool,
}

/// The events transport and the attester's gate. Field order is drop
/// order: the plane ends in [`Self::close`], then the runtime drops.
pub(crate) struct ValidatorEvents {
    plane: StreamPlane,
    _rt: AeronRuntime,
    gate: AttesterGate,
}

impl ValidatorEvents {
    /// Open the transport, publish the lifecycles, and start the board
    /// and the attester's watch.
    ///
    /// # Errors
    ///
    /// Returns an error when the plane, the runtime, or a handle fails to
    /// open.
    pub(crate) async fn open(cfg: &EventsConfig<'_>) -> Result<Self> {
        let mut plane = StreamPlane::from_config(cfg.log_cfg, "validator-events")
            .context("build the events stream plane")?;
        let rt = AeronRuntime::spawn(cfg.aeron_dir).context("spawn events runtime")?;
        let events: ServiceEventsPublisherHandle =
            plane.publisher(&rt).await.context("open events")?;
        events.spawn_process_beacon();
        let gate = AttesterGate::new();
        if cfg.attester_on {
            events.spawn_beacon(Beacon::new(
                Identity {
                    service: AttesterGate::SERVICE.to_string(),
                    instance: cfg.host_id.to_string(),
                },
                gate.lifecycle().subscribe(),
            ));
        }
        let board = plane
            .subscriber::<ServiceEventsSubscriberHandle>(&rt)
            .context("open the events subscription")?
            .spawn_board();
        AttesterWatch {
            gate: gate.clone(),
            board,
            own: process().subscribe(),
            instance: cfg.host_id.to_string(),
        }
        .spawn();
        Ok(Self {
            plane,
            _rt: rt,
            gate,
        })
    }

    /// The attester's gate, for the attester each revolution spawns.
    pub(crate) fn attester_gate(&self) -> AttesterGate {
        self.gate.clone()
    }

    /// End the plane's registrations before the runtime drops.
    pub(crate) async fn close(self) {
        self.plane.shutdown().await;
    }
}

/// The task that pauses the attester on a divergence: this validator's
/// own, or one on the board.
struct AttesterWatch {
    gate: AttesterGate,
    board: watch::Receiver<BoardView>,
    own: watch::Receiver<Slots>,
    instance: String,
}

impl AttesterWatch {
    fn spawn(mut self) {
        tokio::spawn(async move { while self.step().await.is_continue() {} });
    }

    /// Wait for a change of the board or of this process; then follow
    /// the root. `Break` when either source closes.
    async fn step(&mut self) -> ControlFlow<()> {
        let changed = tokio::select! {
            changed = self.board.changed() => changed,
            changed = self.own.changed() => changed,
        };
        if changed.is_err() {
            return ControlFlow::Break(());
        }
        self.gate.follow(AttesterGate::root(
            &self.own.borrow(),
            &self.instance,
            &self.board.borrow(),
        ));
        ControlFlow::Continue(())
    }
}
