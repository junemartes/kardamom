//! The `events` stream's two ends: a beacon task that publishes a
//! lifecycle's records, and a board task that keeps every service's
//! latest state. The state machine, the heartbeat, and the expiry live in
//! `kardamom_obs::events`; this module moves the records over Aeron.

use std::ops::ControlFlow;

use kardamom_obs::events::{Beacon, BoardFeed, BoardView};
use kardamom_types::service::ServiceEvent;
use tokio::sync::{mpsc, watch};

use super::simple::{ServiceEventsPublisherHandle, ServiceEventsSubscriberHandle};

impl ServiceEventsPublisherHandle {
    /// Publish `beacon`'s records on a detached task, until its
    /// lifecycle drops.
    pub fn spawn_beacon(&self, beacon: Beacon) {
        let publisher = self.clone();
        tokio::spawn(beacon.run(move |event| publisher.publish_logged(event)));
    }

    /// Publish this process's records on a detached task. Nothing before
    /// the exporter is installed: the identity comes from it.
    pub fn spawn_process_beacon(&self) {
        if let Some(beacon) = Beacon::of_process() {
            self.spawn_beacon(beacon);
        }
    }

    /// Publish one record. A record that fails to encode is logged; the
    /// next heartbeat repeats the state.
    fn publish_logged(&self, event: &ServiceEvent) {
        if let Err(error) = self.publish_best_effort(event) {
            tracing::warn!(%error, service = %event.service, "service event not published");
        }
    }
}

impl ServiceEventsSubscriberHandle {
    /// Keep the latest state of every service on a task, and return the
    /// receiver of the board's view.
    #[must_use]
    pub fn spawn_board(mut self) -> watch::Receiver<BoardView> {
        let (feed, tx, view) = BoardFeed::new();
        tokio::spawn(feed.run());
        tokio::spawn(async move { while self.forward(&tx).await.is_continue() {} });
        view
    }

    /// Move one record from the stream to the board. `Break` when the
    /// stream or the board ends.
    async fn forward(&mut self, tx: &mpsc::Sender<ServiceEvent>) -> ControlFlow<()> {
        let Some((_, event)) = self.recv().await else {
            return ControlFlow::Break(());
        };
        match tx.send(event).await {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }
}
