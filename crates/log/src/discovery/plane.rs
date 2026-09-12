//! The stream plane: the one seam a service opens its channels through.
//!
//! With discovery off, every handle opens on its static `[channels]`
//! URI, unchanged. With discovery on, a publisher opens a dynamic MDC
//! publication on the advertised interface and registers it, and a
//! subscriber opens one multi-destination subscription and runs a
//! reconcile task that attaches every discovered publisher.
//!
//! The reconcile task holds a command-only [`Destinations`] handle, never
//! an [`AeronRuntime`] clone, so it can outlive nothing: the runtime shuts
//! down when its last owner drops, and the task ends on cancel.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::catalog::{Catalog, RegistrationSpec};
use super::endpoint::{MANUAL_SUBSCRIPTION_URI, PortAllocator, advertise_ip, publication_uri};
use super::reconcile::{DestinationPort, Reconciler};
use super::record::{PUBLISHER_SERVICE, PublisherRecord, Scope, Topic};
use super::registration::Registration;
use super::watch::{Membership, MembershipWatch, WatchTiming};
use super::{Instance, catalog_from_config, scope_from_config};
use crate::aeron_live::{AeronRuntime, Destinations, PubHandle, TypedSubscription};
use crate::codec::WireMessage;
use crate::config::{ChannelsConfig, DiscoveryConfig, LogConfig};
use crate::error::LogError;

/// A publisher handle the plane can open either way.
pub trait DiscoveredPublisher: Sized {
    const TOPIC: Topic;
    fn stream_id(ch: &ChannelsConfig) -> i32;
    /// # Errors
    ///
    /// Returns an error if the static publication fails to open.
    fn open_static(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError>;
    fn from_publication(inner: PubHandle) -> Self;
}

/// A single-stream subscriber handle the plane can open either way.
pub trait DiscoveredSubscriber: Sized {
    const TOPIC: Topic;
    type Msg: WireMessage;
    fn stream_id(ch: &ChannelsConfig) -> i32;
    /// # Errors
    ///
    /// Returns an error if the static subscription fails to open.
    fn open_static(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError>;
    fn from_subscription(rx: TypedSubscription<Self::Msg>) -> Self;
}

/// One stream a discovered publication or subscription is keyed on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamKey {
    pub topic: Topic,
    pub stream_id: i32,
    pub lane: Option<u8>,
}

impl StreamKey {
    /// The catalog filter of every publisher of this stream in `scope`.
    fn filter(self, scope: &Scope) -> BTreeMap<String, String> {
        let mut meta = scope.meta();
        meta.insert("topic".into(), self.topic.as_str().into());
        meta.insert("stream_id".into(), self.stream_id.to_string());
        if let Some(lane) = self.lane {
            meta.insert("lane_id".into(), lane.to_string());
        }
        meta
    }
}

impl DestinationPort for Destinations {
    fn attach(&self, uri: &str) -> Result<(), LogError> {
        self.add(uri)
    }

    fn detach(&self, uri: &str) -> Result<(), LogError> {
        self.remove(uri)
    }
}

/// The discovery side of a plane: catalog, identity, endpoints, and the
/// tasks and registrations opened so far.
struct Discovered {
    cfg: DiscoveryConfig,
    scope: Scope,
    catalog: Catalog,
    instance: Instance,
    ip: Ipv4Addr,
    ports: PortAllocator,
    /// The `publisher_id` label of every record this process registers.
    label: String,
    cancel: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    registrations: Vec<Registration>,
}

impl Discovered {
    /// Open a dynamic MDC publication for `key` and register it.
    async fn open_publication(
        &mut self,
        rt: &AeronRuntime,
        key: StreamKey,
    ) -> Result<PubHandle, LogError> {
        let port = self.ports.allocate()?;
        let control = SocketAddr::new(IpAddr::V4(self.ip), port);
        let uri = publication_uri(control, &self.cfg.flow_control);
        let publication = rt.open_publication(&uri, key.stream_id)?;
        let record = PublisherRecord {
            id: self.instance.service_id(key.topic, key.stream_id),
            control,
            topic: key.topic,
            stream_id: key.stream_id,
            lane: key.lane,
            publisher_id: self.label.clone(),
            session_id: None,
        };
        let spec = RegistrationSpec {
            entry: record.entry(&self.scope),
            ttl: self.cfg.check_ttl(),
            deregister_after: self.cfg.deregister_after(),
        };
        let registration = Registration::register(self.catalog.clone(), spec).await?;
        info!(topic = %key.topic, stream_id = key.stream_id, %control, "discovery: publication open");
        self.registrations.push(registration);
        Ok(publication)
    }

    /// Open a multi-destination subscription for `key` and start the
    /// watch and reconcile tasks that attach its publishers.
    fn open_subscription<T: WireMessage>(
        &mut self,
        rt: &AeronRuntime,
        key: StreamKey,
    ) -> Result<TypedSubscription<T>, LogError> {
        let (sub_id, rx) =
            rt.open_subscription_with_id::<T>(MANUAL_SUBSCRIPTION_URI, key.stream_id)?;
        self.start_reconcile(rt.destinations(sub_id), key);
        Ok(rx)
    }

    fn start_reconcile(&mut self, port: Destinations, key: StreamKey) {
        let (watch, membership) = MembershipWatch::new(
            self.catalog.clone(),
            PUBLISHER_SERVICE.into(),
            key.filter(&self.scope),
            WatchTiming::from_config(&self.cfg),
            self.cancel.clone(),
        );
        let attach = ReconcileTask {
            key,
            membership,
            reconciler: Some(Reconciler::new(
                IpAddr::V4(self.ip),
                self.cfg.removal_grace(),
            )),
            port: Some(port),
            cancel: self.cancel.clone(),
            tick: self.cfg.removal_grace().max(Duration::from_millis(500)) / 2,
        };
        self.tasks.push(tokio::spawn(watch.run()));
        self.tasks.push(tokio::spawn(attach.run()));
        info!(topic = %key.topic, stream_id = key.stream_id, "discovery: subscription open");
    }

    async fn shutdown(self) {
        self.cancel.cancel();
        for task in self.tasks {
            let _ = task.await;
        }
        for registration in self.registrations {
            if let Err(e) = registration.deregister().await {
                warn!(error = %e, "discovery: deregistration failed at shutdown");
            }
        }
    }
}

/// The reconcile loop of one subscription: re-plan on every membership
/// change and on a timer, so a removal grace expires without a new
/// snapshot.
struct ReconcileTask {
    key: StreamKey,
    membership: watch::Receiver<Membership>,
    /// Taken during a blocking apply and put back after; never `None`
    /// between steps.
    reconciler: Option<Reconciler>,
    port: Option<Destinations>,
    cancel: CancellationToken,
    tick: Duration,
}

impl ReconcileTask {
    async fn run(mut self) {
        while self.step().await {}
    }

    /// Wait for a change or the tick, then plan and apply. `false` ends
    /// the loop: cancelled, or the watch is gone.
    async fn step(&mut self) -> bool {
        tokio::select! {
            () = self.cancel.cancelled() => return false,
            changed = self.membership.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            () = tokio::time::sleep(self.tick) => {}
        }
        self.apply().await;
        true
    }

    /// One plan-and-apply pass. The driver calls block, so they run on
    /// a blocking thread with the reconciler and port moved across and
    /// back.
    async fn apply(&mut self) {
        let snapshot = self.membership.borrow_and_update().clone();
        let (Some(mut reconciler), Some(port)) = (self.reconciler.take(), self.port.take()) else {
            return;
        };
        let key = self.key;
        let outcome = tokio::task::spawn_blocking(move || {
            let plan = reconciler.plan(&snapshot, Instant::now());
            if !plan.attach.is_empty() || !plan.detach.is_empty() {
                info!(
                    topic = %key.topic,
                    stream_id = key.stream_id,
                    attach = plan.attach.len(),
                    detach = plan.detach.len(),
                    "discovery: reconciling publishers"
                );
            }
            reconciler.apply(&plan, &port, &snapshot);
            (reconciler, port)
        })
        .await;
        match outcome {
            Ok((reconciler, port)) => {
                self.reconciler = Some(reconciler);
                self.port = Some(port);
            }
            Err(e) => {
                warn!(error = %e, "discovery: reconcile pass panicked; subscription stays as is");
            }
        }
    }
}

/// The seam every service opens its channels through. See the module
/// doc.
pub struct StreamPlane {
    channels: ChannelsConfig,
    discovered: Option<Discovered>,
}

impl StreamPlane {
    /// Build the plane from a resolved `LogConfig`. `label` is the
    /// `publisher_id` of every record this process registers, for logs.
    ///
    /// # Errors
    ///
    /// Returns an error if discovery is enabled and the environment, the
    /// interface, or the Consul client cannot be resolved.
    pub fn from_config(cfg: &LogConfig, label: &str) -> Result<Self, LogError> {
        if !cfg.discovery.enabled {
            return Ok(Self::static_only(cfg.channels.clone()));
        }
        let selector = cfg.discovery.advertise_interface.as_ref().ok_or_else(|| {
            LogError::Discovery("discovery enabled without an advertise interface".into())
        })?;
        let ip = advertise_ip(selector)?;
        let instance = Instance::from_env()?;
        let catalog = catalog_from_config(&cfg.discovery)?;
        Ok(Self::with_catalog(cfg, label, catalog, instance, ip))
    }

    /// A plane over a given catalog, identity, and address: the
    /// production path once resolved, and the test path over the
    /// in-memory catalog.
    #[must_use]
    pub fn with_catalog(
        cfg: &LogConfig,
        label: &str,
        catalog: Catalog,
        instance: Instance,
        ip: Ipv4Addr,
    ) -> Self {
        let ports = PortAllocator::new(ip, instance.ports);
        Self {
            channels: cfg.channels.clone(),
            discovered: Some(Discovered {
                cfg: cfg.discovery.clone(),
                scope: scope_from_config(&cfg.discovery),
                catalog,
                instance,
                ip,
                ports,
                label: label.to_string(),
                cancel: CancellationToken::new(),
                tasks: Vec::new(),
                registrations: Vec::new(),
            }),
        }
    }

    /// A plane that opens every handle on its static channel.
    #[must_use]
    pub fn static_only(channels: ChannelsConfig) -> Self {
        Self {
            channels,
            discovered: None,
        }
    }

    #[must_use]
    pub fn channels(&self) -> &ChannelsConfig {
        &self.channels
    }

    #[must_use]
    pub fn is_discovered(&self) -> bool {
        self.discovered.is_some()
    }

    /// Open publisher handle `H`.
    ///
    /// # Errors
    ///
    /// Returns an error if the publication fails to open or, when
    /// discovered, if no port binds or the registration fails.
    pub async fn publisher<H: DiscoveredPublisher>(
        &mut self,
        rt: &AeronRuntime,
    ) -> Result<H, LogError> {
        let key = StreamKey {
            topic: H::TOPIC,
            stream_id: H::stream_id(&self.channels),
            lane: None,
        };
        match &mut self.discovered {
            None => H::open_static(rt, &self.channels),
            Some(d) => d.open_publication(rt, key).await.map(H::from_publication),
        }
    }

    /// Open subscriber handle `H`.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription fails to open.
    pub fn subscriber<H: DiscoveredSubscriber>(
        &mut self,
        rt: &AeronRuntime,
    ) -> Result<H, LogError> {
        let key = StreamKey {
            topic: H::TOPIC,
            stream_id: H::stream_id(&self.channels),
            lane: None,
        };
        match &mut self.discovered {
            None => H::open_static(rt, &self.channels),
            Some(d) => d
                .open_subscription::<H::Msg>(rt, key)
                .map(H::from_subscription),
        }
    }

    /// Stop the watch and reconcile tasks and deregister every
    /// publication. Call this before the Aeron runtime drops.
    pub async fn shutdown(self) {
        if let Some(d) = self.discovered {
            d.shutdown().await;
        }
    }
}
