//! Webhook subscriptions: the registry, the per-subscription outbox,
//! and the delivery loop.
//!
//! A subscription is `{ url, filter, secret }`. Its id is the hash of
//! the three, so a repeated registration is the same subscription. Every
//! instance stores every subscription on disk; the instance the
//! subscription id hashes to ([`InstanceSet`]) delivers it.
//!
//! An owned subscription runs two tasks. The appender takes the live
//! events the filter selects and appends them to the outbox. The
//! deliverer reads the outbox from a persisted cursor and posts each
//! event, at least once: exponential retries with a one-minute cap, ten
//! attempts, then it gives up on that event and moves on. The subscriber
//! deduplicates by the idempotency key, `tx_hash` plus stage.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::Duration;

use alloy_primitives::{B256, keccak256};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::delivery::{Worker, WorkerSpec};
use crate::dto::{StatusFilter, WebhookAck, WebhookRequest};
use crate::hub::HubHandle;
use crate::metrics as m;
use crate::outbox::OutboxError;
use crate::ring::Stamped;
use crate::shard::InstanceSet;

/// The signature header: `sha256=<hex HMAC-SHA256 of the body>`.
pub const SIGNATURE_HEADER: &str = "x-kardamom-signature";
/// The idempotency key header: `<tx_hash>:<stage>`.
pub const IDEMPOTENCY_HEADER: &str = "x-kardamom-idempotency-key";
/// The subscription id header.
pub const SUBSCRIPTION_HEADER: &str = "x-kardamom-subscription";

/// A stored subscription.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscription {
    pub id: B256,
    pub url: String,
    pub filter: StatusFilter,
    pub secret: String,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RegisterError {
    #[error("the url is not an http or https url: {0}")]
    BadUrl(String),
    #[error("the secret is empty")]
    EmptySecret,
    #[error("the registry is not running")]
    Closed,
    #[error("the subscription could not be stored: {0}")]
    Store(String),
}

impl Subscription {
    /// Parse a registration. The id is the hash of the request, so the
    /// same request names the same subscription on every instance.
    ///
    /// # Errors
    ///
    /// Returns an error for a URL that is not `http` or `https`, or an
    /// empty secret.
    pub fn parse(request: WebhookRequest) -> Result<Self, RegisterError> {
        let url =
            reqwest::Url::parse(&request.url).map_err(|e| RegisterError::BadUrl(e.to_string()))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(RegisterError::BadUrl(url.scheme().to_string()));
        }
        if request.secret.is_empty() {
            return Err(RegisterError::EmptySecret);
        }
        let id = keccak256(
            serde_json::to_vec(&request)
                .map_err(|e| RegisterError::Store(format!("serialize the registration: {e}")))?,
        );
        Ok(Self {
            id,
            url: request.url,
            filter: request.filter,
            secret: request.secret,
        })
    }

    /// The HMAC-SHA256 signature of `body` under this subscription's
    /// secret, as the signature header carries it.
    ///
    /// # Panics
    ///
    /// Never: HMAC accepts a key of any length.
    #[must_use]
    pub fn sign(&self, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_bytes())
            .expect("HMAC accepts a key of any length");
        mac.update(body);
        format!(
            "sha256={}",
            alloy_primitives::hex::encode(mac.finalize().into_bytes())
        )
    }
}

/// The webhook component's settings.
#[derive(Clone, Debug)]
pub struct WebhooksConfig {
    /// The directory of the subscription files, outboxes and cursors.
    pub dir: PathBuf,
    pub instances: InstanceSet,
    /// A fully delivered outbox longer than this is cut to zero.
    pub retain_bytes: u64,
    pub request_timeout: Duration,
    /// Live events queued per owned subscription ahead of its appender.
    pub queue: NonZeroUsize,
}

/// One registration from the HTTP endpoint.
pub struct Register {
    pub request: WebhookRequest,
    pub reply: oneshot::Sender<Result<WebhookAck, RegisterError>>,
}

/// The registry's client side.
#[derive(Clone)]
pub struct Registrar {
    tx: mpsc::Sender<Register>,
}

impl Registrar {
    /// Register `request`, or find it registered already.
    ///
    /// # Errors
    ///
    /// Returns the registry's refusal, or `Closed` when it is gone.
    pub async fn register(&self, request: WebhookRequest) -> Result<WebhookAck, RegisterError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Register { request, reply })
            .await
            .map_err(|_| RegisterError::Closed)?;
        rx.await.map_err(|_| RegisterError::Closed)?
    }
}

/// The fan-out task: live events into the owned outboxes, and the
/// registry.
pub struct Webhooks {
    cfg: WebhooksConfig,
    live: broadcast::Receiver<Stamped>,
    registry: mpsc::Receiver<Register>,
    http: reqwest::Client,
    known: HashSet<B256>,
    workers: HashMap<B256, Worker>,
    shutdown: CancellationToken,
}

#[derive(Debug, Error)]
pub enum WebhookError {
    #[error("webhook directory {0}: {1}")]
    Dir(PathBuf, std::io::Error),
    #[error("subscription file {0}: {1}")]
    File(PathBuf, String),
    #[error(transparent)]
    Outbox(#[from] OutboxError),
    #[error("http client: {0}")]
    Http(#[from] reqwest::Error),
}

enum Step {
    Live(Result<Stamped, broadcast::error::RecvError>),
    Register(Register),
    Stop,
}

impl Webhooks {
    /// Load the stored subscriptions and start the owned ones.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory or a stored subscription cannot
    /// be read, or an outbox cannot be opened.
    pub fn start(
        cfg: WebhooksConfig,
        hub: &HubHandle,
        shutdown: CancellationToken,
    ) -> Result<(Self, Registrar), WebhookError> {
        std::fs::create_dir_all(&cfg.dir).map_err(|e| WebhookError::Dir(cfg.dir.clone(), e))?;
        let http = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .build()?;
        let (tx, registry) = mpsc::channel(64);
        let mut me = Self {
            live: hub.subscribe(),
            cfg,
            registry,
            http,
            known: HashSet::new(),
            workers: HashMap::new(),
            shutdown,
        };
        for sub in me.load_subscriptions()? {
            me.adopt(&sub)?;
        }
        me.report_counts();
        Ok((me, Registrar { tx }))
    }

    fn load_subscriptions(&self) -> Result<Vec<Subscription>, WebhookError> {
        let entries = std::fs::read_dir(&self.cfg.dir)
            .map_err(|e| WebhookError::Dir(self.cfg.dir.clone(), e))?;
        entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
            .map(|p| Subscription::read(&p))
            .collect()
    }

    /// Take `sub` in: remember it, and run it if this instance owns it.
    fn adopt(&mut self, sub: &Subscription) -> Result<(), WebhookError> {
        self.known.insert(sub.id);
        if !self.cfg.instances.owns(sub.id) {
            return Ok(());
        }
        let worker = self.spawn_worker(sub)?;
        self.workers.insert(sub.id, worker);
        Ok(())
    }

    fn spawn_worker(&self, sub: &Subscription) -> Result<Worker, WebhookError> {
        WorkerSpec {
            sub,
            dir: &self.cfg.dir,
            http: self.http.clone(),
            retain_bytes: self.cfg.retain_bytes,
            queue: self.cfg.queue,
            shutdown: self.shutdown.clone(),
        }
        .start()
    }

    fn report_counts(&self) {
        let owned = self.workers.len();
        let other = self.known.len().saturating_sub(owned);
        metrics::gauge!(m::WEBHOOK_SUBSCRIPTIONS, "owned" => "true").set(m::count(owned));
        metrics::gauge!(m::WEBHOOK_SUBSCRIPTIONS, "owned" => "false").set(m::count(other));
    }

    /// Fan out and register until the shutdown token fires or the hub
    /// ends.
    pub async fn run(mut self) {
        while self.step().await.is_continue() {}
    }

    async fn step(&mut self) -> ControlFlow<()> {
        let next = tokio::select! {
            () = self.shutdown.cancelled() => Step::Stop,
            r = self.live.recv() => Step::Live(r),
            reg = self.registry.recv() => reg.map_or(Step::Stop, Step::Register),
        };
        match next {
            Step::Live(Ok(stamped)) => self.on_live(&stamped),
            Step::Live(Err(broadcast::error::RecvError::Lagged(n))) => {
                metrics::counter!(m::WEBHOOK_FEED_LAGGED_TOTAL).increment(n);
            }
            Step::Live(Err(broadcast::error::RecvError::Closed)) | Step::Stop => {
                return ControlFlow::Break(());
            }
            Step::Register(reg) => self.on_register(reg),
        }
        ControlFlow::Continue(())
    }

    /// Queue one live event for every owned subscription it matches. A
    /// full queue drops the event and counts it: the fan-out never
    /// waits for one subscription.
    fn on_live(&self, stamped: &Stamped) {
        self.workers
            .iter()
            .filter(|(_, w)| w.filter.matches(&stamped.event))
            .filter(|(_, w)| w.events.try_send(stamped.clone()).is_err())
            .for_each(|(id, _)| {
                metrics::counter!(m::WEBHOOK_QUEUE_FULL_TOTAL, "subscription" => format!("{id:#x}"))
                    .increment(1);
            });
    }

    fn on_register(&mut self, reg: Register) {
        let outcome = self.register(reg.request);
        // A caller that went away while its registration ran needs no
        // answer.
        let _ = reg.reply.send(outcome);
    }

    fn register(&mut self, request: WebhookRequest) -> Result<WebhookAck, RegisterError> {
        let sub = Subscription::parse(request)?;
        let owner = self.cfg.instances.owner(sub.id);
        if self.known.contains(&sub.id) {
            return Ok(WebhookAck { id: sub.id, owner });
        }
        sub.write(&self.cfg.dir.join(format!("{:#x}.json", sub.id)))
            .map_err(|e| RegisterError::Store(e.to_string()))?;
        self.adopt(&sub)
            .map_err(|e| RegisterError::Store(e.to_string()))?;
        self.report_counts();
        tracing::info!(id = %sub.id, owner, url = %sub.url, "webhook subscription registered");
        Ok(WebhookAck { id: sub.id, owner })
    }
}

impl WebhookError {
    fn file(path: &Path, e: impl std::fmt::Display) -> Self {
        Self::File(path.to_path_buf(), e.to_string())
    }
}

impl Subscription {
    /// Load a stored subscription.
    fn read(path: &Path) -> Result<Self, WebhookError> {
        let raw = std::fs::read_to_string(path).map_err(|e| WebhookError::file(path, e))?;
        serde_json::from_str(&raw).map_err(|e| WebhookError::file(path, e))
    }

    /// Store this subscription atomically: a sibling temp file, then a
    /// rename.
    fn write(&self, path: &Path) -> Result<(), WebhookError> {
        let raw = serde_json::to_vec_pretty(self).map_err(|e| WebhookError::file(path, e))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, raw).map_err(|e| WebhookError::file(path, e))?;
        std::fs::rename(&tmp, path).map_err(|e| WebhookError::file(path, e))
    }
}

#[cfg(test)]
#[path = "webhooks_tests.rs"]
mod tests;
