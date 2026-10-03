//! The exporter's HTTP listener: `/metrics` renders the registry,
//! `/ready` answers the service's readiness rule, and `/halt` serves the
//! standing halt as JSON. One listener serves them all, so a Consul
//! check, a Prometheus scrape, and an operator share a port.
//!
//! `POST /halt/clear` ends an operator halt. The repository guards no
//! route with a credential: an admin action is guarded by where it comes
//! from. The route accepts a loopback peer only, so the operator runs
//! the clear from the service's own node, as the runbooks say.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::halt::{self, Halt};
use crate::ready::Readiness;

/// The registry's histogram buckets drain on this period, the same
/// cadence as the exporter crate's own listener.
const UPKEEP: Duration = Duration::from_secs(5);

/// The pause after a failed accept, so a descriptor shortage does not
/// spin the listener task.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// What every connection task reads: the registry handle, the rule, the
/// service name, and the standing halt.
struct Routes {
    handle: PrometheusHandle,
    readiness: Readiness,
    service: &'static str,
    halts: watch::Receiver<Option<Halt>>,
}

impl Routes {
    /// Answer one request. `local` says whether the peer is a loopback
    /// address. An unknown path is 404.
    fn respond(&self, req: &Request<Incoming>, local: bool) -> Response<Full<Bytes>> {
        match req.uri().path() {
            "/metrics" => text(StatusCode::OK, self.handle.render()),
            "/ready" => self.ready(),
            "/halt" => self.halt(),
            "/halt/clear" if req.method() == Method::POST => Self::clear_halt(local),
            _ => text(StatusCode::NOT_FOUND, "not found\n".to_string()),
        }
    }

    /// 503 with the halt while one stands, 200 `ready` when the rule
    /// holds, 503 with the failed conditions when it does not.
    fn ready(&self) -> Response<Full<Bytes>> {
        if let Some(halt) = self.halts.borrow().as_ref() {
            return text(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("not ready:\n{}\n", halt.summary()),
            );
        }
        match self
            .readiness
            .check(&self.handle.render(), SystemTime::now())
        {
            Ok(()) => text(StatusCode::OK, "ready\n".to_string()),
            Err(failed) => text(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("not ready:\n{}\n", failed.join("\n")),
            ),
        }
    }

    /// The standing halt as JSON, or `{"halted": false}`.
    fn halt(&self) -> Response<Full<Bytes>> {
        let body = halt::to_json(self.service, self.halts.borrow().as_ref());
        Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body.to_string())))
            .expect("a status and one header form a valid response")
    }

    /// The operator's clear. A peer off the loopback is refused.
    fn clear_halt(local: bool) -> Response<Full<Bytes>> {
        if !local {
            return text(
                StatusCode::FORBIDDEN,
                "a halt clears from the service's own node only\n".to_string(),
            );
        }
        match halt::clear() {
            Some(halt) => text(StatusCode::OK, format!("cleared {}\n", halt.cause.id())),
            None => text(StatusCode::OK, "no halt stands\n".to_string()),
        }
    }
}

/// A plain-text response.
fn text(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain")
        .body(Full::new(Bytes::from(body)))
        .expect("a status and one header form a valid response")
}

/// The listener task: accepts connections for the process lifetime.
pub(crate) struct Server {
    listener: TcpListener,
    routes: Arc<Routes>,
}

impl Server {
    pub(crate) fn new(
        listener: TcpListener,
        handle: PrometheusHandle,
        readiness: Readiness,
        service: &'static str,
    ) -> Self {
        Self {
            listener,
            routes: Arc::new(Routes {
                handle,
                readiness,
                service,
                halts: halt::subscribe(),
            }),
        }
    }

    /// Spawn the listener and the registry upkeep onto the ambient
    /// runtime.
    pub(crate) fn spawn(self) {
        let handle = self.routes.handle.clone();
        tokio::spawn(async move {
            loop {
                upkeep_tick(&handle).await;
            }
        });
        tokio::spawn(async move {
            loop {
                self.accept_one().await;
            }
        });
    }

    /// Accept one connection and serve it on its own task. Every
    /// connection is independent, so a scraper that keeps its connection
    /// open never blocks a check.
    async fn accept_one(&self) {
        match self.listener.accept().await {
            Ok((stream, peer)) => {
                let routes = self.routes.clone();
                let local = peer.ip().is_loopback();
                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let routes = routes.clone();
                        async move { Ok::<_, Infallible>(routes.respond(&req, local)) }
                    });
                    if let Err(e) = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                    {
                        tracing::debug!(error = %e, "obs-exporter: connection ended with an error");
                    }
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "obs-exporter: accept failed");
                tokio::time::sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

/// One period of registry upkeep.
async fn upkeep_tick(handle: &PrometheusHandle) {
    tokio::time::sleep(UPKEEP).await;
    handle.run_upkeep();
}
