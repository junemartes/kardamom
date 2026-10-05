//! The exporter's HTTP listener: `/metrics` renders the registry and
//! `/ready` answers the service's readiness rule. One listener serves
//! both, so a Consul check and a Prometheus scrape share a port.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;

use crate::ready::Readiness;

/// The registry's histogram buckets drain on this period, the same
/// cadence as the exporter crate's own listener.
const UPKEEP: Duration = Duration::from_secs(5);

/// The pause after a failed accept, so a descriptor shortage does not
/// spin the listener task.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// What every connection task reads: the registry handle and the rule.
struct Routes {
    handle: PrometheusHandle,
    readiness: Readiness,
}

impl Routes {
    /// Answer one request. An unknown path is 404.
    fn respond(&self, req: &Request<Incoming>) -> Response<Full<Bytes>> {
        match req.uri().path() {
            "/metrics" => text(StatusCode::OK, self.handle.render()),
            "/ready" => self.ready(),
            _ => text(StatusCode::NOT_FOUND, "not found\n".to_string()),
        }
    }

    /// 200 `ready` when the rule holds, 503 with the failed conditions
    /// when it does not.
    fn ready(&self) -> Response<Full<Bytes>> {
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
    ) -> Self {
        Self {
            listener,
            routes: Arc::new(Routes { handle, readiness }),
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
            Ok((stream, _)) => {
                let routes = self.routes.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let routes = routes.clone();
                        async move { Ok::<_, Infallible>(routes.respond(&req)) }
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
