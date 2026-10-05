//! One listener for both client surfaces: the JSON-RPC WebSocket feed,
//! and `POST /webhooks`. Every connection runs the jsonrpsee tower
//! service, with the webhook route answered in front of it.

use std::net::SocketAddr;
use std::ops::ControlFlow;

use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use jsonrpsee::server::{
    HttpBody, HttpRequest, HttpResponse, Methods, Server, ServerConfig, ServerHandle, StopHandle,
    TowerService, TowerServiceBuilder, serve_with_graceful_shutdown, stop_channel,
};
use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::Service;
use tower::layer::util::Identity;

use crate::dto::WebhookRequest;
use crate::feed::{Feed, TxStatusFeedApiServer};
use crate::webhooks::{RegisterError, Registrar};

/// The header a registration forwarded by a twin instance carries, so
/// the receiver stores it and forwards it no further.
pub const REPLICA_HEADER: &str = "x-kardamom-replica";
/// The largest registration body accepted.
const MAX_BODY: usize = 64 * 1024;
/// A forward to a twin is retried this many times, one second apart.
const FORWARD_ATTEMPTS: u32 = 3;

type RpcService = TowerService<Identity, Identity>;
type RpcBuilder = TowerServiceBuilder<Identity, Identity>;

#[derive(Clone, Copy, Debug)]
pub struct ListenConfig {
    pub bind: SocketAddr,
    pub max_connections: u32,
}

/// The webhook route: the local registry, and the twin instances every
/// registration is forwarded to.
#[derive(Clone)]
pub struct Hooks {
    registrar: Registrar,
    peers: Vec<String>,
    http: reqwest::Client,
}

impl Hooks {
    /// `peers` are the base URLs of the other instances; a registration
    /// is stored here and forwarded to each of them.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub fn new(registrar: Registrar, peers: Vec<String>) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()?;
        Ok(Self {
            registrar,
            peers,
            http,
        })
    }

    async fn handle(&self, req: HttpRequest<hyper::body::Incoming>) -> HttpResponse<HttpBody> {
        let replica = req.headers().contains_key(REPLICA_HEADER);
        let body = match read_body(req.into_body()).await {
            Ok(b) => b,
            Err(e) => return json_response(StatusCode::BAD_REQUEST, &Problem::new(e)),
        };
        let request: WebhookRequest = match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => return json_response(StatusCode::BAD_REQUEST, &Problem::new(e.to_string())),
        };
        match self.registrar.register(request.clone()).await {
            Ok(ack) => {
                if !replica {
                    self.forward(&request);
                }
                json_response(StatusCode::CREATED, &ack)
            }
            Err(e @ RegisterError::Closed) => json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &Problem::new(e.to_string()),
            ),
            Err(e) => json_response(StatusCode::BAD_REQUEST, &Problem::new(e.to_string())),
        }
    }

    /// Store the registration on every twin too, so the twin can take a
    /// shard over. Best effort, off this request.
    fn forward(&self, request: &WebhookRequest) {
        for peer in &self.peers {
            tokio::spawn(Self::forward_to(
                self.http.clone(),
                format!("{peer}/webhooks"),
                request.clone(),
            ));
        }
    }

    /// POST `request` to `url` as a replica, a few times.
    async fn forward_to(http: reqwest::Client, url: String, request: WebhookRequest) {
        for _ in 0..FORWARD_ATTEMPTS {
            if Self::forward_once(&http, &url, &request).await {
                return;
            }
        }
        tracing::warn!(url, "webhook registration not forwarded to the twin");
    }

    async fn forward_once(http: &reqwest::Client, url: &str, request: &WebhookRequest) -> bool {
        let sent = http
            .post(url)
            .header(REPLICA_HEADER, "1")
            .json(request)
            .send()
            .await;
        match sent {
            Ok(r) if r.status().is_success() => true,
            _ => {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                false
            }
        }
    }
}

/// An error body: `{"error": "..."}`.
#[derive(Serialize)]
struct Problem {
    error: String,
}

impl Problem {
    fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

async fn read_body(body: hyper::body::Incoming) -> Result<Vec<u8>, String> {
    http_body_util::Limited::new(body, MAX_BODY)
        .collect()
        .await
        .map(|c| c.to_bytes().to_vec())
        .map_err(|e| format!("read body: {e}"))
}

fn json_response<T: Serialize>(status: StatusCode, body: &T) -> HttpResponse<HttpBody> {
    let json = serde_json::to_string(body).unwrap_or_default();
    HttpResponse::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(HttpBody::from(json))
        .expect("a status and one static header form a valid response")
}

/// The running listener.
pub struct Running {
    pub addr: SocketAddr,
    handle: ServerHandle,
    accept: JoinHandle<()>,
}

impl Running {
    /// Stop accepting, end every connection, and wait for the accept
    /// loop.
    pub async fn stop(self) {
        let _ = self.handle.stop();
        let _ = self.accept.await;
    }
}

/// Bind `cfg.bind` and serve the feed and the webhook route until
/// `shutdown` fires.
///
/// # Errors
///
/// Returns an error if the bind fails.
pub async fn serve(
    cfg: ListenConfig,
    feed: Feed,
    hooks: Hooks,
    shutdown: CancellationToken,
) -> std::io::Result<Running> {
    let listener = TcpListener::bind(cfg.bind).await?;
    let addr = listener.local_addr()?;
    let (stop_handle, handle) = stop_channel();
    let server_cfg = ServerConfig::builder()
        .max_connections(cfg.max_connections)
        .build();
    let acceptor = Acceptor {
        listener,
        builder: Server::builder()
            .set_config(server_cfg)
            .to_service_builder(),
        methods: feed.into_rpc().into(),
        hooks,
        stop_handle,
        shutdown,
    };
    Ok(Running {
        addr,
        handle,
        accept: tokio::spawn(acceptor.run()),
    })
}

struct Acceptor {
    listener: TcpListener,
    builder: RpcBuilder,
    methods: Methods,
    hooks: Hooks,
    stop_handle: StopHandle,
    shutdown: CancellationToken,
}

impl Acceptor {
    async fn run(mut self) {
        while self.accept_one().await.is_continue() {}
    }

    async fn accept_one(&mut self) -> ControlFlow<()> {
        let accepted = tokio::select! {
            () = self.shutdown.cancelled() => return ControlFlow::Break(()),
            r = self.listener.accept() => r,
        };
        match accepted {
            Ok((sock, _)) => self.spawn_connection(sock),
            Err(e) => tracing::warn!(error = %e, "accept failed"),
        }
        ControlFlow::Continue(())
    }

    fn spawn_connection(&self, sock: TcpStream) {
        let rpc = self
            .builder
            .clone()
            .build(self.methods.clone(), self.stop_handle.clone());
        let hooks = self.hooks.clone();
        let svc = tower::service_fn(move |req: HttpRequest<hyper::body::Incoming>| {
            route(req, rpc.clone(), hooks.clone())
        });
        let stopped = self.stop_handle.clone().shutdown();
        tokio::spawn(async move {
            if let Err(e) = serve_with_graceful_shutdown(sock, svc, stopped).await {
                tracing::debug!(error = %e, "connection ended with an error");
            }
        });
    }
}

/// `POST /webhooks` is answered here; everything else is JSON-RPC.
async fn route(
    req: HttpRequest<hyper::body::Incoming>,
    mut rpc: RpcService,
    hooks: Hooks,
) -> Result<HttpResponse<HttpBody>, tower::BoxError> {
    if req.method() == Method::POST && req.uri().path() == "/webhooks" {
        return Ok(hooks.handle(req).await);
    }
    rpc.call(req).await
}
