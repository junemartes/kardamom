//! The listener of the JSON-RPC WebSocket feed. Every connection runs
//! the jsonrpsee tower service.

use std::net::SocketAddr;
use std::ops::ControlFlow;

use jsonrpsee::server::{
    HttpBody, HttpRequest, HttpResponse, Methods, Server, ServerConfig, ServerHandle, StopHandle,
    TowerService, TowerServiceBuilder, serve_with_graceful_shutdown, stop_channel,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::Service;
use tower::layer::util::Identity;

use crate::feed::{Feed, TxStatusFeedApiServer};

type RpcService = TowerService<Identity, Identity>;
type RpcBuilder = TowerServiceBuilder<Identity, Identity>;

#[derive(Clone, Copy, Debug)]
pub struct ListenConfig {
    pub bind: SocketAddr,
    pub max_connections: u32,
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

/// Bind `cfg.bind` and serve the feed until `shutdown` fires.
///
/// # Errors
///
/// Returns an error if the bind fails.
pub async fn serve(
    cfg: ListenConfig,
    feed: Feed,
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
        let svc = tower::service_fn(move |req: HttpRequest<hyper::body::Incoming>| {
            route(req, rpc.clone())
        });
        let stopped = self.stop_handle.clone().shutdown();
        tokio::spawn(async move {
            if let Err(e) = serve_with_graceful_shutdown(sock, svc, stopped).await {
                tracing::debug!(error = %e, "connection ended with an error");
            }
        });
    }
}

/// Every request is JSON-RPC.
async fn route(
    req: HttpRequest<hyper::body::Incoming>,
    mut rpc: RpcService,
) -> Result<HttpResponse<HttpBody>, tower::BoxError> {
    rpc.call(req).await
}
