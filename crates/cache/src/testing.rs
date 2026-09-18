//! A Redis container for the Docker-gated tests of this crate and of its
//! readers. Behind the `docker-e2e` feature.

use std::num::NonZeroU64;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use crate::config::CacheConfig;

/// A running Redis container, and the `[cache]` config that reaches it
/// over a direct URL, with a short receipt TTL for the expiry tests.
///
/// # Panics
///
/// Panics when the container does not start: no Docker daemon, or no
/// image.
pub async fn redis() -> (ContainerAsync<GenericImage>, CacheConfig) {
    let container = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379_u16.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .expect("redis container");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    let cfg = CacheConfig {
        url: Some(format!("redis://127.0.0.1:{port}")),
        receipt_ttl_secs: NonZeroU64::new(2).unwrap(),
        ..CacheConfig::default()
    };
    (container, cfg)
}
