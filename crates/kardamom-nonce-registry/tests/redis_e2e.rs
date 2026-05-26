//! End-to-end test against a real Redis spun up via testcontainers.

use alloy_primitives::Address;
use kardamom_nonce_registry::{CheckOutcome, NonceRegistry, RegistryConfig};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn start_redis() -> Option<(impl Sized, String)> {
    if !docker_available() {
        eprintln!("skipping redis_e2e: docker daemon not available");
        return None;
    }
    let container = GenericImage::new("redis", "7-alpine")
        .with_exposed_port(6379.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
        .start()
        .await
        .expect("start redis container");
    let port = container.get_host_port_ipv4(6379).await.expect("port");
    let url = format!("redis://127.0.0.1:{port}");
    Some((container, url))
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_miss_then_seed_then_accept_then_reject() {
    let Some((_c, url)) = start_redis().await else {
        return;
    };
    let reg = NonceRegistry::connect(RegistryConfig::new(url))
        .await
        .expect("connect");
    let sender = Address::repeat_byte(0x11);

    // First check: cache miss.
    assert_eq!(
        reg.check_and_increment(sender, 0).await.unwrap(),
        CheckOutcome::CacheMiss
    );

    // Seed from canonical (pretend next_nonce is 5).
    reg.seed(sender, 5).await.unwrap();
    assert_eq!(reg.get(sender).await.unwrap(), Some(5));

    // Wrong nonce: rejected.
    assert_eq!(
        reg.check_and_increment(sender, 4).await.unwrap(),
        CheckOutcome::Rejected { expected: 5 }
    );
    // Underlying value unchanged.
    assert_eq!(reg.get(sender).await.unwrap(), Some(5));

    // Right nonce: accepted, value advances.
    assert_eq!(
        reg.check_and_increment(sender, 5).await.unwrap(),
        CheckOutcome::Accepted { new_next_nonce: 6 }
    );
    assert_eq!(reg.get(sender).await.unwrap(), Some(6));

    // Next correct nonce: still accepted.
    assert_eq!(
        reg.check_and_increment(sender, 6).await.unwrap(),
        CheckOutcome::Accepted { new_next_nonce: 7 }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_check_and_increment_serializes() {
    let Some((_c, url)) = start_redis().await else {
        return;
    };
    let reg = NonceRegistry::connect(RegistryConfig::new(url))
        .await
        .expect("connect");
    let sender = Address::repeat_byte(0x22);
    reg.seed(sender, 0).await.unwrap();

    // 50 racing tasks all try to claim nonce 0. Exactly one must win.
    let mut tasks = Vec::with_capacity(50);
    for _ in 0..50 {
        let r = reg.clone();
        tasks.push(tokio::spawn(async move {
            r.check_and_increment(sender, 0).await
        }));
    }
    let mut accepted = 0;
    let mut rejected = 0;
    for t in tasks {
        match t.await.unwrap().unwrap() {
            CheckOutcome::Accepted { .. } => accepted += 1,
            CheckOutcome::Rejected { .. } => rejected += 1,
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
    assert_eq!(accepted, 1, "exactly one task must win the race");
    assert_eq!(rejected, 49);
    assert_eq!(reg.get(sender).await.unwrap(), Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn key_prefix_isolates_namespaces() {
    let Some((_c, url)) = start_redis().await else {
        return;
    };
    let reg_a = NonceRegistry::connect(RegistryConfig {
        url: url.clone(),
        key_prefix: "a:".into(),
    })
    .await
    .unwrap();
    let reg_b = NonceRegistry::connect(RegistryConfig {
        url,
        key_prefix: "b:".into(),
    })
    .await
    .unwrap();

    let sender = Address::repeat_byte(0x33);
    reg_a.seed(sender, 100).await.unwrap();
    // reg_b sees an independent namespace — cache miss for same sender.
    assert_eq!(reg_b.get(sender).await.unwrap(), None);
    assert_eq!(reg_a.get(sender).await.unwrap(), Some(100));
}
