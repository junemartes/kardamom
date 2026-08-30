use std::net::SocketAddr;

#[tokio::test]
async fn init_rejects_empty_host_id() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let err = kardamom_obs::init("svc", addr, "", "0.0.0", "sha")
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("host_id"));
}
