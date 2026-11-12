use super::*;

#[tokio::test]
async fn a_transport_error_names_its_cause() {
    // The listener closes at the end of the block, so the port refuses.
    let addr = {
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        free.local_addr().unwrap()
    };
    let proxy = DaProxy::new(format!("http://{addr}")).unwrap();
    let error = proxy.put(b"payload").await.unwrap_err().to_string();
    assert!(error.starts_with("data availability: put: "), "{error}");
    assert!(error.contains("Connection refused"), "{error}");
}
