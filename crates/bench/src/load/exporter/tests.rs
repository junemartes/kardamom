use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

/// A reader whose read through the node runs `program` in place of
/// `docker`, with a 1 s bound.
fn reader(program: &'static str) -> ExporterReader {
    ExporterReader::with_exec(program, Duration::from_secs(1)).unwrap()
}

/// Serve `body` as one HTTP response on a loopback port; the port.
async fn serve_once(body: &'static str) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 1024];
        let _ = sock.read(&mut request).await;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(response.as_bytes()).await.unwrap();
    });
    port
}

/// A loopback port that no listener holds.
fn closed_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn a_target_reads_inside_its_node_on_loopback_at_the_same_port() {
    let target = MetricsTarget::bridged(Ipv4Addr::new(10, 0, 0, 5), "executor-0", 9004);
    let url = target.url.as_ref().map(reqwest::Url::as_str);
    assert_eq!(url, Some("http://10.0.0.5:9004/metrics"));
    assert_eq!(target.loopback_url(), "http://127.0.0.1:9004/metrics");
    assert_eq!(target.port(), 9004);
    assert_eq!(MetricsTarget::loopback("executor-0", 9004).url, None);
    assert!(MetricsTarget::named("not a host", 9006).is_err());
}

#[test]
fn named_targets_are_direct_unless_via_docker() {
    let executors = ["kardamom-executor-0".to_string()];
    let sequencers = ["kardamom-sequencer-0".to_string()];
    let direct =
        MetricsTargets::named(&executors, "kardamom-ingress-0", &sequencers, false).unwrap();
    let url = direct.ingress.url.as_ref().map(reqwest::Url::as_str);
    assert_eq!(url, Some("http://kardamom-ingress-0:9006/metrics"));
    assert_eq!(direct.executors[0].port(), 9004);
    assert_eq!(direct.sequencers[0].port(), 9001);
    let via_node =
        MetricsTargets::named(&executors, "kardamom-ingress-0", &sequencers, true).unwrap();
    assert_eq!(
        via_node.ingress,
        MetricsTarget::loopback("kardamom-ingress-0", 9006)
    );
}

#[tokio::test]
async fn a_direct_read_needs_no_fallback() {
    let port = serve_once("kardamom_service_up 1\n").await;
    let target = MetricsTarget::bridged(Ipv4Addr::LOCALHOST, "ingress-0", port);
    let read = reader("false").read(&target).await;
    assert_eq!(
        read,
        ExporterRead {
            body: Some("kardamom_service_up 1\n".into()),
            fell_back: false,
        }
    );
}

#[tokio::test]
async fn a_failed_direct_read_falls_back_through_the_node() {
    let port = closed_port();
    let target = MetricsTarget::bridged(Ipv4Addr::LOCALHOST, "ingress-0", port);
    // `echo` prints the command line of the read through the node.
    let read = reader("echo").read(&target).await;
    assert!(read.fell_back);
    assert_eq!(
        read.body.unwrap().trim(),
        format!("exec ingress-0 curl -fsS --max-time 5 http://127.0.0.1:{port}/metrics")
    );
    let failed = reader("false").read(&target).await;
    assert_eq!(
        failed,
        ExporterRead {
            body: None,
            fell_back: true,
        }
    );
}

#[tokio::test]
async fn a_loopback_target_reads_through_the_node_with_no_fallback() {
    let target = MetricsTarget::loopback("ingress-0", 9006);
    let read = reader("echo").read(&target).await;
    assert!(!read.fell_back);
    assert!(read.body.unwrap().starts_with("exec ingress-0 curl"));
}

#[tokio::test]
async fn a_stalled_read_through_the_node_ends_at_the_bound() {
    // `sleep exec ...` fails at once, so a shell script stands in for a
    // stalled `docker exec`.
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("stalled-docker");
    std::fs::write(&script, "#!/bin/sh\nsleep 30\n").unwrap();
    let mut perms = std::fs::metadata(&script).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
    std::fs::set_permissions(&script, perms).unwrap();
    let program: &'static str = Box::leak(script.to_string_lossy().into_owned().into_boxed_str());
    let target = MetricsTarget::loopback("ingress-0", 9006);
    let start = Instant::now();
    let read = reader(program).read(&target).await;
    assert_eq!(read.body, None);
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "{:?}",
        start.elapsed()
    );
}
