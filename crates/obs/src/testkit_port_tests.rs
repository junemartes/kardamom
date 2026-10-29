use std::net::SocketAddr;

use super::{TestPorts, free_port, free_udp_port};

#[test]
fn ports_come_from_below_the_ephemeral_range_and_do_not_repeat() {
    let ports: Vec<u16> = (0..64).map(|_| free_port().port()).collect();
    assert!(ports.iter().all(|p| (20_000..32_000).contains(p)));
    let mut unique = ports.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), ports.len());
    assert!((20_000..32_000).contains(&free_udp_port().port()));
}

#[test]
fn a_port_that_does_not_bind_is_skipped() {
    let tried = std::cell::RefCell::new(Vec::new());
    let got = TestPorts::shared().claim(|port| {
        tried.borrow_mut().push(port);
        if tried.borrow().len() < 3 {
            return Err(std::io::Error::from(std::io::ErrorKind::AddrInUse));
        }
        Ok(SocketAddr::from(([127, 0, 0, 1], port)))
    });
    let tried = tried.into_inner();
    assert_eq!(tried.len(), 3);
    assert_eq!(got.port(), tried[2]);
    assert!(tried[..2].iter().all(|p| *p != got.port()));
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "The test callback has the socket binder's fallible signature."
)]
fn available(port: u16) -> std::io::Result<SocketAddr> {
    Ok(SocketAddr::from(([127, 0, 0, 1], port)))
}

#[test]
fn overlapping_cursors_cannot_claim_the_same_unbound_ports() {
    let directory = tempfile::tempdir().unwrap();
    let left = TestPorts::new(0, directory.path().to_path_buf());
    let right = TestPorts::new(1, directory.path().to_path_buf());
    let pairs: Vec<_> = (0..600)
        .map(|_| (left.claim(available).port(), right.claim(available).port()))
        .collect();
    let left: std::collections::HashSet<_> = pairs.iter().map(|(l, _)| *l).collect();
    let right: std::collections::HashSet<_> = pairs.iter().map(|(_, r)| *r).collect();
    assert_eq!(left.len(), 600);
    assert_eq!(right.len(), 600);
    assert!(left.is_disjoint(&right));
}

#[test]
fn released_range_is_available_to_a_new_allocator() {
    let directory = tempfile::tempdir().unwrap();
    let first = {
        let owner = TestPorts::new(0, directory.path().to_path_buf());
        owner.claim(available)
    };
    let replacement = TestPorts::new(0, directory.path().to_path_buf());
    assert_eq!(replacement.claim(available), first);
}

struct LeaseChild(std::process::Child);

impl LeaseChild {
    fn start(directory: &std::path::Path, result: &std::path::Path) -> Self {
        Self(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "testkit::port_tests::port_lease_child",
                    "--nocapture",
                ])
                .env("KARDAMOM_PORT_TEST_DIRECTORY", directory)
                .env("KARDAMOM_PORT_TEST_RESULT", result)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        )
    }

    fn claimed(&mut self, result: &std::path::Path) -> u16 {
        super::poll_sync(
            "the child port lease",
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(10),
            || {
                anyhow::ensure!(self.0.try_wait()?.is_none(), "the lease child exited early");
                Ok(std::fs::read_to_string(result)
                    .ok()
                    .and_then(|port| port.parse().ok()))
            },
        )
        .unwrap()
    }
}

impl Drop for LeaseChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn port_lease_child() {
    let Some(directory) = std::env::var_os("KARDAMOM_PORT_TEST_DIRECTORY") else {
        return;
    };
    let owner = TestPorts::new(0, directory.into());
    let port = owner.claim(available).port();
    std::fs::write(
        std::env::var_os("KARDAMOM_PORT_TEST_RESULT").unwrap(),
        port.to_string(),
    )
    .unwrap();
    let mut byte = [0];
    std::io::Read::read(&mut std::io::stdin(), &mut byte).unwrap();
}

#[test]
fn process_leases_exclude_unbound_ports_and_release_after_a_crash() {
    let directory = tempfile::tempdir().unwrap();
    let left_path = directory.path().join("left-port");
    let right_path = directory.path().join("right-port");
    let first = {
        let mut left = LeaseChild::start(directory.path(), &left_path);
        let mut right = LeaseChild::start(directory.path(), &right_path);
        let first = left.claimed(&left_path);
        assert_ne!(first, right.claimed(&right_path));
        first
    };
    let replacement = TestPorts::new(
        u32::from(first) - TestPorts::FIRST,
        directory.path().to_path_buf(),
    );
    assert_eq!(replacement.claim(available).port(), first);
}
