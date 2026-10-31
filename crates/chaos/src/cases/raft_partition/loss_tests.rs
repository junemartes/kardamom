use std::net::Ipv4Addr;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;

use serde_json::{Value, json};

use super::super::rules::{LOSS_CHAIN, Rules};
use super::super::{CHAIN, PORTS};
use super::Counts;

struct Table {
    dir: tempfile::TempDir,
    store: PathBuf,
}

impl Table {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("iptables");
        std::fs::write(
            &executable,
            include_str!("../lookup_blackout/rules/test_iptables.py"),
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let store = dir.path().join("table.json");
        std::fs::write(
            &store,
            json!({"OUTPUT": [["-p", "tcp", "-j", "ACCEPT"]]}).to_string(),
        )
        .unwrap();
        Self { dir, store }
    }

    fn execute(&self, script: &str, failure: &str) -> bool {
        let output = Command::new("sh")
            .args(["-c", script])
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.dir.path().display()),
            )
            .env("LOOKUP_TABLE", &self.store)
            .env("LOOKUP_FAIL", failure)
            .output()
            .unwrap();
        output.status.success()
    }

    fn read(&self) -> Value {
        serde_json::from_slice(&std::fs::read(&self.store).unwrap()).unwrap()
    }

    fn target(&self, chain: &str, peer: Ipv4Addr, port: u16, protocol: &str, draw: f64) -> String {
        self.read()[chain]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|rule| Self::matched(rule, peer, port, protocol, draw))
            .unwrap_or_else(|| "RETURN".into())
    }

    fn matched(
        rule: &Value,
        peer: Ipv4Addr,
        port: u16,
        protocol: &str,
        draw: f64,
    ) -> Option<String> {
        let args: Vec<&str> = rule
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        let value = |flag| args.windows(2).find(|p| p[0] == flag).map(|p| p[1]);
        if value("-p") != Some(protocol)
            || value("-d") != Some(peer.to_string().as_str())
            || value("--dport") != Some(port.to_string().as_str())
        {
            return None;
        }
        if let Some(probability) = value("--probability") {
            assert_eq!(value("--mode"), Some("random"));
            if draw >= probability.parse::<f64>().unwrap() {
                return None;
            }
        }
        value("-j").map(str::to_owned)
    }

    fn assert_scope(&self, peer: Ipv4Addr, port: u16) {
        assert_eq!(self.target(LOSS_CHAIN, peer, port, "udp", 0.01), "DROP");
        assert_eq!(self.target(LOSS_CHAIN, peer, port, "udp", 0.90), "RETURN");
        assert_eq!(self.target(LOSS_CHAIN, peer, port, "tcp", 0.01), "RETURN");
    }
}

const PEERS: [Ipv4Addr; 2] = [Ipv4Addr::new(10, 0, 0, 2), Ipv4Addr::new(10, 0, 0, 3)];

#[test]
fn partial_loss_only_matches_peer_raft_udp_and_preserves_passing_packets() {
    let table = Table::new();
    assert!(table.execute(&Rules::loss(&PEERS).arm(), ""));
    assert_eq!(table.read()[LOSS_CHAIN].as_array().unwrap().len(), 16);
    PEERS
        .into_iter()
        .flat_map(|peer| PORTS.into_iter().map(move |port| (peer, port)))
        .for_each(|(peer, port)| table.assert_scope(peer, port));
    for port in [53, 6379, 9024, 40200, 40205] {
        assert_eq!(
            table.target(LOSS_CHAIN, PEERS[0], port, "udp", 0.01),
            "RETURN"
        );
    }
    assert_eq!(
        table.target(
            LOSS_CHAIN,
            Ipv4Addr::new(10, 0, 0, 9),
            PORTS[0],
            "udp",
            0.01
        ),
        "RETURN"
    );
}

#[test]
fn cleanup_restores_the_table_and_leaves_an_independent_fault_chain() {
    let table = Table::new();
    let original = table.read();
    assert!(table.execute(&Rules::partition(&PEERS).arm(), ""));
    let partition = table.read();
    assert!(table.execute(&Rules::loss(&PEERS).arm(), ""));
    assert!(table.execute(&Rules::cleanup(LOSS_CHAIN), ""));
    assert_eq!(table.read(), partition);
    assert_eq!(table.target(CHAIN, PEERS[0], PORTS[0], "udp", 0.90), "DROP");
    assert!(table.execute(&Rules::cleanup(CHAIN), ""));
    assert_eq!(table.read(), original);
    assert!(table.execute(&Rules::cleanup(LOSS_CHAIN), ""));
}

#[test]
fn cleanup_removes_a_partially_armed_loss_chain() {
    let table = Table::new();
    let original = table.read();
    let failure = format!(
        "-A {LOSS_CHAIN} -p udp -d {} --dport {}",
        PEERS[1], PORTS[1]
    );
    assert!(!table.execute(&Rules::loss(&PEERS).arm(), &failure));
    assert!(table.read()[LOSS_CHAIN].as_array().unwrap().len() > 1);
    assert!(table.execute(&Rules::cleanup(LOSS_CHAIN), ""));
    assert_eq!(table.read(), original);
}

#[test]
fn fault_proof_requires_both_outcomes_on_each_peer() {
    let drop = "10 600 DROP udp -- * * 0.0.0.0/0 10.0.0.2 udp dpt:40202 statistic mode random probability 0.10000000009\n";
    let pass = "90 5400 RETURN udp -- * * 0.0.0.0/0 10.0.0.2 udp dpt:40202\n";
    let both = format!("{drop}{pass}");
    let counts = Counts::read(&both, PEERS[0]).unwrap();
    assert_eq!((counts.dropped, counts.passed), (10, 90));
    assert!(Counts::read(&both, PEERS[1]).is_err());
    assert!(Counts::read(drop, PEERS[0]).is_err());
    assert!(Counts::read(pass, PEERS[0]).is_err());
    assert!(Counts::read(&both.replace("udp", "tcp"), PEERS[0]).is_err());
    assert!(Counts::read(&both.replace("RETURN", "ACCEPT"), PEERS[0]).is_err());
    assert!(Counts::read("", PEERS[0]).is_err());
}
