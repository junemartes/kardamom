use super::*;

/// Evaluate the documented u32 operators against complete IPv4 packets.
/// The rule under test is parsed; its offsets are not copied into the evaluator.
struct U32<'a> {
    packet: &'a [u8],
}

impl U32<'_> {
    fn accepts(&self, program: &str) -> bool {
        program
            .split("&&")
            .all(|test| self.test(test).unwrap_or(false))
    }

    fn test(&self, test: &str) -> Option<bool> {
        let (location, expected) = test.split_once('=')?;
        let tokens = location
            .replace(">>", " >> ")
            .replace('&', " & ")
            .replace('@', " @ ");
        let tokens: Vec<_> = tokens.split_whitespace().collect();
        let first = self.read(Self::number(tokens.first()?)?)?;
        let (_, value) = tokens[1..].as_chunks::<2>().0.iter().try_fold(
            (0u32, first),
            |(base, value), pair| {
                let number = Self::number(pair[1])?;
                match pair[0] {
                    "&" => Some((base, value & number)),
                    ">>" => Some((base, value.checked_shr(number)?)),
                    "@" => {
                        let base = base.checked_add(value)?;
                        Some((base, self.read(base.checked_add(number)?)?))
                    }
                    _ => None,
                }
            },
        )?;
        Some(value == Self::number(expected.trim())?)
    }

    fn read(&self, offset: u32) -> Option<u32> {
        let offset = usize::try_from(offset).ok()?;
        Some(u32::from_be_bytes(
            self.packet
                .get(offset..offset.checked_add(4)?)?
                .try_into()
                .ok()?,
        ))
    }

    fn number(value: &str) -> Option<u32> {
        value.strip_prefix("0x").map_or_else(
            || value.parse().ok(),
            |hex| u32::from_str_radix(hex, 16).ok(),
        )
    }
}

struct Datagram {
    bytes: Vec<u8>,
    payload: usize,
}

impl Datagram {
    /// The Aeron DATA wire layout: frame length, version/flags/type,
    /// term offset, session, stream, term and reserved value, all little endian.
    fn data(stream: u32, ihl: u8) -> Self {
        let ip = usize::from(ihl) * 4;
        let payload = ip + 8;
        let mut bytes = vec![0u8; payload + 32];
        bytes[0] = 0x40 | ihl;
        bytes[9] = 17;
        bytes[payload..payload + 4].copy_from_slice(&32u32.to_le_bytes());
        bytes[payload + 5] = 0xc0;
        bytes[payload + 6] = 1;
        bytes[payload + 12..payload + 16].copy_from_slice(&1002u32.to_le_bytes());
        bytes[payload + 16..payload + 20].copy_from_slice(&stream.to_le_bytes());
        Self { bytes, payload }
    }

    fn matches(&self) -> bool {
        U32 {
            packet: &self.bytes,
        }
        .accepts(RECEIPT)
    }
}

#[test]
fn receipt_frames_match_with_ip_options_and_all_aeron_fragment_flags() {
    [5, 6, 15]
        .into_iter()
        .flat_map(|ihl| [0, 0x40, 0x80, 0xc0, 0x20].map(|flags| (ihl, flags)))
        .for_each(|(ihl, flags)| {
            let mut packet = Datagram::data(1002, ihl);
            packet.bytes[packet.payload + 5] = flags;
            assert!(packet.matches());
        });
}

#[test]
fn a_colocated_sealer_keeps_its_cluster_egress_and_control_frames() {
    for stream in [101, 102, 1003, 1015, 2000] {
        assert!(!Datagram::data(stream, 5).matches(), "stream {stream}");
    }
    for frame_type in [0, 2, 3, 4, 5, 6] {
        let mut control = Datagram::data(1002, 5);
        control.bytes[control.payload + 6] = frame_type;
        assert!(!control.matches(), "frame type {frame_type}");
    }
}

#[test]
fn truncated_or_fragmented_ip_packets_and_another_protocol_version_do_not_match() {
    let mut packet = Datagram::data(1002, 5);
    packet.bytes[6] = 0x20;
    assert!(!packet.matches());
    packet.bytes[6] = 0;
    packet.bytes[7] = 1;
    assert!(!packet.matches());
    packet.bytes[7] = 0;
    packet.bytes[packet.payload + 4] = 1;
    assert!(!packet.matches());
    packet.bytes[packet.payload + 4] = 0;
    packet.bytes.truncate(packet.payload + 19);
    assert!(!packet.matches());
}

#[test]
fn only_real_drop_counters_prove_that_receipts_hit_the_fault() {
    assert!(
        Rules::require_drops(
            "Chain KDM_LOOKUP_RX (1 references)\n0 0 DROP udp -- anywhere anywhere"
        )
        .is_err()
    );
    assert!(Rules::require_drops("500 9000 ACCEPT udp -- anywhere anywhere").is_err());
    assert!(Rules::require_drops("3 200 DROP udp -- anywhere anywhere").is_ok());
}

#[test]
fn the_packet_selector_agrees_with_the_deployment_transport() {
    let channels = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/cluster/config/channels.toml.tpl"
    ));
    assert!(
        channels
            .lines()
            .any(|line| line == "tx_receipts_stream_id = 1002")
    );
    let vars = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/cluster/ansible/group_vars/all.yml"
    ));
    assert!(
        vars.lines()
            .any(|line| line == format!("  executor_nonce_query: {QUERY_PORT}"))
    );
    let rules = Rules::new(vec![Ipv4Addr::new(192, 168, 56, 17)]).arm();
    assert!(!rules.contains("ip route"));
    assert!(
        rules
            .lines()
            .filter(|line| line.contains("-p udp"))
            .all(|line| line.contains("--u32"))
    );
}

struct Table {
    directory: tempfile::TempDir,
}

impl Table {
    fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("iptables");
        std::fs::write(&executable, include_str!("test_iptables.py")).unwrap();
        std::fs::set_permissions(executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(
            directory.path().join("table.json"),
            r#"{"INPUT":[["-j","KEEP"]],"OUTPUT":[],"KEEP":[["-j","ACCEPT"]]}"#,
        )
        .unwrap();
        Self { directory }
    }

    fn run(&self, script: &str, failure: &str) -> bool {
        std::process::Command::new("bash")
            .args(["-c", script])
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin", self.directory.path().display()),
            )
            .env("LOOKUP_TABLE", self.directory.path().join("table.json"))
            .env("LOOKUP_FAIL", failure)
            .status()
            .unwrap()
            .success()
    }

    fn read(&self) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(self.directory.path().join("table.json")).unwrap())
            .unwrap()
    }

    fn clears_partial_arm(failure: &str) {
        let table = Self::new();
        let before = table.read();
        let rules = Rules::new(vec![Ipv4Addr::new(192, 168, 56, 17)]);
        assert!(!table.run(&rules.arm(), failure));
        assert!(table.run(&Rules::clear_input(), ""));
        assert!(table.run(&Rules::clear_output(), ""));
        assert_eq!(table.read(), before);
    }
}

#[test]
fn each_partial_arm_cleans_up_without_touching_unrelated_rules() {
    ["-N KDM_LOOKUP_TX", "-A KDM_LOOKUP_TX", "-I OUTPUT"]
        .into_iter()
        .for_each(Table::clears_partial_arm);
}

#[test]
fn allowing_queries_keeps_the_receipt_fault_until_final_cleanup() {
    let table = Table::new();
    let before = table.read();
    let rules = Rules::new(vec![Ipv4Addr::new(192, 168, 56, 17)]);
    assert!(table.run(&rules.arm(), ""));
    let armed = table.read();
    assert_eq!(armed[OUTPUT].as_array().unwrap().len(), 3);
    assert_eq!(armed[INPUT].as_array().unwrap().len(), 1);
    assert!(table.run(&Rules::allow_queries(), ""));
    let answered = table.read();
    assert_eq!(answered[OUTPUT], serde_json::json!([]));
    assert_eq!(answered[INPUT], armed[INPUT]);
    assert!(table.run(&Rules::clear_input(), ""));
    assert!(table.run(&Rules::clear_output(), ""));
    assert_eq!(table.read(), before);
}
