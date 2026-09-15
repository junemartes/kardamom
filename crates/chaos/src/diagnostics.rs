//! Failure diagnostics: the evidence the CI step and the Make target
//! collect from a cluster left up after a failed shard. Every section
//! is best-effort and prints what it can; the dump never fails.

use std::collections::BTreeMap;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;

use crate::contract::{Node, NodeContract};
use crate::lifecycle::NOMAD_HTTP_PORT;
use crate::nodes::Nodes;
use crate::nomad::{Alloc, Nomad, Streams};
use crate::stages::{head_lines, matching_lines, tail_lines};

const JOBS: [&str; 12] = [
    "aeron",
    "anvil",
    "cluster",
    "sealer",
    "sequencer",
    "executor",
    "validator",
    "ingress",
    "batcher",
    "da-watcher",
    "redis",
    "state-mirror",
];
/// A throwaway group and port, so the probe never collides with the
/// media driver's sockets.
const PROBE_GROUP: &str = "239.192.99.99";
const PROBE_PORT: u16 = 45_999;
const PROBE_PACKETS: u32 = 30;
const PROBE_RECEIVE: Duration = Duration::from_secs(5);
const MULTICAST_NET: &str = "239.192.56.0/24";
const CAPTURE_SECS: &str = "8";
const CAPTURE_PACKETS: &str = "250";
const FLOWS_SHOWN: usize = 40;
const LOG_HEAD: usize = 30;
const LOG_TAIL: usize = 40;
const CLUSTER_LOG_TAIL: usize = 200;
/// Cluster session lifecycle lines, from the sealer and from every client.
/// A tail alone hides them: a stuck sequencer fills its last 40 lines with
/// rewind warnings, and the session events that explain it happened
/// minutes earlier.
const SESSION_MARKERS: &[&str] = &[
    "cluster SESSION",
    "cluster session",
    "cluster egress silent",
    "RESYNC",
];
const SESSION_EVENTS: usize = 60;
const AERON_ERRORS: &str = "for f in /opt/kardamom/cluster/*error*.log /opt/kardamom/aeron-mount/cluster-dir/*error*.log; do [ -f \"$f\" ] && { echo \"--- $f ---\"; cat \"$f\"; }; done";
const CLUSTER_TOOL: &str = r#"inner="$(docker ps --format "{{.Names}}" | grep -m1 "^cluster-")"
[ -n "$inner" ] || { echo "(no inner cluster container running)"; exit 0; }
docker exec "$inner" java -cp /opt/kardamom/cluster-node.jar io.aeron.cluster.ClusterTool /opt/kardamom/cluster errors 2>&1 | tail -40
echo "--- list-members ---"
docker exec "$inner" java -cp /opt/kardamom/cluster-node.jar io.aeron.cluster.ClusterTool /opt/kardamom/cluster list-members 2>&1 | tail -3"#;

/// The dump of one cluster.
pub struct Diagnostics {
    contract: NodeContract,
    nomad: Nomad,
    nodes: Nodes,
}

impl Diagnostics {
    /// Build the dump for the cluster the contract describes.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract has no control node or the
    /// Nomad client cannot be built.
    pub fn new(contract: NodeContract) -> anyhow::Result<Self> {
        let nomad = Nomad::new(&contract.nomad_addr(NOMAD_HTTP_PORT)?)?;
        Ok(Self {
            contract,
            nomad,
            nodes: Nodes,
        })
    }

    /// Print every section to stdout.
    pub async fn dump(&self) {
        crate::log("FAILURE diagnostics: host bridge, multicast, Nomad jobs and allocation logs");
        self.host_bridge().await;
        self.multicast_groups().await;
        self.multicast_probe().await;
        self.capture().await;
        for job in JOBS {
            self.job_section(job).await;
        }
        for sealer in self.contract.of_role("sealer") {
            self.sealer_section(sealer).await;
        }
    }

    fn bridge(&self) -> &str {
        &self.contract.network.bridge
    }

    async fn host_bridge(&self) {
        section("host: bridge-nf-call-iptables");
        println!(
            "{}",
            read_or(
                "/proc/sys/net/bridge/bridge-nf-call-iptables",
                "(br_netfilter not loaded)"
            )
        );
        section("host: iptables FORWARD (counters)");
        println!(
            "{}",
            head_lines(
                &host_cmd("sudo", &["iptables", "-nvL", "FORWARD"]).await,
                25
            )
        );
        section(format!("host: bridge {}", self.bridge()));
        println!(
            "{}",
            host_cmd("ip", &["-d", "link", "show", self.bridge()]).await
        );
        println!(
            "  multicast_snooping={}",
            read_or(
                &format!("/sys/class/net/{}/bridge/multicast_snooping", self.bridge()),
                "?"
            )
        );
    }

    /// The 239.x groups each worker joined. The control node runs no
    /// media driver.
    async fn multicast_groups(&self) {
        let workers = self.contract.nodes.values().filter(|n| !n.control_plane);
        for node in workers {
            println!(
                "----- {}: joined multicast groups (ip maddr, 239.x only) -----",
                node.name
            );
            let out = self
                .nodes
                .exec(&node.container, "ip maddr show dev eth0")
                .await
                .unwrap_or_default();
            println!("{}", groups_in(&out));
        }
    }

    /// A raw-UDP multicast send from sealer-0 to ingress-0 on a
    /// throwaway group, independent of Aeron. Zero packets received
    /// means the bridge forwards no multicast at all.
    async fn multicast_probe(&self) {
        let (Ok(receiver), Ok(sender)) = (
            self.contract.node("ingress-0"),
            self.contract.node("sealer-0"),
        ) else {
            return;
        };
        section(format!(
            "multicast probe {} -> {} (grp {PROBE_GROUP}:{PROBE_PORT})",
            sender.ip, receiver.ip
        ));
        let receive = receiver_script(&receiver.ip.to_string());
        let _ = self
            .nodes
            .docker(&["exec", "-d", &receiver.container, "python3", "-c", &receive])
            .await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let send = sender_script(&sender.ip.to_string());
        let sender_out = self
            .nodes
            .docker_ok(&["exec", &sender.container, "python3", "-c", &send])
            .await
            .unwrap_or_default();
        println!("{sender_out}");
        tokio::time::sleep(PROBE_RECEIVE).await;
        let got = self
            .nodes
            .exec(&receiver.container, "cat /tmp/mcast_probe")
            .await
            .unwrap_or_else(|_| "?".to_string());
        println!(
            "probe: ingress-0 received {got}/{PROBE_PACKETS} packets (0 => bridge not forwarding multicast)"
        );
    }

    /// The cluster multicast flows on the bridge for a few seconds,
    /// summarized by source and destination.
    async fn capture(&self) {
        section(format!(
            "tcpdump {}: cluster multicast src->dst (<= {CAPTURE_SECS}s)",
            self.bridge()
        ));
        let filter = format!("udp and dst net {MULTICAST_NET}");
        let args = [
            "timeout",
            CAPTURE_SECS,
            "tcpdump",
            "-i",
            self.bridge(),
            "-nn",
            "-t",
            "-c",
            CAPTURE_PACKETS,
            &filter,
        ];
        let out = host_cmd("sudo", &args).await;
        if out.is_empty() {
            println!("(tcpdump unavailable or no multicast captured)");
            return;
        }
        println!("{}", flow_summary(&out));
    }

    /// Every allocation of a job, with its task states and its logs.
    /// A dead-but-not-collected allocation still serves its logs, and a
    /// job gone dead is often exactly the failure to explain.
    async fn job_section(&self, job: &str) {
        let Ok(allocs) = self.nomad.allocations(job).await else {
            return;
        };
        for alloc in &allocs {
            self.alloc_section(job, alloc).await;
        }
    }

    async fn alloc_section(&self, job: &str, alloc: &Alloc) {
        println!("----- {job} alloc {}: status -----", alloc.short_id());
        println!(
            "  client_status={} node={}",
            alloc.client_status, alloc.node_name
        );
        for (task, state) in &alloc.task_states {
            println!("  task {task}: {}", task_summary(state));
        }
        let logs = self
            .nomad
            .alloc_logs(alloc, Streams::Both)
            .await
            .unwrap_or_default();
        let tail = if job == "cluster" {
            CLUSTER_LOG_TAIL
        } else {
            LOG_TAIL
        };
        println!(
            "----- {job} alloc {}: logs (head {LOG_HEAD}) -----",
            alloc.short_id()
        );
        println!("{}", head_lines(&logs, LOG_HEAD));
        println!(
            "----- {job} alloc {}: logs (tail {tail}) -----",
            alloc.short_id()
        );
        println!("{}", tail_lines(&logs, tail));
        println!(
            "----- {job} alloc {}: session events (last {SESSION_EVENTS}) -----",
            alloc.short_id()
        );
        println!("{}", matching_lines(&logs, SESSION_MARKERS, SESSION_EVENTS));
    }

    async fn sealer_section(&self, node: &Node) {
        section(format!("{}: Aeron cluster error logs", node.name));
        println!(
            "{}",
            self.nodes
                .exec(&node.container, AERON_ERRORS)
                .await
                .unwrap_or_default()
        );
        section(format!("{}: ClusterTool errors + members", node.name));
        println!(
            "{}",
            self.nodes
                .exec(&node.container, CLUSTER_TOOL)
                .await
                .unwrap_or_default()
        );
    }
}

fn section(title: impl std::fmt::Display) {
    println!("===== {title} =====");
}

fn read_or(path: &str, fallback: &str) -> String {
    std::fs::read_to_string(path).map_or_else(|_| fallback.to_string(), |s| s.trim().to_string())
}

async fn host_cmd(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await
        .map(|o| String::from_utf8_lossy(&o.stdout).trim_end().to_string())
        .unwrap_or_default()
}

/// The 239.x addresses of an `ip maddr` listing, one per line.
fn groups_in(maddr: &str) -> String {
    let mut groups: Vec<&str> = maddr
        .lines()
        .filter(|l| l.contains("inet 239."))
        .filter_map(|l| l.split_whitespace().nth(1))
        .collect();
    groups.sort_unstable();
    groups
        .iter()
        .map(|g| format!("  {g}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `count src -> dst` per flow of a `tcpdump -nn -t` capture, most
/// frequent first. Field 2 is the source, field 4 the destination with
/// a trailing colon.
fn flow_summary(capture: &str) -> String {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let flows = capture.lines().filter_map(|l| {
        let fields: Vec<&str> = l.split_whitespace().collect();
        let dst = fields.get(3)?.trim_end_matches(':');
        Some(format!("{} -> {dst}", fields.get(1)?))
    });
    for flow in flows {
        *counts.entry(flow).or_default() += 1;
    }
    let mut sorted: Vec<(String, usize)> = counts.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    sorted
        .iter()
        .take(FLOWS_SHOWN)
        .map(|(flow, n)| format!("{n:7} {flow}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The state and the last event message of one task.
fn task_summary(state: &serde_json::Value) -> String {
    let last = state
        .get("Events")
        .and_then(serde_json::Value::as_array)
        .and_then(|e| e.last())
        .and_then(|e| e.get("DisplayMessage"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    format!(
        "{} {last}",
        state
            .get("State")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?")
    )
}

fn receiver_script(ip: &str) -> String {
    format!(
        "import socket,struct
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('',{PROBE_PORT}))
mreq=struct.pack('4s4s',socket.inet_aton('{PROBE_GROUP}'),socket.inet_aton('{ip}'))
s.setsockopt(socket.IPPROTO_IP,socket.IP_ADD_MEMBERSHIP,mreq)
s.settimeout({})
n=0
try:
    while True:
        s.recvfrom(2048); n+=1
except socket.timeout:
    pass
open('/tmp/mcast_probe','w').write(str(n))
",
        PROBE_RECEIVE.as_secs()
    )
}

fn sender_script(ip: &str) -> String {
    format!(
        "import socket
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
s.setsockopt(socket.IPPROTO_IP,socket.IP_MULTICAST_IF,socket.inet_aton('{ip}'))
s.setsockopt(socket.IPPROTO_IP,socket.IP_MULTICAST_TTL,1)
for _ in range({PROBE_PACKETS}):
    s.sendto(b'probe',('{PROBE_GROUP}',{PROBE_PORT}))
print('probe: sealer-0 sent {PROBE_PACKETS} packets')
"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flows_count_by_source_and_destination() {
        let capture = "IP 192.168.56.5.4000 > 239.192.56.13.4001: UDP\n\
                       IP 192.168.56.5.4000 > 239.192.56.13.4001: UDP\n\
                       IP 192.168.56.6.4000 > 239.192.56.12.4001: UDP\n";
        let summary = flow_summary(capture);
        let mut lines = summary.lines();
        assert_eq!(
            lines.next().unwrap().trim(),
            "2 192.168.56.5.4000 -> 239.192.56.13.4001"
        );
        assert_eq!(
            lines.next().unwrap().trim(),
            "1 192.168.56.6.4000 -> 239.192.56.12.4001"
        );
    }

    #[test]
    fn groups_keep_only_the_cluster_range_sorted() {
        let maddr = "1:\tinet 224.0.0.1\n\tinet 239.192.56.13\n\tinet 239.192.56.12\n";
        assert_eq!(groups_in(maddr), "  239.192.56.12\n  239.192.56.13");
    }

    #[test]
    fn a_task_summary_names_the_state_and_the_last_event() {
        let state = serde_json::json!({"State": "dead", "Events": [
            {"DisplayMessage": "Started"}, {"DisplayMessage": "Exit Code: 1"}]});
        assert_eq!(task_summary(&state), "dead Exit Code: 1");
    }
}
