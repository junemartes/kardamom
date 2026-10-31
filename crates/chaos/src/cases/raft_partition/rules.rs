//! Only outbound peer-bound Raft UDP enters the fault chain.

use std::fmt::Write;
use std::net::Ipv4Addr;

use super::{CHAIN, PORTS};

pub(super) const LOSS_CHAIN: &str = "KDM_CHAOS_RAFT_LOSS";

pub(super) struct Rules<'a> {
    chain: &'static str,
    peers: &'a [Ipv4Addr],
    lossy: bool,
}

impl<'a> Rules<'a> {
    pub(super) fn partition(peers: &'a [Ipv4Addr]) -> Self {
        Self {
            chain: CHAIN,
            peers,
            lossy: false,
        }
    }

    pub(super) fn loss(peers: &'a [Ipv4Addr]) -> Self {
        Self {
            chain: LOSS_CHAIN,
            peers,
            lossy: true,
        }
    }

    pub(super) fn arm(&self) -> String {
        let rules = self
            .peers
            .iter()
            .flat_map(|peer| PORTS.into_iter().map(move |port| (peer, port)))
            .fold(String::new(), |script, (peer, port)| {
                self.append(script, *peer, port)
            });
        let chain = self.chain;
        format!(
            "set -eu\niptables -w 5 -N {chain}\n{rules}iptables -w 5 -I OUTPUT 1 -j {chain}\niptables -w 5 -C OUTPUT -j {chain}"
        )
    }

    fn append(&self, mut script: String, peer: Ipv4Addr, port: u16) -> String {
        let selector = format!("-p udp -d {peer} --dport {port}");
        let probability = if self.lossy {
            " -m statistic --mode random --probability 0.1"
        } else {
            ""
        };
        writeln!(
            script,
            "iptables -w 5 -A {} {selector}{probability} -j DROP",
            self.chain
        )
        .expect("writing to a String is infallible");
        if self.lossy {
            writeln!(
                script,
                "iptables -w 5 -A {} {selector} -j RETURN",
                self.chain
            )
            .expect("writing to a String is infallible");
        }
        script
    }

    pub(super) fn cleanup(chain: &str) -> String {
        format!(
            "set -eu\nif iptables -w 5 -S {chain} >/dev/null 2>&1; then\niptables -w 5 -D OUTPUT -j {chain} 2>/dev/null || true\niptables -w 5 -F {chain}\niptables -w 5 -X {chain}\nfi"
        )
    }
}
