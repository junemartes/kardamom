//! Match receipt DATA frames, including on an ephemeral Aeron receive port.

use std::fmt::Write;
use std::net::Ipv4Addr;

const INPUT: &str = "KDM_LOOKUP_RX";
const OUTPUT: &str = "KDM_LOOKUP_TX";
/// IPv4 IHL locates UDP. DATA has version0/type1 and its little-endian
/// stream id at payload offset16. Flags vary across fragmented messages.
const RECEIPT: &str =
    "4&0x3FFF=0 && 0>>22&0x3C@12&0xFF00FFFF=0x00000100 && 0>>22&0x3C@24=0xEA030000";
const QUERY_PORT: u16 = 9024;

pub(super) struct Rules {
    executors: Vec<Ipv4Addr>,
}

impl Rules {
    pub(super) fn new(executors: Vec<Ipv4Addr>) -> Self {
        Self { executors }
    }

    pub(super) fn arm(&self) -> String {
        let mut script = format!("set -eu\niptables -w 5 -N {INPUT}\niptables -w 5 -N {OUTPUT}\n");
        self.executors.iter().for_each(|ip| {
            writeln!(
                script,
                "iptables -w 5 -A {INPUT} -p udp -s {ip} -m u32 --u32 '{RECEIPT}' -j DROP"
            )
            .expect("String write cannot fail");
            writeln!(
                script,
                "iptables -w 5 -A {OUTPUT} -p tcp -d {ip} --dport {QUERY_PORT} -j DROP"
            )
            .expect("String write cannot fail");
        });
        let cache = [6379, 26379]
            .map(|port| format!("iptables -w 5 -A {OUTPUT} -p tcp --dport {port} -j DROP"))
            .join("\n");
        writeln!(
            script,
            "{cache}\niptables -w 5 -I INPUT 1 -j {INPUT}\niptables -w 5 -I OUTPUT 1 -j {OUTPUT}"
        )
        .expect("String write cannot fail");
        script
    }

    pub(super) fn allow_queries() -> String {
        format!("set -eu\niptables -w 5 -F {OUTPUT}\niptables -w 5 -Z {INPUT}")
    }

    pub(super) fn counters() -> String {
        format!("iptables -w 5 -L {INPUT} -n -v -x")
    }

    pub(super) fn require_drops(body: &str) -> anyhow::Result<()> {
        let dropped = body
            .lines()
            .filter_map(|line| {
                let mut fields = line.split_whitespace();
                let packets = fields.next()?.parse::<u64>().ok()?;
                fields.next()?;
                (fields.next()? == "DROP").then_some(packets)
            })
            .any(|packets| packets > 0);
        anyhow::ensure!(
            dropped,
            "lookup-blackout: no receipt DATA packet hit the fault: {body}"
        );
        Ok(())
    }

    pub(super) fn clear_input() -> String {
        Self::clear("INPUT", INPUT)
    }

    pub(super) fn clear_output() -> String {
        Self::clear("OUTPUT", OUTPUT)
    }

    fn clear(parent: &str, chain: &str) -> String {
        format!(
            "set -eu\nif iptables -w 5 -S {chain} >/dev/null 2>&1; then\nif iptables -w 5 -C {parent} -j {chain} 2>/dev/null; then iptables -w 5 -D {parent} -j {chain}; fi\niptables -w 5 -F {chain}\niptables -w 5 -X {chain}\nfi"
        )
    }
}

#[cfg(test)]
mod tests;
