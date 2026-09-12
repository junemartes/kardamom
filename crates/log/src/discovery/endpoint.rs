//! Endpoints: the advertised address, port allocation, and the Aeron URIs
//! of the dynamic MDC transport.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};

use crate::config::InterfaceSelector;
use crate::error::LogError;

/// The URI of every discovered subscription: a multi-destination
/// subscription with no destination until the reconciler attaches one.
pub const MANUAL_SUBSCRIPTION_URI: &str = "aeron:udp?control-mode=manual";

/// Resolve the one IPv4 address `selector` names on this host. Zero or
/// several matches are rejected: an advertised endpoint must be
/// unambiguous.
///
/// # Errors
///
/// Returns an error if the interfaces cannot be listed, if no interface
/// matches, or if more than one address matches.
pub fn advertise_ip(selector: &InterfaceSelector) -> Result<Ipv4Addr, LogError> {
    let interfaces = if_addrs::get_if_addrs()
        .map_err(|e| LogError::Discovery(format!("list network interfaces: {e}")))?;
    let matches: Vec<Ipv4Addr> = interfaces
        .iter()
        .filter_map(|i| match (i.ip(), selector) {
            (IpAddr::V4(ip), InterfaceSelector::Name(name)) => (i.name == *name).then_some(ip),
            (IpAddr::V4(ip), InterfaceSelector::Network { .. }) => {
                selector.contains(ip).then_some(ip)
            }
            (IpAddr::V6(_), _) => None,
        })
        .collect();
    match matches.as_slice() {
        [ip] => Ok(*ip),
        [] => Err(LogError::Discovery(format!(
            "advertise interface {:?} matches no IPv4 address on this host",
            String::from(selector.clone())
        ))),
        many => Err(LogError::Discovery(format!(
            "advertise interface {:?} is ambiguous: {many:?}",
            String::from(selector.clone())
        ))),
    }
}

/// A UDP port range Nomad allocated to this process, `first-last`
/// inclusive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortRange {
    first: u16,
    last: u16,
}

impl std::str::FromStr for PortRange {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (first, last) = s
            .split_once('-')
            .ok_or_else(|| format!("port range {s}: expected first-last"))?;
        let first: u16 = first
            .trim()
            .parse()
            .map_err(|e| format!("port range {s}: bad first port: {e}"))?;
        let last: u16 = last
            .trim()
            .parse()
            .map_err(|e| format!("port range {s}: bad last port: {e}"))?;
        if first == 0 || last < first {
            return Err(format!(
                "port range {s}: first must be at least 1 and at most last"
            ));
        }
        Ok(Self { first, last })
    }
}

/// Hands out bindable UDP ports on the advertised address: the next free
/// port of the allocated range, or an OS-chosen port when no range was
/// allocated. Every port is probed with a bind before it is handed out,
/// so a record never advertises a port another process holds.
#[derive(Debug)]
pub struct PortAllocator {
    ip: Ipv4Addr,
    range: Option<PortRange>,
    next: u16,
}

impl PortAllocator {
    #[must_use]
    pub fn new(ip: Ipv4Addr, range: Option<PortRange>) -> Self {
        Self {
            ip,
            range,
            next: range.map_or(0, |r| r.first),
        }
    }

    /// The next bindable port.
    ///
    /// # Errors
    ///
    /// Returns an error if the range is exhausted, or if no port binds.
    pub fn allocate(&mut self) -> Result<u16, LogError> {
        match self.range {
            None => Self::ephemeral(self.ip),
            Some(range) => self.next_in_range(range),
        }
    }

    fn ephemeral(ip: Ipv4Addr) -> Result<u16, LogError> {
        let socket = UdpSocket::bind((ip, 0))
            .map_err(|e| LogError::Discovery(format!("bind an ephemeral UDP port on {ip}: {e}")))?;
        let port = socket
            .local_addr()
            .map_err(|e| LogError::Discovery(format!("read the bound port on {ip}: {e}")))?
            .port();
        Ok(port)
    }

    fn next_in_range(&mut self, range: PortRange) -> Result<u16, LogError> {
        let candidates = self.next..=range.last;
        let found = candidates
            .clone()
            .find(|port| UdpSocket::bind((self.ip, *port)).is_ok());
        let port = found.ok_or_else(|| {
            LogError::Discovery(format!(
                "no free UDP port left in {}-{} on {}",
                range.first, range.last, self.ip
            ))
        })?;
        self.next = port.saturating_add(1);
        Ok(port)
    }
}

/// The URI of a dynamic MDC publication bound to `control`, with the
/// configured flow control when `flow_control` is not empty.
#[must_use]
pub fn publication_uri(control: SocketAddr, flow_control: &str) -> String {
    let mut uri = format!("aeron:udp?control={control}|control-mode=dynamic");
    if !flow_control.is_empty() {
        uri.push_str("|fc=");
        uri.push_str(flow_control);
    }
    uri
}

/// The destination a subscriber attaches to join the publication whose
/// control endpoint is `control`, receiving on an OS-chosen port of
/// `local`.
#[must_use]
pub fn destination_uri(local: IpAddr, control: SocketAddr) -> String {
    format!("aeron:udp?endpoint={local}:0|control={control}|control-mode=dynamic")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_range_parses_and_rejects_bad_bounds() {
        let r: PortRange = "40300-40319".parse().unwrap();
        assert_eq!(
            r,
            PortRange {
                first: 40300,
                last: 40319
            }
        );
        assert!("40319-40300".parse::<PortRange>().is_err());
        assert!("0-5".parse::<PortRange>().is_err());
        assert!("abc".parse::<PortRange>().is_err());
    }

    #[test]
    fn ranged_allocation_skips_a_held_port() {
        let ip = Ipv4Addr::LOCALHOST;
        let probe = UdpSocket::bind((ip, 0)).unwrap();
        let held = probe.local_addr().unwrap().port();
        let range = PortRange {
            first: held,
            last: held.saturating_add(5),
        };
        let mut alloc = PortAllocator::new(ip, Some(range));
        let got = alloc.allocate().unwrap();
        assert_ne!(got, held, "the held port is skipped");
        assert!(got > held && got <= range.last);
        let next = alloc.allocate().unwrap();
        assert!(next > got, "ports are handed out in order");
    }

    #[test]
    fn uris_carry_control_mode_and_flow_control() {
        let control: SocketAddr = "192.168.56.31:40300".parse().unwrap();
        assert_eq!(
            publication_uri(control, ""),
            "aeron:udp?control=192.168.56.31:40300|control-mode=dynamic"
        );
        assert_eq!(
            publication_uri(control, "min"),
            "aeron:udp?control=192.168.56.31:40300|control-mode=dynamic|fc=min"
        );
        assert_eq!(
            destination_uri(IpAddr::V4(Ipv4Addr::new(192, 168, 56, 41)), control),
            "aeron:udp?endpoint=192.168.56.41:0|control=192.168.56.31:40300|control-mode=dynamic"
        );
    }

    #[test]
    fn network_selector_matches_loopback() {
        let selector = InterfaceSelector::try_from("127.0.0.0/8".to_string()).unwrap();
        assert_eq!(advertise_ip(&selector).unwrap(), Ipv4Addr::LOCALHOST);
        let none = InterfaceSelector::try_from("203.0.113.0/24".to_string()).unwrap();
        assert!(advertise_ip(&none).is_err());
    }
}
