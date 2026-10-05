//! Endpoints: the advertised address and the Aeron URIs of the dynamic
//! MDC transport.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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

/// The URI of a dynamic MDC publication with its control endpoint on
/// `ip` and port 0, with the configured flow control when `flow_control`
/// is not empty. The media driver binds an OS-chosen port, and
/// [`crate::aeron_live::AeronRuntime::open_mdc_publication`] reads it back.
#[must_use]
pub fn publication_uri(ip: IpAddr, flow_control: &str) -> String {
    let control = SocketAddr::new(ip, 0);
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
    fn uris_carry_control_mode_and_flow_control() {
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 56, 31));
        assert_eq!(
            publication_uri(ip, ""),
            "aeron:udp?control=192.168.56.31:0|control-mode=dynamic"
        );
        assert_eq!(
            publication_uri(ip, "min"),
            "aeron:udp?control=192.168.56.31:0|control-mode=dynamic|fc=min"
        );
        let control: SocketAddr = "192.168.56.31:40300".parse().unwrap();
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
