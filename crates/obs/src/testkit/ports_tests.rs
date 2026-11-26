use super::{PortBlock, PortSlot, TestPorts, free_port, free_udp_port};

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
        Ok(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
    });
    let tried = tried.into_inner();
    assert_eq!(tried.len(), 3);
    assert_eq!(got.port(), tried[2]);
    assert!(tried[..2].iter().all(|p| *p != got.port()));
}

/// Two blocks never share a port, and every block ends below the ports
/// that `free_port` gives.
#[test]
fn the_blocks_lie_apart_below_the_single_ports() {
    let mut firsts: Vec<u16> = PortBlock::ALL.iter().map(|b| b.first()).collect();
    firsts.sort_unstable();
    let apart = firsts.windows(2).all(|w| w[1] - w[0] >= PortBlock::SPAN);
    assert!(apart, "two blocks overlap: {firsts:?}");
    let last = PortBlock::ALL.iter().map(|b| b.last()).max().unwrap();
    assert!(u32::from(last) < TestPorts::FIRST, "a block ends at {last}");
}

/// Every port of every slot stays in its block, and the slots of one
/// block do not overlap.
#[test]
fn every_slot_stays_in_its_block() {
    let outside: Vec<(PortBlock, u32)> = PortBlock::ALL
        .iter()
        .flat_map(|block| (0..u32::from(PortSlot::COUNT)).map(move |i| (*block, i)))
        .filter(|(block, i)| {
            let slot = block.slot(*i);
            slot.port(0) < block.first() || slot.port(PortSlot::SPAN - 1) > block.last()
        })
        .collect();
    assert_eq!(outside, Vec::new(), "these slots leave their block");
    let mut ports: Vec<u16> = (0..u32::from(PortSlot::COUNT))
        .map(|i| PortBlock::IngressRecovery.slot(i).port(0))
        .collect();
    ports.dedup();
    assert_eq!(ports.len(), usize::from(PortSlot::COUNT));
}

/// A template spells a port as the hundreds of its slot and two digits.
#[test]
fn the_hundreds_and_two_digits_spell_a_port_of_the_slot() {
    let slot = PortBlock::IngressRecovery.slot(3);
    assert_eq!(
        format!("{}{}{}", slot.hundreds(), 4, 1),
        slot.port(41).to_string()
    );
}

/// The kernel of this host gives no port-0 socket a test port.
#[cfg(target_os = "linux")]
#[test]
fn every_test_port_lies_below_the_ephemeral_range() {
    let range = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").unwrap();
    let floor: u32 = range.split_whitespace().next().unwrap().parse().unwrap();
    let last = TestPorts::FIRST + TestPorts::COUNT - 1;
    assert!(
        last < floor,
        "the test ports end at {last}, in the ephemeral range from {floor}"
    );
}
