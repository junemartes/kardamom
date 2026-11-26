//! The loopback ports of the tests, in one map:
//!
//! | ports | owner |
//! |---|---|
//! | 8010 to 8021 | the archive of the test driver image and of `just aeron-driver-up`; the refetch tests add 8012 and 8013 |
//! | 10000 to 19999 | [`PortBlock`]: the fixed Aeron ports of one test binary in each block |
//! | 20000 to 31999 | [`free_port`] and [`free_udp_port`]: one port for each call |
//! | 32768 to 60999 | the ephemeral range of the kernel (`ip_local_port_range`), by default |
//!
//! The kernel binds each socket that asks for port 0 to a free port of
//! its ephemeral range. A fixed port in that range can thus belong to
//! another socket before the test binds it. A media driver binds a
//! channel port after the add call returns, so a failed bind is only an
//! entry in the error log of the driver, and the test only times out.
//! Thus no test port lies in the ephemeral range.

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};

/// A free loopback TCP port, bound and released, so the caller can pass the
/// address to [`crate::init`] or to a child process before anything listens
/// on it.
///
/// # Panics
///
/// Panics if no port of the test range binds.
#[must_use]
pub fn free_port() -> SocketAddr {
    TestPorts::shared().claim(|port| TcpListener::bind(("127.0.0.1", port))?.local_addr())
}

/// A free loopback UDP port, bound and released. See [`free_port`].
///
/// # Panics
///
/// Panics if no port of the test range binds.
#[must_use]
pub fn free_udp_port() -> SocketAddr {
    TestPorts::shared().claim(|port| UdpSocket::bind(("127.0.0.1", port))?.local_addr())
}

/// The ports that [`free_port`] and [`free_udp_port`] give: 20000 to
/// 31999.
///
/// A child process binds a released port later. Below the ephemeral
/// range, only an explicit bind can take the port in that window. One
/// cursor for each process also keeps two calls from giving the same
/// port. The start depends on the process id, so two test binaries at
/// once start apart.
struct TestPorts {
    next: AtomicU32,
}

impl TestPorts {
    const FIRST: u32 = 20_000;
    const COUNT: u32 = 12_000;

    fn shared() -> &'static Self {
        static PORTS: OnceLock<TestPorts> = OnceLock::new();
        PORTS.get_or_init(|| Self {
            next: AtomicU32::new(std::process::id() % Self::COUNT),
        })
    }

    /// The next port of the range, in cursor order, wrapping at the end.
    fn next_port(&self) -> u16 {
        let offset = self.next.fetch_add(1, Ordering::Relaxed) % Self::COUNT;
        u16::try_from(Self::FIRST + offset).expect("the test range ends below u16::MAX")
    }

    /// The first port, from the cursor on, that `bind` accepts.
    fn claim(&self, bind: impl Fn(u16) -> std::io::Result<SocketAddr>) -> SocketAddr {
        (0..Self::COUNT)
            .map(|_| self.next_port())
            .find_map(|port| bind(port).ok())
            .expect("a free loopback port in 20000-31999")
    }
}

/// The block of fixed Aeron UDP ports of one test binary: 1000 ports in
/// ten slots of 100.
///
/// The test binaries run at the same time in CI, so two blocks never
/// overlap. A test with a host-native driver shares the network of the
/// host. A test with a driver in a container shares the network of the
/// container with the port-0 sockets of that driver. A process takes the
/// slot of its process id, so two runs of one binary on one host start
/// apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortBlock {
    /// `kardamom-ingress`, `tests/ingress_recovery.rs`.
    IngressRecovery,
    /// `kardamom-log`, `tests/mdc_transport.rs`.
    MdcTransport,
    /// `kardamom-log`, `tests/mds_destination_removal.rs`.
    MdsDestinationRemoval,
    /// `kardamom-log`, `tests/mds_remove_destination.rs`.
    MdsRemoveDestination,
    /// `kardamom-log`, `tests/discovered_tx_data.rs`.
    DiscoveredTxData,
    /// `kardamom-log`, `tests/transport_pressure.rs`.
    TransportPressure,
    /// `kardamom-log`, `tests/driver_error_log.rs`.
    DriverErrorLog,
    /// `kardamom-cluster-adapter`, `live::fragmented_tests`.
    FragmentedControl,
}

impl PortBlock {
    /// Every block. A new block also goes here, so that the tests check it.
    #[cfg(test)]
    const ALL: [Self; 8] = [
        Self::IngressRecovery,
        Self::MdcTransport,
        Self::MdsDestinationRemoval,
        Self::MdsRemoveDestination,
        Self::DiscoveredTxData,
        Self::TransportPressure,
        Self::DriverErrorLog,
        Self::FragmentedControl,
    ];
    /// The ports of one block.
    const SPAN: u16 = 1_000;

    /// The first port of the block.
    const fn first(self) -> u16 {
        match self {
            Self::IngressRecovery => 10_000,
            Self::MdcTransport => 11_000,
            Self::MdsDestinationRemoval => 12_000,
            Self::MdsRemoveDestination => 13_000,
            Self::DiscoveredTxData => 14_000,
            Self::TransportPressure => 15_000,
            Self::DriverErrorLog => 16_000,
            Self::FragmentedControl => 17_000,
        }
    }

    /// The last port of the block.
    #[cfg(test)]
    const fn last(self) -> u16 {
        self.first() + Self::SPAN - 1
    }

    /// The slot of this test process.
    #[must_use]
    pub fn of_process(self) -> PortSlot {
        self.slot(std::process::id())
    }

    /// Port `offset` of the slot of this test process.
    ///
    /// # Panics
    ///
    /// Panics if `offset` is 100 or more. See [`PortSlot::port`].
    #[must_use]
    pub fn port(self, offset: u16) -> u16 {
        self.of_process().port(offset)
    }

    /// The slot of `index`, modulo the count of slots.
    fn slot(self, index: u32) -> PortSlot {
        let slot = u16::try_from(index % u32::from(PortSlot::COUNT))
            .expect("the slot count is below u16::MAX");
        PortSlot {
            first: self.first() + slot * PortSlot::SPAN,
        }
    }
}

/// The 100 fixed ports of one block for one test process.
#[derive(Clone, Copy, Debug)]
pub struct PortSlot {
    first: u16,
}

impl PortSlot {
    /// The ports of one slot.
    const SPAN: u16 = 100;
    /// The slots of one block.
    const COUNT: u16 = PortBlock::SPAN / Self::SPAN;

    /// Port `offset` of the slot.
    ///
    /// # Panics
    ///
    /// Panics if `offset` is 100 or more: that port is in the next slot.
    #[must_use]
    pub fn port(self, offset: u16) -> u16 {
        (offset < Self::SPAN)
            .then(|| self.first + offset)
            .expect("the offset of a port in its slot is below 100")
    }

    /// The first port of the slot without its last two digits: port
    /// `offset` is `hundreds * 100 + offset`. A channel template can thus
    /// spell a port as digits, for example `{hundreds}{lane}{replica}`.
    #[must_use]
    pub fn hundreds(self) -> u16 {
        self.first / Self::SPAN
    }
}

#[cfg(test)]
#[path = "ports_tests.rs"]
mod tests;
