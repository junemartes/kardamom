//! A real Sentinel group for the Docker-gated tests of this crate and of
//! its users: a primary, a replica and three Sentinels on one Docker
//! network, each on a host port that the fixture holds for its life.
#![allow(
    clippy::missing_panics_doc,
    reason = "a test fixture: a failed setup step or a failed check panics the test"
)]
#![allow(
    clippy::must_use_candidate,
    reason = "a test fixture; its callers read every value they need"
)]

use std::time::{Duration, SystemTime};

use crate::{AccountCache, CacheConfig};
use alloy_primitives::{Address, U256};
use kardamom_types::{AccountRow, BPosition};
use redis::aio::MultiplexedConnection;
use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const BOUND: Duration = Duration::from_secs(30);
const PAUSE: Duration = Duration::from_millis(50);

#[derive(Clone, Copy)]
pub enum Fault {
    Freeze,
    Kill,
}

pub struct Fixture {
    pub cache: AccountCache,
    /// The configuration the cache client connected with.
    pub cfg: CacheConfig,
    pub primary: ContainerAsync<GenericImage>,
    pub replica: ContainerAsync<GenericImage>,
    sentinels: [ContainerAsync<GenericImage>; 3],
    /// The host ports of the five containers, held until the fixture
    /// ends, also while the containers are down.
    _ports: [PortLease; 5],
}

/// A host port that this test holds for its whole life. The lock file
/// keeps a second test run on the same host off the port, also while
/// the container that maps it is down for a restart.
pub struct PortLease {
    port: u16,
    _lock: std::fs::File,
}

impl PortLease {
    /// The reserved range lies below the kernel's ephemeral range, so no
    /// outgoing connection of another process takes a port in it.
    const RANGE: std::ops::Range<u16> = 20_000..30_000;

    /// Take the first free port of the range, from a start that differs
    /// per process.
    fn take() -> Self {
        let span = u32::from(Self::RANGE.end - Self::RANGE.start);
        let start = std::process::id().wrapping_mul(7919) % span;
        (0..span)
            .map(|step| (start + step) % span)
            .find_map(|offset| Self::try_port(Self::RANGE.start + u16::try_from(offset).unwrap()))
            .expect("a free port in the reserved range")
    }

    fn try_port(port: u16) -> Option<Self> {
        let dir = std::env::temp_dir().join("kardamom-test-ports");
        std::fs::create_dir_all(&dir).ok()?;
        let lock = std::fs::File::create(dir.join(format!("{port}.lock"))).ok()?;
        lock.try_lock().ok()?;
        std::net::TcpListener::bind(("127.0.0.1", port)).ok()?;
        Some(Self { port, _lock: lock })
    }
}

impl Fixture {
    pub async fn new() -> Self {
        let token = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let network = format!("component-sentinel-{}-{token}", std::process::id());
        let ports: [PortLease; 5] = std::array::from_fn(|_| PortLease::take());
        let primary = Self::redis(&network, &[], ports[0].port).await;
        let primary_ip = primary.get_bridge_ip_address().await.unwrap().to_string();
        let replica = Self::redis(
            &network,
            &["--replicaof", &primary_ip, "6379"],
            ports[1].port,
        )
        .await;
        let (a, b, c) = tokio::join!(
            Self::sentinel(&network, &primary_ip, ports[2].port),
            Self::sentinel(&network, &primary_ip, ports[3].port),
            Self::sentinel(&network, &primary_ip, ports[4].port)
        );
        let sentinels = [a, b, c];
        let mut addresses = Vec::new();
        for sentinel in &sentinels {
            addresses.push(Self::url(sentinel, 26379).await);
        }
        let cfg = CacheConfig {
            sentinels: addresses,
            ..CacheConfig::default()
        };
        let cache = AccountCache::connect(&cfg).await.unwrap();
        let fixture = Self {
            cache,
            cfg,
            primary,
            replica,
            sentinels,
            _ports: ports,
        };
        fixture
            .cache
            .write_rows(BPosition::from_index(10), &[Self::row(1)])
            .await
            .unwrap();
        tokio::time::timeout(BOUND, fixture.ready())
            .await
            .expect("replica and Sentinel quorum must be ready before the fault");
        fixture
    }

    async fn redis(network: &str, extra: &[&str], host_port: u16) -> ContainerAsync<GenericImage> {
        let mut command = vec![
            "redis-server",
            "--protected-mode",
            "no",
            "--save",
            "",
            "--appendonly",
            "no",
        ];
        command.extend_from_slice(extra);
        GenericImage::new("redis", "7-alpine")
            .with_exposed_port(6379_u16.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .with_network(network)
            .with_mapped_port(host_port, 6379_u16.tcp())
            .with_cmd(command)
            .start()
            .await
            .unwrap()
    }

    async fn sentinel(
        network: &str,
        primary: &str,
        host_port: u16,
    ) -> ContainerAsync<GenericImage> {
        let script = format!(
            "cat > /tmp/sentinel.conf <<'CONF'\nport 26379\nprotected-mode no\nsentinel monitor kardamom {primary} 6379 2\nsentinel down-after-milliseconds kardamom 500\nsentinel failover-timeout kardamom 5000\nCONF\nexec redis-server /tmp/sentinel.conf --sentinel"
        );
        GenericImage::new("redis", "7-alpine")
            .with_entrypoint("sh")
            .with_exposed_port(26379_u16.tcp())
            .with_wait_for(WaitFor::message_on_stdout("+monitor master"))
            .with_network(network)
            .with_mapped_port(host_port, 26379_u16.tcp())
            .with_cmd(["-c", &script])
            .start()
            .await
            .unwrap()
    }

    /// Kill the primary, the replica and every Sentinel at once. No Redis
    /// process keeps data on disk, so the group loses every key.
    pub async fn lose_everything(&self) {
        let ids: Vec<&str> = self.all().map(ContainerAsync::id).collect();
        let output = tokio::process::Command::new("docker")
            .arg("kill")
            .args(&ids)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "{output:?}");
    }

    /// Start the group again in its first order, and wait until the
    /// Sentinels name a primary that answers as one.
    pub async fn start_everything(&self) {
        for container in self.all() {
            container.start().await.unwrap();
        }
        Self::role(&self.primary, "master").await;
        tokio::time::timeout(BOUND, async {
            while !self.primary_named().await {
                tokio::time::sleep(PAUSE).await;
            }
        })
        .await
        .expect("the Sentinels must name a primary after the total loss");
    }

    /// The Sentinel at `index`.
    pub fn sentinel_at(&self, index: usize) -> &ContainerAsync<GenericImage> {
        &self.sentinels[index]
    }

    fn all(&self) -> impl Iterator<Item = &ContainerAsync<GenericImage>> {
        [&self.primary, &self.replica]
            .into_iter()
            .chain(&self.sentinels)
    }

    /// Every Sentinel names the same primary, and that primary answers
    /// as master.
    async fn primary_named(&self) -> bool {
        let mut named = Vec::new();
        for sentinel in &self.sentinels {
            named.push(Self::master_of(sentinel).await);
        }
        let primary = self
            .primary
            .get_bridge_ip_address()
            .await
            .ok()
            .map(|ip| ip.to_string());
        named.iter().all(|n| n.is_some() && *n == primary)
    }

    async fn master_of(sentinel: &ContainerAsync<GenericImage>) -> Option<String> {
        let url = Self::url(sentinel, 26379).await;
        let mut connection = redis::Client::open(url)
            .ok()?
            .get_multiplexed_async_connection()
            .await
            .ok()?;
        let addr: Vec<String> = redis::cmd("SENTINEL")
            .arg("GET-MASTER-ADDR-BY-NAME")
            .arg("kardamom")
            .query_async(&mut connection)
            .await
            .ok()?;
        addr.into_iter().next()
    }

    async fn url(container: &ContainerAsync<GenericImage>, port: u16) -> String {
        format!(
            "redis://127.0.0.1:{}",
            container.get_host_port_ipv4(port).await.unwrap()
        )
    }

    pub async fn raw(container: &ContainerAsync<GenericImage>, port: u16) -> MultiplexedConnection {
        let url = Self::url(container, port).await;
        tokio::time::timeout(
            Duration::from_secs(3),
            redis::Client::open(url)
                .unwrap()
                .get_multiplexed_async_connection(),
        )
        .await
        .unwrap()
        .unwrap()
    }

    async fn ready(&self) {
        while !self.ready_once().await {
            tokio::time::sleep(PAUSE).await;
        }
    }

    async fn ready_once(&self) -> bool {
        let mut sentinel = Self::raw(&self.sentinels[0], 26379).await;
        let others: Vec<redis::Value> = redis::cmd("SENTINEL")
            .arg("SENTINELS")
            .arg("kardamom")
            .query_async(&mut sentinel)
            .await
            .unwrap();
        let replicas: Vec<redis::Value> = redis::cmd("SENTINEL")
            .arg("REPLICAS")
            .arg("kardamom")
            .query_async(&mut sentinel)
            .await
            .unwrap();
        // A replica that still syncs can let WAIT time out on a loaded host. The
        // fixture is then not ready yet, and the bounded wait tries again.
        others.len() == 2
            && replicas.len() == 1
            && self
                .cache
                .wait_replica()
                .await
                .is_ok_and(|count| count == 1)
    }

    pub fn row(nonce: u64) -> AccountRow {
        AccountRow {
            address: Address::repeat_byte(0x42),
            nonce,
            balance: U256::from(nonce.checked_mul(100).unwrap()),
        }
    }

    pub async fn fault(&self, fault: Fault) {
        match fault {
            Fault::Freeze => {
                self.signal("STOP").await;
                self.assert_stopped().await;
            }
            Fault::Kill => self.signal("KILL").await,
        }
    }

    pub async fn restore(&self, fault: Fault) {
        match fault {
            Fault::Freeze => self.signal("CONT").await,
            Fault::Kill => self.primary.start().await.unwrap(),
        }
    }

    async fn signal(&self, signal: &str) {
        Self::send(self.primary.id(), signal).await;
    }

    /// Send `signal` to the main process of the container `id`.
    pub async fn send(id: &str, signal: &str) {
        let output = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::process::Command::new("docker")
                .args(["kill", "--signal", signal, id])
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(output.status.success(), "{output:?}");
    }

    async fn assert_stopped(&self) {
        let mut status = self
            .primary
            .exec(ExecCommand::new(["cat", "/proc/1/status"]))
            .await
            .unwrap();
        let bytes = status.stdout_to_vec().await.unwrap();
        let state = String::from_utf8(bytes).unwrap();
        assert!(state.contains("State:\tT (stopped)"), "{state}");
    }

    pub async fn role(container: &ContainerAsync<GenericImage>, wanted: &str) {
        tokio::time::timeout(BOUND, async {
            while !Self::is_role(container, wanted).await.unwrap_or(false) {
                tokio::time::sleep(PAUSE).await;
            }
        })
        .await
        .expect("Redis must reach the expected role");
    }

    async fn is_role(
        container: &ContainerAsync<GenericImage>,
        wanted: &str,
    ) -> redis::RedisResult<bool> {
        let mut connection = redis::Client::open(Self::url(container, 6379).await)?
            .get_multiplexed_async_connection()
            .await?;
        let info: String = redis::cmd("INFO")
            .arg("replication")
            .query_async(&mut connection)
            .await?;
        Ok(info.lines().any(|line| line == format!("role:{wanted}")))
    }

    pub async fn write(&self, nonce: u64, position: u64) {
        tokio::time::timeout(BOUND, async {
            while self
                .cache
                .write_rows(BPosition::from_index(position), &[Self::row(nonce)])
                .await
                .is_err()
            {
                tokio::time::sleep(PAUSE).await;
            }
        })
        .await
        .expect("the same client must follow the promoted primary");
    }

    /// Read `address` again after a short pause.
    ///
    /// # Errors
    ///
    /// Returns the cache error while the group does not answer.
    pub async fn read_again(
        &self,
        address: Address,
    ) -> Result<Option<crate::AccountView>, crate::CacheError> {
        tokio::time::sleep(PAUSE).await;
        self.cache.account(address).await
    }

    pub async fn assert_row(&self, nonce: u64, position: u64) {
        let expected = Self::row(nonce);
        let actual = self.cache.account(expected.address).await.unwrap().unwrap();
        assert_eq!(
            (actual.nonce, actual.balance, actual.tx_idx),
            (nonce, expected.balance, position)
        );
    }
}

impl Fixture {
    /// The primary fails by `fault`: the client fails, the replica is
    /// promoted, the same client writes through the promoted primary
    /// with a reloaded script, and the old primary returns as a replica.
    pub async fn recover_after(&self, fault: Fault) {
        self.fault(fault).await;
        assert!(
            self.cache.account(Fixture::row(1).address).await.is_err(),
            "the cache output must actually fail before promotion"
        );
        Fixture::role(&self.replica, "master").await;
        let mut promoted = Fixture::raw(&self.replica, 6379).await;
        redis::cmd("SCRIPT")
            .arg("FLUSH")
            .query_async::<()>(&mut promoted)
            .await
            .unwrap();
        let hash = redis::Script::new(crate::script::APPLY_ROW)
            .get_hash()
            .to_owned();
        let absent: Vec<bool> = redis::cmd("SCRIPT")
            .arg("EXISTS")
            .arg(&hash)
            .query_async(&mut promoted)
            .await
            .unwrap();
        assert_eq!(absent, [false]);
        self.write(3, 30).await;
        self.assert_row(3, 30).await;
        let loaded: Vec<bool> = redis::cmd("SCRIPT")
            .arg("EXISTS")
            .arg(&hash)
            .query_async(&mut promoted)
            .await
            .unwrap();
        assert_eq!(
            loaded,
            [true],
            "the promoted primary must receive the real script"
        );

        self.restore(fault).await;
        Fixture::role(&self.primary, "slave").await;
        self.write(2, 20).await;
        self.assert_row(3, 30).await;
    }
}
