use std::time::{Duration, SystemTime};

use alloy_primitives::{Address, U256};
use kardamom_cache::{AccountCache, CacheConfig};
use kardamom_types::{AccountRow, BPosition};
use redis::aio::MultiplexedConnection;
use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const BOUND: Duration = Duration::from_secs(30);
const PAUSE: Duration = Duration::from_millis(50);

#[derive(Clone, Copy)]
pub(super) enum Fault {
    Freeze,
    Kill,
}

pub(super) struct Fixture {
    pub(super) cache: AccountCache,
    /// The configuration the cache client connected with.
    #[allow(
        dead_code,
        reason = "the state-mirror total-loss test connects its mirror with it"
    )]
    pub(super) cfg: CacheConfig,
    pub(super) primary: ContainerAsync<GenericImage>,
    pub(super) replica: ContainerAsync<GenericImage>,
    sentinels: [ContainerAsync<GenericImage>; 3],
}

impl Fixture {
    pub(super) async fn new() -> Self {
        let token = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let network = format!("component-sentinel-{}-{token}", std::process::id());
        let primary = Self::redis(&network, &[]).await;
        let primary_ip = primary.get_bridge_ip_address().await.unwrap().to_string();
        let replica = Self::redis(&network, &["--replicaof", &primary_ip, "6379"]).await;
        let (a, b, c) = tokio::join!(
            Self::sentinel(&network, &primary_ip),
            Self::sentinel(&network, &primary_ip),
            Self::sentinel(&network, &primary_ip)
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

    async fn redis(network: &str, extra: &[&str]) -> ContainerAsync<GenericImage> {
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
            .with_mapped_port(Self::free_port(), 6379_u16.tcp())
            .with_cmd(command)
            .start()
            .await
            .unwrap()
    }

    async fn sentinel(network: &str, primary: &str) -> ContainerAsync<GenericImage> {
        let script = format!(
            "cat > /tmp/sentinel.conf <<'CONF'\nport 26379\nprotected-mode no\nsentinel monitor kardamom {primary} 6379 2\nsentinel down-after-milliseconds kardamom 500\nsentinel failover-timeout kardamom 5000\nCONF\nexec redis-server /tmp/sentinel.conf --sentinel"
        );
        GenericImage::new("redis", "7-alpine")
            .with_entrypoint("sh")
            .with_exposed_port(26379_u16.tcp())
            .with_wait_for(WaitFor::message_on_stdout("+monitor master"))
            .with_network(network)
            .with_mapped_port(Self::free_port(), 26379_u16.tcp())
            .with_cmd(["-c", &script])
            .start()
            .await
            .unwrap()
    }

    /// A fixed host port for one container port. A restarted container
    /// keeps it, so the same client reaches the group after a total loss,
    /// as a deployment reaches it at a stable address.
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    /// Kill the primary, the replica and every Sentinel at once. No Redis
    /// process keeps data on disk, so the group loses every key.
    pub(super) async fn lose_everything(&self) {
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
    pub(super) async fn start_everything(&self) {
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

    pub(super) async fn raw(
        container: &ContainerAsync<GenericImage>,
        port: u16,
    ) -> MultiplexedConnection {
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

    pub(super) fn row(nonce: u64) -> AccountRow {
        AccountRow {
            address: Address::repeat_byte(0x42),
            nonce,
            balance: U256::from(nonce.checked_mul(100).unwrap()),
        }
    }

    pub(super) async fn fault(&self, fault: Fault) {
        match fault {
            Fault::Freeze => {
                self.signal("STOP").await;
                self.assert_stopped().await;
            }
            Fault::Kill => self.signal("KILL").await,
        }
    }

    pub(super) async fn restore(&self, fault: Fault) {
        match fault {
            Fault::Freeze => self.signal("CONT").await,
            Fault::Kill => self.primary.start().await.unwrap(),
        }
    }

    async fn signal(&self, signal: &str) {
        let output = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::process::Command::new("docker")
                .args(["kill", "--signal", signal, self.primary.id()])
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

    pub(super) async fn role(container: &ContainerAsync<GenericImage>, wanted: &str) {
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

    pub(super) async fn write(&self, nonce: u64, position: u64) {
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
    pub(super) async fn read_again(
        &self,
        address: Address,
    ) -> Result<Option<kardamom_cache::AccountView>, kardamom_cache::CacheError> {
        tokio::time::sleep(PAUSE).await;
        self.cache.account(address).await
    }

    pub(super) async fn assert_row(&self, nonce: u64, position: u64) {
        let expected = Self::row(nonce);
        let actual = self.cache.account(expected.address).await.unwrap().unwrap();
        assert_eq!(
            (actual.nonce, actual.balance, actual.tx_idx),
            (nonce, expected.balance, position)
        );
    }
}
