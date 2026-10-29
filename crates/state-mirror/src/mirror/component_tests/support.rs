use std::num::NonZeroU64;
use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use kardamom_cache::{AccountCache, CacheConfig, keys};
use kardamom_log::aeron_live::{RawFrame, TxReceiptsReceiver};
use kardamom_state::{StateEnvBuilder, StateWriter, WriteBatch};
use kardamom_types::{
    AccountChange, AccountRow, BPosition, BlockBoundary, BlockDelta, Receipt, ReceiptBatch,
};
use redis::aio::MultiplexedConnection;
use testcontainers::{ContainerAsync, GenericImage};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::super::{Mirror, MirrorInputs};
use crate::head::HeadFile;
use crate::rebuild::Rebuild;

pub(super) const ACCOUNTS: u64 = 1_025;
const BOUND: Duration = Duration::from_secs(5);

pub(super) struct Fixture {
    pub(super) cache: AccountCache,
    pub(super) admin: MultiplexedConnection,
    cfg: CacheConfig,
    root: tempfile::TempDir,
    _redis: ContainerAsync<GenericImage>,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        let (redis, mut cfg) = kardamom_cache::testing::redis().await;
        cfg.head_ttl_secs = NonZeroU64::new(60).unwrap();
        cfg.receipt_ttl_secs = NonZeroU64::new(60).unwrap();
        let admin = redis::Client::open(cfg.url.as_deref().unwrap())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let cache = AccountCache::connect(&cfg).await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let fixture = Self {
            cache,
            admin,
            cfg,
            root,
            _redis: redis,
        };
        fixture.checkpoint();
        fixture
    }

    pub(super) async fn interrupted_scan() -> Self {
        Self::interrupted_scan_from(None).await
    }

    pub(super) async fn interrupted_scan_from(persisted: Option<u64>) -> Self {
        let mut fixture = Self::new().await;
        let head = fixture.head_file(0);
        if let Some(position) = persisted {
            head.write(position).unwrap();
        }
        fixture.permit_prefix(512).await;
        let first = fixture.start(0, true).await;
        first.send(&Self::batch(0, 1, 20));
        fixture.denied().await;
        fixture.assert_row(511, 1, 20).await;
        assert!(
            fixture
                .cache
                .account(Self::row(512, 1).address)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(fixture.cache.heads(&[0]).await.unwrap(), [None]);
        assert!(
            first
                .stop()
                .await
                .unwrap_err()
                .to_string()
                .contains("shutdown")
        );
        assert!(head.rebuild_pending().unwrap());
        assert_eq!(head.read(), persisted);

        fixture.permit_all().await;
        fixture
            .cache
            .set_head(1, BPosition::from_index(30))
            .await
            .unwrap();
        let live = fixture.start(1, false).await;
        let newest = Self::batch(0, 4, 30);
        live.send(&newest);
        // Observing the receipt proves the second actor writes before the older scan restarts.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while fixture
                .cache
                .receipt(newest.receipts[0].from, 4)
                .await
                .unwrap()
                .is_none()
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        live.close().await.unwrap();
        fixture
    }

    pub(super) fn head_file(&self, id: u32) -> HeadFile {
        HeadFile::new(self.root.path().join(format!("mirror-{id}/head")))
    }

    fn checkpoint(&self) {
        let env = StateEnvBuilder::new(self.root.path().join("state"))
            .open()
            .unwrap();
        let accounts = (0..ACCOUNTS)
            .map(|index| {
                let row = Self::row(index, 1);
                AccountChange {
                    address: row.address,
                    nonce: row.nonce,
                    balance: row.balance,
                    code_hash: B256::ZERO,
                }
            })
            .collect();
        kardamom_state::genesis::seed_genesis(
            &env,
            &kardamom_types::GenesisAlloc::of_accounts(accounts),
        )
        .unwrap();
        let mut writer = StateWriter::spawn(env.clone()).unwrap();
        writer
            .delta_tx
            .send(WriteBatch::new(
                BlockBoundary {
                    block_number: 1,
                    end_tx_idx: BPosition::from_index(21),
                    l2_timestamp: 1,
                    l1_origin: 0,
                    base_fee: 0,
                    gas_used: 0,
                },
                BlockDelta {
                    block_number: 1,
                    ..BlockDelta::default()
                },
            ))
            .unwrap();
        let deadline = std::time::Instant::now() + BOUND;
        while writer
            .snapshot_rx
            .recv_deadline(deadline)
            .expect("checkpoint commit")
            .block_number()
            < 1
        {}
        writer.shutdown().unwrap();
        kardamom_state::checkpoint::create_checkpoint(&env, &self.root.path().join("checkpoints"))
            .unwrap();
    }

    pub(super) fn row(index: u64, nonce: u64) -> AccountRow {
        let mut bytes = [0; 20];
        bytes[12..].copy_from_slice(&index.to_be_bytes());
        AccountRow {
            address: Address::from(bytes),
            nonce,
            balance: U256::from(nonce.checked_mul(100).unwrap()),
        }
    }

    pub(super) fn batch(index: u64, nonce: u64, position: u64) -> ReceiptBatch {
        let row = Self::row(index, nonce);
        ReceiptBatch {
            receipts: vec![Receipt {
                tx_hash: B256::from(U256::from(position)),
                from: row.address,
                nonce,
                tx_idx: BPosition::from_index(position),
                status: true,
                ..Receipt::default()
            }],
            accounts: vec![row],
        }
    }

    pub(super) async fn permit_prefix(&mut self, count: u64) {
        let patterns: Vec<String> = (0..count)
            .map(|index| format!("~{}", keys::account(Self::row(index, 1).address)))
            .collect();
        redis::cmd("ACL")
            .arg("SETUSER")
            .arg("mirror")
            .arg("on")
            .arg("resetpass")
            .arg(">component-test")
            .arg("+@all")
            .arg("resetkeys")
            .arg("~head:*")
            .arg("~rcpt:*")
            .arg(patterns)
            .query_async::<()>(&mut self.admin)
            .await
            .unwrap();
        redis::cmd("ACL")
            .arg("LOG")
            .arg("RESET")
            .query_async::<()>(&mut self.admin)
            .await
            .unwrap();
    }

    pub(super) async fn permit_all(&mut self) {
        redis::cmd("ACL")
            .arg("SETUSER")
            .arg("mirror")
            .arg("on")
            .arg("resetpass")
            .arg(">component-test")
            .arg("+@all")
            .arg("~*")
            .query_async::<()>(&mut self.admin)
            .await
            .unwrap();
    }

    pub(super) async fn denied(&mut self) {
        tokio::time::timeout(BOUND, self.wait_for_denial())
            .await
            .expect("the Redis fault must be exercised");
    }

    async fn wait_for_denial(&mut self) {
        while self.denials().await == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn denials(&mut self) -> usize {
        let entries: Vec<redis::Value> = redis::cmd("ACL")
            .arg("LOG")
            .query_async(&mut self.admin)
            .await
            .unwrap();
        entries.len()
    }

    pub(super) async fn start(&self, id: u32, force: bool) -> Actor {
        let mut cfg = self.cfg.clone();
        cfg.username = Some("mirror".into());
        cfg.url = cfg
            .url
            .map(|url| url.replacen("redis://", "redis://mirror:component-test@", 1));
        let cache = AccountCache::connect(&cfg).await.unwrap();
        let (input, rx) = unbounded_channel();
        let shutdown = CancellationToken::new();
        let mirror = Mirror::new(MirrorInputs {
            cache,
            receipts: TxReceiptsReceiver::from_test_frames(rx),
            id,
            head_count: 2,
            head: self.head_file(id),
            rebuild: Rebuild::new(
                self.root.path().join("checkpoints"),
                self.root.path().join(format!("scratch-{id}")),
            ),
            force_rebuild: force,
            audit_hours: 0,
            shutdown: shutdown.clone(),
        });
        Actor {
            input: Some(input),
            shutdown,
            task: Some(tokio::spawn(mirror.run())),
        }
    }

    pub(super) async fn head(&self, id: u32, position: u64) {
        tokio::time::timeout(BOUND, self.wait_for_head(id, position))
            .await
            .expect("mirror head must advance");
    }

    async fn wait_for_head(&self, id: u32, position: u64) {
        while !self.observed_head(id, position).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn observed_head(&self, id: u32, position: u64) -> bool {
        match self.cache.heads(&[id]).await {
            Ok(heads) => heads == [Some(position)],
            Err(error) => {
                eprintln!("head observation failed within the recovery budget: {error}");
                false
            }
        }
    }

    pub(super) async fn assert_row(&self, index: u64, nonce: u64, position: u64) {
        let expected = Self::row(index, nonce);
        let actual = self
            .cache
            .account(expected.address)
            .await
            .unwrap()
            .expect("covered account");
        assert_eq!(
            (actual.nonce, actual.balance, actual.tx_idx),
            (expected.nonce, expected.balance, position),
            "account {index}"
        );
    }

    pub(super) async fn assert_checkpoint_suffix(&self, from: u64) {
        for index in from..ACCOUNTS {
            self.assert_row(index, 1, 20).await;
        }
    }
}

pub(super) struct Actor {
    input: Option<UnboundedSender<RawFrame>>,
    shutdown: CancellationToken,
    task: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Actor {
    pub(super) fn send(&self, batch: &ReceiptBatch) {
        self.input
            .as_ref()
            .unwrap()
            .send(RawFrame {
                bytes: kardamom_log::codec::encode(batch).unwrap().to_vec(),
                pos: batch.end_tx_idx().unwrap(),
                session: 7,
                stream_pos: 1,
            })
            .unwrap();
    }

    pub(super) async fn stop(mut self) -> anyhow::Result<()> {
        self.shutdown.cancel();
        self.join().await
    }

    pub(super) async fn close(mut self) -> anyhow::Result<()> {
        self.input = None;
        self.join().await
    }

    async fn join(&mut self) -> anyhow::Result<()> {
        tokio::time::timeout(BOUND, self.task.as_mut().unwrap())
            .await
            .expect("bounded mirror shutdown")
            .unwrap()
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
