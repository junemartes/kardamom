//! The real sender and startup path while L1 hides mined receipts and logs.

use alloy_node_bindings::Anvil;
use alloy_primitives::address;
use alloy_rpc_types_eth::Filter;
use kardamom_deployer::Deployer;
use kardamom_deployer::testkit::{AnvilRig, Funding};
use kardamom_l1_fault_proxy::{Fault, FaultProxy};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::*;
use crate::testkit_da::FakeDaProxy;

const OWNER: Address = address!("00000000000000000000000000000000DEAD0001");

struct Post(JoinHandle<Result<()>>);

impl Post {
    async fn finish(mut self) {
        tokio::time::timeout(Duration::from_secs(10), &mut self.0)
            .await
            .expect("post must finish after canonical confirmation")
            .unwrap()
            .unwrap();
    }

    async fn cancel(mut self) {
        self.0.abort();
        assert!((&mut self.0).await.as_ref().unwrap_err().is_cancelled());
    }
}

impl Drop for Post {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Fixture {
    proxy: FaultProxy,
    rig: AnvilRig,
    da: FakeDaProxy,
    disk: tempfile::TempDir,
    indexer: TcpListener,
    key: String,
    settlement: Address,
    batcher: Address,
}

impl Fixture {
    async fn new() -> Self {
        let rig = AnvilRig::spawn(Anvil::new(), &[(OWNER, Funding::FundAndImpersonate)])
            .await
            .expect("this component test requires a working Anvil; it must not skip");
        let signer = PrivateKeySigner::from(rig.anvil.keys()[2].clone());
        let batcher = signer.address();
        let key = alloy_primitives::hex::encode(signer.to_bytes());
        let settlement = Deployer::new(rig.provider.clone(), OWNER)
            .deploy_settlement(OWNER, 42, batcher)
            .await;
        let proxy = FaultProxy::spawn(&rig.anvil.endpoint(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        Self {
            proxy,
            rig,
            da: FakeDaProxy::start(),
            disk: tempfile::tempdir().unwrap(),
            indexer: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            key,
            settlement,
            batcher,
        }
    }

    fn args(&self) -> LiveArgs {
        LiveArgs {
            rpcs: vec![self.proxy.url()],
            key: self.key.clone(),
            settlement: self.settlement,
            da_proxy: self.da.url(),
            config: self.disk.path().join("unused.toml"),
            cursor_file: self.disk.path().join("cursor.json"),
            spool_dir: self.disk.path().join("spool"),
            log_config: None,
            aeron_dir: None,
            chain_id: 42,
            cluster_egress_endpoint: None,
            void_voter_id: None,
            egress: None,
            replay_destination_endpoint: None,
            archive_control_response_endpoint: None,
            blocks_per_batch: NonZeroUsize::new(1).unwrap(),
            compress: false,
            flush_ms: NonZeroU64::new(1).unwrap(),
            idle_flush_ms: NonZeroU64::new(1).unwrap(),
            target_payload_bytes: NonZeroUsize::new(1024).unwrap(),
            l1_retries: 0,
            // This listener never answers. A startup that waits for it cannot pass.
            indexer_url: Some(format!("http://{}", self.indexer.local_addr().unwrap())),
            l1_silence: Duration::from_secs(1),
            da_lag_budget_blocks: 10,
            block_refs_sources: Vec::new(),
        }
    }

    fn cursor(block: u64, index: u64) -> BatchCursor {
        BatchCursor {
            next_index: block.checked_mul(10).unwrap(),
            next_block: block.checked_add(1).unwrap(),
            last_batch_index: index,
        }
    }

    async fn start(&self, blocks: std::ops::RangeInclusive<u64>) -> Post {
        let args = self.args();
        let resumed = args
            .start_l1_side()
            .await
            .unwrap_or_else(|_| panic!("start the feed"));
        self.feed(resumed, blocks)
    }

    async fn automine(&self, enabled: bool) {
        let _: () = self
            .rig
            .provider
            .raw_request("anvil_setAutomine".into(), (enabled,))
            .await
            .unwrap();
    }

    async fn mine(&self) {
        let _: serde_json::Value = self
            .rig
            .provider
            .raw_request("evm_mine".into(), ())
            .await
            .unwrap();
    }

    async fn await_mempool(&self, nonce: u64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self
                .rig
                .provider
                .get_transaction_count(self.batcher)
                .pending()
                .await
                .unwrap()
                != nonce
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the signed post must reach Anvil's mempool");
    }

    fn replay(
        blocks: std::ops::RangeInclusive<u64>,
    ) -> tokio::sync::mpsc::Receiver<kardamom_engine::reader::ReaderToExec> {
        let (send, receive) = tokio::sync::mpsc::channel(2);
        blocks.for_each(|block| {
            send.try_send(kardamom_engine::reader::ReaderToExec::Boundary(
                kardamom_types::BlockBoundaryStart {
                    block_number: block,
                    end_tx_idx: kardamom_types::BPosition::from_index(
                        block.checked_mul(10).unwrap(),
                    ),
                    l2_timestamp: block.checked_add(1_000).unwrap(),
                    l1_origin: 0,
                },
            ))
            .unwrap();
        });
        receive
    }

    fn feed<P: Provider + 'static>(
        &self,
        resumed: L1Side<P>,
        blocks: std::ops::RangeInclusive<u64>,
    ) -> Post {
        resumed
            .provider
            .client()
            .set_poll_interval(Duration::from_millis(10));
        let args = self.args();
        let spool = Spool::open(&args.spool_dir).unwrap();
        let (restored, resume) =
            continue_from_spool(&spool, resumed.cursor, resumed.skip_through_block).unwrap();
        let mut feed = resumed.feed(&args, spool, restored, resume);
        Post(tokio::spawn(async move {
            let error = feed.run(Self::replay(blocks)).await;
            anyhow::ensure!(
                error.to_string().contains("reader channel closed"),
                "{error:#}"
            );
            Ok(())
        }))
    }

    /// Start the batcher under `lies`, as a restart under them does, and
    /// post `blocks` through them: each post confirms one block, and the
    /// durable cursor follows the last.
    async fn post_through(&self, lies: &[Fault], blocks: std::ops::RangeInclusive<u64>) {
        self.proxy
            .set_faults(serde_json::from_value(serde_json::json!(lies)).unwrap());
        let last = *blocks.end();
        self.start(blocks).await.finish().await;
        self.assert_cursor(Self::cursor(last, last));
    }

    fn hide_receipts_and_logs(&self) {
        self.proxy.set_faults(
            serde_json::from_value(serde_json::json!([
                Fault::NullReceipts { from_block: 0 },
                Fault::SwallowLogs {
                    address: self.settlement
                }
            ]))
            .unwrap(),
        );
    }

    async fn await_landed(&self, index: u64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while L1Truth::read(&self.rig.provider, self.settlement)
                .await
                .unwrap()
                .last_batch_index
                != index
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the real transaction must land despite the hidden receipt");
    }

    async fn prove_hidden(&self, index: u64) {
        self.await_landed(index).await;
        let filter = Filter::new().address(self.settlement).from_block(0);
        let logs = self.rig.provider.get_logs(&filter).await.unwrap();
        let hash = logs.last().unwrap().transaction_hash.unwrap();
        let provider = connect_l1(&[self.proxy.url()], &self.key).unwrap();
        assert!(
            provider
                .get_transaction_receipt(hash)
                .await
                .unwrap()
                .is_none()
        );
        assert!(provider.get_logs(&filter).await.unwrap().is_empty());
        assert!(
            self.rig
                .provider
                .get_transaction_receipt(hash)
                .await
                .unwrap()
                .is_some()
        );
    }

    fn assert_cursor(&self, expected: BatchCursor) {
        assert_eq!(
            BatchCursor::load(&self.args().cursor_file).unwrap(),
            Some(expected)
        );
    }

    fn assert_spooled(&self, expected: &[u64]) {
        let restored = Spool::open(&self.args().spool_dir).unwrap().load().unwrap();
        assert_eq!(
            restored
                .blocks
                .iter()
                .map(|block| block.block_number)
                .collect::<Vec<_>>(),
            expected
        );
    }

    async fn assert_history(&self, count: u64) {
        let records = crate::l1::read_posted_batches(&self.rig.provider, self.settlement, 0)
            .await
            .unwrap();
        assert_eq!(u64::try_from(records.len()).unwrap(), count);
        records.iter().zip(1..=count).for_each(|(record, index)| {
            assert_eq!(
                (record.index, record.l2_block_start, record.l2_block_end),
                (index, index, index)
            );
            let stored = self.da.stored();
            let frames = crate::recon::reconstruct(stored.get(&record.da_cert).unwrap()).unwrap();
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].block_number, index);
            assert_eq!(
                frames[0].cursor.unwrap().end_tx_idx,
                index.checked_mul(10).unwrap()
            );
        });
        assert_eq!(
            self.rig
                .provider
                .get_transaction_count(self.batcher)
                .await
                .unwrap(),
            count
        );
    }
}

#[tokio::test]
#[ignore = "requires Anvil and the compiled settlement contract"]
async fn hidden_mined_receipts_reconcile_without_duplicate_posts_or_early_cursor() {
    let fixture = Fixture::new().await;
    fixture.start(1..=1).await.finish().await;
    fixture.hide_receipts_and_logs();
    fixture.automine(false).await;
    let pending = fixture.start(2..=2).await;
    fixture.await_mempool(2).await;
    fixture.assert_spooled(&[2]);
    assert!(!pending.0.is_finished());
    fixture.assert_cursor(Fixture::cursor(1, 1));
    fixture.mine().await;
    fixture.prove_hidden(2).await;
    pending.finish().await;
    fixture.assert_cursor(Fixture::cursor(2, 2));
    fixture.proxy.set_fault(Fault::None);
    fixture.automine(true).await;
    fixture.start(3..=3).await.finish().await;
    fixture.assert_cursor(Fixture::cursor(3, 3));
    fixture.assert_history(3).await;
    fixture.assert_spooled(&[]);
}

#[tokio::test]
#[ignore = "requires Anvil and the compiled settlement contract"]
async fn restart_during_hidden_receipts_uses_contract_truth_without_waiting_for_the_indexer() {
    let fixture = Fixture::new().await;
    fixture.start(1..=1).await.finish().await;
    fixture.hide_receipts_and_logs();
    fixture.automine(false).await;
    let pending = fixture.start(2..=2).await;
    fixture.await_mempool(2).await;
    fixture.assert_spooled(&[2]);
    assert!(!pending.0.is_finished());
    fixture.assert_cursor(Fixture::cursor(1, 1));
    pending.cancel().await;
    fixture.mine().await;
    fixture.prove_hidden(2).await;
    fixture.assert_cursor(Fixture::cursor(1, 1));
    let args = fixture.args();
    let resumed = tokio::time::timeout(Duration::from_secs(5), args.start_l1_side())
        .await
        .expect("startup must not wait for the unavailable indexer")
        .unwrap_or_else(|_| panic!("startup must use the contract and durable cursor"));
    assert_eq!(resumed.cursor, Fixture::cursor(1, 1));
    assert_eq!(resumed.skip_through_block, 2);
    assert_eq!(resumed.l1_truth.last_batch_index, 2);
    fixture.proxy.set_fault(Fault::None);
    fixture.automine(true).await;
    fixture.feed(resumed, 2..=3).finish().await;
    fixture.assert_cursor(Fixture::cursor(3, 3));
    fixture.assert_history(3).await;
    fixture.assert_spooled(&[]);
}

#[tokio::test]
#[ignore = "requires Anvil and the compiled settlement contract"]
async fn every_lie_lets_a_restarted_batcher_post_and_confirm_three_batches() {
    let fixture = Fixture::new().await;
    fixture.start(1..=1).await.finish().await;
    let settlement = fixture.settlement;
    let lies = [
        vec![Fault::WrongBlockHash { from_block: 0 }],
        vec![Fault::BrokenParentChain { from_block: 0 }],
        vec![Fault::SwallowLogs {
            address: settlement,
        }],
        vec![
            Fault::WrongBlockHash { from_block: 0 },
            Fault::SwallowLogs {
                address: settlement,
            },
        ],
    ];
    // The reader channel holds two blocks, so three posts are two starts.
    for (first, lie) in (2_u64..).step_by(3).zip(lies) {
        fixture.post_through(&lie, first..=first + 1).await;
        fixture.post_through(&lie, first + 2..=first + 2).await;
    }
    fixture.proxy.set_fault(Fault::None);
    fixture.assert_history(13).await;
    fixture.assert_spooled(&[]);
}

#[tokio::test]
#[ignore = "requires Anvil and the compiled settlement contract"]
async fn a_refusing_provider_fail_stops_the_post_and_a_restart_posts_it_once() {
    let fixture = Fixture::new().await;
    fixture.start(1..=1).await.finish().await;
    for (block, refusal) in [(2, Fault::Down), (3, Fault::RateLimit)] {
        fixture.post_after_refusal(refusal, block).await;
    }
    fixture.assert_history(3).await;
    fixture.assert_spooled(&[]);
}

impl Fixture {
    /// Start the batcher, then refuse every call while it posts `block`.
    /// The bounded retries end in a fail-stop: no batch lands, the cursor
    /// stays and the spool keeps the block. Once the provider answers, the
    /// restart posts the block exactly once.
    async fn post_after_refusal(&self, refusal: Fault, block: u64) {
        let resumed = self
            .args()
            .start_l1_side()
            .await
            .unwrap_or_else(|_| panic!("start"));
        self.proxy.set_fault(refusal);
        self.feed(resumed, block..=block).fail_stop().await;
        let truth = L1Truth::read(&self.rig.provider, self.settlement)
            .await
            .unwrap();
        assert_eq!(
            truth.last_batch_index,
            block - 1,
            "{refusal:?}: no post lands"
        );
        self.assert_cursor(Self::cursor(block - 1, block - 1));
        self.assert_spooled(&[block]);
        self.proxy.set_fault(Fault::None);
        self.start(block..=block).await.finish().await;
        self.assert_cursor(Self::cursor(block, block));
    }
}

impl Post {
    /// The run ends with the error of a post that failed every attempt.
    async fn fail_stop(mut self) {
        let error = tokio::time::timeout(Duration::from_secs(10), &mut self.0)
            .await
            .expect("the bounded retries must end the run")
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("post batch"), "{error:#}");
    }
}
