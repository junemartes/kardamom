use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256, Bytes, LogData, U256, address};
use alloy_rpc_types_eth::Log;
use alloy_sol_types::SolEvent;
use kardamom_batcher::da::DaProxy;
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_batcher::testkit_da::FakeDaProxy;
use kardamom_da_watcher::rpc_source::DepositInitiated;
use kardamom_da_watcher::source::fakes::MockL1Source;
use kardamom_da_watcher::{L1Header, L1Source, L1SourceError, L1Sources};
use kardamom_obs::halt::HaltCause;

use super::{FollowConfig, Follower, FollowerParts, Tick};
use crate::schedule::FinalitySchedule;
use crate::sink::fakes::RecordedBlocks;
use crate::store::Store;
use crate::{IndexerError, L1Block};

const SETTLEMENT: Address = address!("00000000000000000000000000000000000000a1");
const LOCKBOX: Address = address!("00000000000000000000000000000000000000b2");

fn config(start_block: u64) -> FollowConfig {
    FollowConfig {
        settlement: SETTLEMENT,
        lockbox: LOCKBOX,
        start_block: Some(start_block),
        poll_interval: Duration::from_secs(12),
        blocks_per_tick: NonZeroU64::new(64).unwrap(),
        max_log_range: NonZeroU64::new(10).unwrap(),
        schedule: None,
    }
}

/// A follower over one scripted source, a recording sink, a fake DA
/// proxy, and a store in a temporary directory.
struct Rig<S> {
    sink: Arc<RecordedBlocks>,
    da: FakeDaProxy,
    store: Store,
    follower: Follower<S, Arc<RecordedBlocks>>,
    dir: tempfile::TempDir,
}

impl<S: L1Source> Rig<S> {
    fn new(source: S, cfg: FollowConfig) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let sink = Arc::new(RecordedBlocks::default());
        let da = FakeDaProxy::start();
        let follower = Follower::open(FollowerParts {
            source,
            sink: sink.clone(),
            da: DaProxy::new(da.url()).unwrap(),
            store: store.clone(),
            cfg,
        })
        .unwrap();
        Self {
            sink,
            da,
            store,
            follower,
            dir,
        }
    }
}

/// A log of `address` in block `number` of the mock chain.
fn log_at(number: u64, address: Address, data: LogData, index: u64) -> Log {
    Log {
        inner: alloy_primitives::Log { address, data },
        block_hash: Some(MockL1Source::filler_hash(number)),
        block_number: Some(number),
        block_timestamp: None,
        transaction_hash: Some(B256::repeat_byte(0x77)),
        transaction_index: Some(0),
        log_index: Some(index),
        removed: false,
    }
}

fn batch_posted(number: u64, index: u64, da_cert: Bytes) -> Log {
    let event = IKardamomL2Settlement::BatchPosted {
        batchIndex: index,
        daCert: da_cert,
        l2BlockStart: 1,
        l2BlockEnd: 9,
        recordsCommitment: B256::repeat_byte(0x42),
    };
    log_at(number, SETTLEMENT, event.encode_log_data(), 0)
}

fn deposit(number: u64) -> Log {
    let event = DepositInitiated {
        depositNonce: 1,
        from: address!("00000000000000000000000000000000000000cc"),
        to: address!("00000000000000000000000000000000000000dd"),
        mint: U256::from(1_000u64),
        gasLimit: 21_000,
        data: Bytes::new(),
    };
    log_at(number, LOCKBOX, event.encode_log_data(), 1)
}

/// One finality step of 32 blocks: one tip read, one header batch, and
/// four log queries of at most 10 blocks; one record per block, in order.
#[tokio::test]
async fn a_step_costs_one_tip_one_batch_and_one_query_per_ten_blocks() {
    let source = Arc::new(MockL1Source::new());
    source.push_tip(Ok(132));
    let mut rig = Rig::new(source.clone(), config(101));
    let tick = rig.follower.tick().await.unwrap();
    assert_eq!(
        tick,
        Tick::Advanced {
            to: 132,
            batches: 0,
            caught_up: true
        }
    );
    assert_eq!(source.calls(), 1 + 1 + 4);
    let numbers: Vec<u64> = rig.sink.blocks().iter().map(|b| b.number).collect();
    assert_eq!(numbers, (101..=132).collect::<Vec<_>>());
    let cursor = rig.follower.cursor().l1_block.unwrap();
    assert_eq!(
        (cursor.number, cursor.hash),
        (132, MockL1Source::filler_hash(132))
    );
    assert_eq!(rig.store.cursor().unwrap(), rig.follower.cursor());
}

/// The records carry each block's batches and its epoch; the archive holds
/// the batch, its payload, and the epoch.
#[tokio::test]
async fn a_record_carries_its_batches_and_its_deposits() {
    let source = Arc::new(MockL1Source::new());
    source.push_tip(Ok(12));
    let mut rig = Rig::new(source.clone(), config(11));
    let cert = DaProxy::new(rig.da.url())
        .unwrap()
        .put(b"payload")
        .await
        .unwrap();
    source
        .raw_logs
        .lock()
        .unwrap()
        .push_back(Ok(vec![batch_posted(11, 4, cert.clone()), deposit(12)]));
    rig.follower.tick().await.unwrap();
    let blocks = rig.sink.blocks();
    assert_eq!(blocks[0].batches.len(), 1);
    assert_eq!(blocks[0].batches[0].index, 4);
    assert_eq!(blocks[0].batches[0].da_cert, cert);
    assert!(blocks[0].epoch.deposits.is_empty());
    assert_eq!(blocks[1].epoch.deposits.len(), 1);
    assert_eq!(blocks[1].parent_hash, blocks[0].hash);
    assert_eq!(blocks[1].timestamp, 12 * 12);
    assert_eq!(rig.store.batch(4).unwrap().unwrap().l1_block, 11);
    assert_eq!(rig.store.payload(&cert).unwrap().unwrap(), b"payload");
    assert_eq!(rig.follower.cursor().last_batch, Some(4));
}

/// The epoch store and the stream carry the same bytes: an epoch read
/// from the store is byte-equal to the epoch of the record on the stream,
/// and the stored record is the published one.
#[tokio::test]
async fn the_epoch_store_and_the_stream_carry_the_same_bytes() {
    let source = Arc::new(MockL1Source::new());
    source.push_tip(Ok(21));
    let mut rig = Rig::new(source.clone(), config(20));
    source
        .raw_logs
        .lock()
        .unwrap()
        .push_back(Ok(vec![deposit(21)]));
    rig.follower.tick().await.unwrap();
    rig.sink.blocks().iter().for_each(|published| {
        let wire = rkyv::to_bytes::<rkyv::rancor::Error>(published).unwrap();
        let on_stream: L1Block = rkyv::from_bytes::<L1Block, rkyv::rancor::Error>(&wire).unwrap();
        let epoch_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&on_stream.epoch).unwrap();
        let stored = rig.store.epoch_bytes(published.number).unwrap().unwrap();
        assert_eq!(stored, epoch_bytes.as_slice());
        let block = rig.store.block_bytes(published.number).unwrap().unwrap();
        assert_eq!(block, wire.as_slice());
    });
    assert_eq!(rig.sink.blocks()[1].epoch.deposits.len(), 1);
}

/// A block that does not descend from the cursor's block is a chain
/// break; the cursor stays, and nothing is published.
#[tokio::test]
async fn a_block_that_does_not_descend_from_the_cursor_halts() {
    let source = Arc::new(MockL1Source::new());
    source.push_tip(Ok(5));
    let mut rig = Rig::new(source.clone(), config(4));
    rig.follower.tick().await.unwrap();
    source.push_tip(Ok(7));
    source.parent_lies.lock().unwrap().insert(6);
    let error = rig.follower.tick().await.unwrap_err();
    assert!(
        matches!(error, IndexerError::ChainBreak { number: 6, .. }),
        "{error}"
    );
    assert_eq!(error.halt().unwrap().cause, HaltCause::L1ChainBreak);
    assert_eq!(rig.follower.cursor().l1_block.unwrap().number, 5);
    assert_eq!(rig.sink.blocks().len(), 2);
}

/// A step whose last header is not the light client's is the operator
/// halt `l1_light_client_mismatch`, and nothing of the step is published.
#[tokio::test]
async fn a_step_that_does_not_end_at_the_light_client_header_halts() {
    let a = Scripted::batching();
    let b = Scripted::batching();
    let light_client = Scripted::batchless();
    a.0.push_tip(Ok(40));
    b.0.push_tip(Ok(40));
    light_client.0.push_tip(Ok(40));
    light_client
        .0
        .hashes
        .lock()
        .unwrap()
        .insert(40, B256::repeat_byte(0xEE));
    // The light client fails the batch, so the two public sources settle
    // the headers; only the anchor sees its header.
    let set = L1Sources::new(vec![("a".into(), a), ("b".into(), b)])
        .with_light_client("light client".into(), light_client);
    let mut rig = Rig::new(set, config(33));
    let error = rig.follower.tick().await.unwrap_err();
    assert!(
        matches!(error, IndexerError::LightClientMismatch { number: 40, .. }),
        "{error}"
    );
    let halt = error.halt().unwrap();
    assert_eq!(halt.cause, HaltCause::L1LightClientMismatch);
    assert_eq!(halt.clears, kardamom_obs::halt::Clears::Operator);
    assert!(rig.sink.blocks().is_empty());
    assert_eq!(rig.follower.cursor().l1_block, None);
}

/// A record the stream does not take holds the cursor: the next tick
/// reads the range again and publishes it whole.
#[tokio::test]
async fn a_refused_publish_holds_the_cursor() {
    let source = Arc::new(MockL1Source::new());
    source.push_tip(Ok(3));
    let mut rig = Rig::new(source.clone(), config(1));
    *rig.sink.refuse.lock().unwrap() = true;
    let error = rig.follower.tick().await.unwrap_err();
    assert!(matches!(error, IndexerError::Publish(_)), "{error}");
    assert_eq!(rig.follower.cursor().l1_block, None);
    *rig.sink.refuse.lock().unwrap() = false;
    source.push_tip(Ok(3));
    rig.follower.tick().await.unwrap();
    let numbers: Vec<u64> = rig.sink.blocks().iter().map(|b| b.number).collect();
    assert_eq!(numbers, [1, 2, 3]);
}

/// The poll plan: one slot without a schedule; with one, at once behind
/// the tip, until the next boundary at the tip, and one slot at a time
/// once the awaited boundary passed.
#[test]
fn the_poll_plan_follows_the_finality_schedule() {
    let slot = Duration::from_secs(12);
    let mut fixed = Rig::new(MockL1Source::new(), config(1));
    let caught_up = Tick::Advanced {
        to: 9,
        batches: 0,
        caught_up: true,
    };
    assert_eq!(fixed.follower.plan(&caught_up, 1_000), slot);
    assert_eq!(fixed.follower.plan(&Tick::Idle, 1_000), slot);

    let mut cfg = config(1);
    cfg.schedule = Some(FinalitySchedule {
        genesis_time: 1_000,
        seconds_per_slot: NonZeroU64::new(12).unwrap(),
        slots_per_epoch: NonZeroU64::new(32).unwrap(),
    });
    let mut aligned = Rig::new(MockL1Source::new(), cfg);
    let behind = Tick::Advanced {
        to: 9,
        batches: 0,
        caught_up: false,
    };
    assert_eq!(aligned.follower.plan(&behind, 1_100), Duration::ZERO);
    assert_eq!(aligned.follower.plan(&Tick::Idle, 1_100), slot);
    assert_eq!(
        aligned.follower.plan(&caught_up, 1_100),
        Duration::from_secs(284)
    );
    assert_eq!(
        aligned.follower.plan(&Tick::Idle, 1_200),
        Duration::from_secs(184)
    );
    assert_eq!(aligned.follower.plan(&Tick::Idle, 1_384), slot);
    assert_eq!(aligned.follower.plan(&Tick::Idle, 1_400), slot);
}

/// A scripted source that serves header batches or not: a light client
/// behind an execution RPC without batch support serves single reads
/// only.
struct Scripted(MockL1Source, bool);

impl Scripted {
    fn batching() -> Self {
        Self(MockL1Source::new(), true)
    }

    fn batchless() -> Self {
        Self(MockL1Source::new(), false)
    }
}

#[async_trait::async_trait]
impl L1Source for Scripted {
    async fn finalized_block_number(&self) -> Result<u64, L1SourceError> {
        self.0.finalized_block_number().await
    }

    async fn block_ids(&self, number: u64) -> Result<(B256, B256), L1SourceError> {
        self.0.block_ids(number).await
    }

    async fn headers(&self, from: u64, to: u64) -> Result<Vec<L1Header>, L1SourceError> {
        if !self.1 {
            return Err(L1SourceError::Provider("no batch".into()));
        }
        self.0.headers(from, to).await
    }

    async fn logs(&self, filter: &alloy_rpc_types_eth::Filter) -> Result<Vec<Log>, L1SourceError> {
        self.0.logs(filter).await
    }
}

#[tokio::test]
async fn a_failed_cursor_write_replays_the_range() {
    let source = Arc::new(MockL1Source::new());
    source.push_tip(Ok(3));
    let mut rig = Rig::new(source.clone(), config(1));
    let tmp = rig.dir.path().join("cursor.tmp");
    std::fs::create_dir(&tmp).unwrap();
    assert!(rig.follower.tick().await.is_err());
    assert_eq!(rig.follower.cursor().l1_block, None);
    std::fs::remove_dir(&tmp).unwrap();
    source.push_tip(Ok(3));
    rig.follower.tick().await.unwrap();
    let numbers: Vec<u64> = rig.sink.blocks().iter().map(|b| b.number).collect();
    assert_eq!(numbers, [1, 2, 3, 1, 2, 3]);
    assert_eq!(rig.store.cursor().unwrap(), rig.follower.cursor());
}
