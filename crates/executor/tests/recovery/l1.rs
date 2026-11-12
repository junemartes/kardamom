//! executor-fleet-total-wipe-recover through the production L1 scan.
//! A real Anvil holds the deployed settlement contract, the production
//! DA store serves the payloads over HTTP, and the production batch
//! post records each batch on L1. The rebuild reads only L1 and the DA
//! store: the `BatchPosted` log scan, the payload fetch by certificate,
//! the decode, the replay and the executor-image strip.

use std::path::{Path, PathBuf};

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_batcher::da::DaProxy;
use kardamom_batcher::da_store::DaStore;
use kardamom_batcher::l1::{post_batch, read_posted_batches, recover_blocks};
use kardamom_deployer::Deployer;
use kardamom_deployer::testkit::{AnvilRig, Funding};
use kardamom_engine::ReplayGenesis;
use kardamom_reconstruct::{Reconstruction, block_frame_to_replay};

use super::fleet::{NodeHandle, World};
use super::verdict::CATCH_UP;
use super::{BLOCK_EVERY, FETCHED, RESTORED, RESUMED};

const OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
/// The L2 blocks in each posted batch.
const BLOCKS_PER_BATCH: u64 = 4;

/// The L1 side: Anvil with the settlement contract, and the DA store.
struct L1 {
    rig: AnvilRig,
    settlement: Address,
    batcher: PrivateKeySigner,
    da: DaProxy,
    _store: tempfile::TempDir,
}

impl L1 {
    async fn start() -> Self {
        let rig = AnvilRig::spawn(Anvil::new(), &[(OWNER, Funding::FundAndImpersonate)])
            .await
            .expect("this component test requires a working Anvil; it must not skip");
        let batcher = PrivateKeySigner::from(rig.anvil.keys()[2].clone());
        let settlement = Deployer::new(rig.provider.clone(), OWNER)
            .deploy_settlement(OWNER, 1, batcher.address())
            .await;
        let store = tempfile::tempdir().unwrap();
        let served = DaStore::open(store.path())
            .unwrap()
            .serve("127.0.0.1:0".parse().unwrap())
            .unwrap();
        Self {
            rig,
            settlement,
            batcher,
            da: DaProxy::new(format!("http://{served}")).unwrap(),
            _store: store,
        }
    }

    /// Post blocks `1..=head` as the batcher does: pack, disperse to the
    /// DA store, record the certificate on L1.
    async fn post(&self, world: &World, head: u64) {
        let provider = alloy_provider::ProviderBuilder::new()
            .wallet(alloy_network::EthereumWallet::from(self.batcher.clone()))
            .connect_http(self.rig.anvil.endpoint_url());
        let blocks = world.chain.closed_blocks(head);
        let cfg = BatcherConfig::default();
        let batches: Vec<_> = blocks
            .chunks(usize::try_from(BLOCKS_PER_BATCH).unwrap())
            .map(|group| pack_blocks(&cfg, group).unwrap())
            .collect();
        let mut index = 0;
        for batch in &batches {
            index = post_batch(&provider, self.settlement, index, batch, &self.da)
                .await
                .unwrap();
        }
    }

    /// Rebuild an executor image through `head` from L1 and the DA store
    /// alone, as `kardamom-reconstruct --executor-image` does.
    fn rebuild(&self, world: &World, head: u64, dir: &Path) {
        let descriptors = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(read_posted_batches(
                &self.rig.provider,
                self.settlement,
                0,
            ))
        })
        .unwrap();
        let frames = recover_blocks(&descriptors, &self.da).unwrap();
        let blocks = frames
            .iter()
            .filter(|f| f.block_number <= head)
            .map(block_frame_to_replay)
            .collect();
        let (genesis, _) = kardamom_engine::bin_support::resolve_genesis(
            Some(&world.root.path().join("genesis.toml")),
            1,
        )
        .unwrap();
        let outcome = Reconstruction {
            state_dir: dir,
            durability: kardamom_state::Durability::Durable,
        }
        .run(&ReplayGenesis::of(&genesis.unwrap()), blocks)
        .unwrap();
        assert_eq!(outcome.head_block, head, "{} batches", descriptors.len());
        assert!(
            outcome.head_end_tx_idx.is_some(),
            "the image carries a resume cursor"
        );
        kardamom_reconstruct::strip_to_executor_image(dir).unwrap();
    }
}

impl NodeHandle {
    fn install(&self, image: &Path) {
        NodeHandle::copy_dir(image, &self.cfg.state_dir);
        let _ = std::fs::remove_file(self.cfg.state_dir.join("mdbx.lck"));
    }
}

/// executor-fleet-total-wipe-recover: the job stops; every state DB and
/// every checkpoint goes. The image is rebuilt from the batches the
/// batcher posted to L1 and the DA store, through the production scan,
/// and installed on every node. Each replica resumes from the image's
/// cursor with no restore and no fetch, and the fleet converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Anvil and the compiled settlement contract"]
async fn a_total_wipe_resumes_every_replica_from_an_image_rebuilt_from_l1() {
    let l1 = L1::start().await;
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, 100_000, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.start();
    fleet.wait_head(16, CATCH_UP);
    fleet.stop();
    let durable = fleet.0.iter().map(NodeHandle::durable_block).min().unwrap();
    let head = durable - durable % BLOCKS_PER_BATCH;
    l1.post(&world, head).await;
    fleet.0.iter().for_each(NodeHandle::wipe_all);
    let image: PathBuf = world.root.path().join("rebuilt-from-l1");
    l1.rebuild(&world, head, &image);
    fleet.0.iter().for_each(|n| n.install(&image));
    fleet.start();
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let last = load.halt();
    fleet.assert_converged(&world, last, CATCH_UP);
    fleet.0.iter().for_each(|n| {
        let log = n.last_log();
        assert!(
            log.contains(&format!("resume_block={head}")) && log.contains(RESUMED),
            "{}: no resume from the image at {head}:\n{log}",
            n.name()
        );
        assert!(
            !log.contains(RESTORED) && !log.contains(FETCHED),
            "{}:\n{log}",
            n.name()
        );
        let env = kardamom_state::StateEnvBuilder::new(&n.cfg.state_dir)
            .open()
            .unwrap();
        assert!(
            !kardamom_state::has_trie(&env).unwrap(),
            "{}: a stale trie",
            n.name()
        );
    });
}
