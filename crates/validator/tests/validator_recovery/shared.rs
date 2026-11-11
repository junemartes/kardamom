//! The fixtures that every test of the process shares: the reference
//! chain and the media driver. The chain and its checkpoint never change,
//! and each test opens its own streams and egress on the driver, so one
//! of each serves every test. The first test builds the chain while the
//! driver starts. No destructor of a static runs, so a process exit hook
//! removes the driver container.

use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, OnceLock};

use kardamom_log::testing::AeronTestCluster;

use super::chain::Chain;

/// The container of the shared driver, for the exit hook.
static CONTAINER: OnceLock<String> = OnceLock::new();
static SHARED: OnceLock<Shared> = OnceLock::new();

/// Remove the driver container when the process exits.
extern "C" fn remove_container() {
    if let Some(id) = CONTAINER.get() {
        let _ = Command::new("docker").args(["rm", "-f", id]).output();
    }
}

pub(crate) struct Shared {
    pub(crate) chain: Arc<Chain>,
    driver: AeronTestCluster,
    /// The runtime the driver started on.
    _rt: tokio::runtime::Runtime,
}

impl Shared {
    /// The fixtures of the process, built on the first call.
    pub(crate) fn get() -> &'static Self {
        SHARED.get_or_init(Self::start)
    }

    fn start() -> Self {
        let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("validator-recovery-chain");
        if dir.exists() {
            std::fs::remove_dir_all(&dir).unwrap();
        }
        std::fs::create_dir_all(&dir).unwrap();
        let building = std::thread::spawn(move || {
            Chain::build(
                &dir,
                crate::BLOCKS,
                crate::CHECKPOINT_BLOCK,
                &[crate::JOIN_BLOCKS, crate::BLOCKS],
            )
        });
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let driver = rt.block_on(async {
            kardamom_log::testing::require_docker().await;
            AeronTestCluster::single_node_with_client_liveness(crate::LIVENESS)
                .await
                .unwrap()
        });
        if let Some(id) = driver.container_id(0) {
            CONTAINER.set(id.to_string()).unwrap();
            // SAFETY: the hook is a plain function with no argument; it
            // reads only a static.
            assert_eq!(unsafe { libc::atexit(remove_container) }, 0);
        }
        Self {
            chain: Arc::new(building.join().unwrap()),
            driver,
            _rt: rt,
        }
    }

    /// The `aeron.dir` of the shared driver.
    pub(crate) fn aeron_dir(&self) -> PathBuf {
        self.driver.aeron_dir_host(0).to_path_buf()
    }
}
