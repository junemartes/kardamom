//! This tool asks: does the multi-version cache scale?
//!
//! Per-transaction execution cost rises with worker count, even on a
//! fully idle machine: a transfer costs roughly twice as much at eight
//! workers as at one. Inside the pool, the read path is tangled with
//! revm, the allocator, the scheduler, and the state backend. This
//! benchmark strips all of that away: threads do nothing but the
//! shared-structure traffic a transfer performs, against a real
//! `MvCache`.
//!
//! The benchmark must mirror a real block, or it measures its own
//! artifacts. Two rules follow from that:
//!
//! - `publish_account` does a sorted insert into a per-account version
//!   list. A real block publishes at most `MAX_BLOCK_TXS` versions and
//!   then discards the cache, so this benchmark must do the same: a
//!   long-running loop against one cache is quadratic and measures the
//!   harness, not the engine.
//! - The engine skips the fee sink on publish; it folds the fee as a
//!   delta instead. Publishing it makes every transaction in the block
//!   write one hot key, which is a property of the benchmark, not the
//!   engine.
//!
//! So this benchmark uses a fresh cache per block, a realistic block
//! size, and skips the fee sink. The `--with-sink` mode restores it, to
//! price the accumulator deferral, not to be mistaken for cache behavior.

use std::num::NonZeroUsize;

use alloy_primitives::{Address, B256, U256};
use kardamom_stm::mv::{AccountVersion, MvCache};

/// The distinct accounts in play, matching the transfers scenario.
/// This decides how often two threads land on the same shard.
const ACCOUNTS: usize = 96;
/// The transactions per block, near the pool's `MAX_BLOCK_TXS`.
const BLOCK_TXS: usize = 4000;
const BLOCKS: usize = 60;

fn addr(i: usize) -> Address {
    let mut b = [0u8; 20];
    b[12..20].copy_from_slice(&(i as u64).to_be_bytes());
    Address::from(b)
}

fn version(i: u64) -> AccountVersion {
    AccountVersion {
        nonce: i,
        balance: U256::from(i),
        code_hash: B256::ZERO,
    }
}

/// One transaction's worth of shared-structure traffic: the two
/// account reads and two account publishes that a value transfer performs.
#[inline]
fn tx_pattern(mv: &MvCache, idx: u32, sender: usize, recipient: usize, with_sink: bool) {
    std::hint::black_box(mv.read_account(idx, &addr(sender)));
    std::hint::black_box(mv.read_account(idx, &addr(recipient)));
    mv.publish_account(idx, addr(sender), version(u64::from(idx)));
    mv.publish_account(idx, addr(recipient), version(u64::from(idx)));
    if with_sink {
        mv.publish_account(idx, addr(0), version(u64::from(idx)));
    }
}

/// How threads pick the accounts they touch.
#[derive(Clone, Copy, PartialEq)]
enum Access {
    /// Every thread cycles through every account. This is the worst
    /// case, and not what the pool does.
    Shared,
    /// Each thread owns a disjoint residue class of accounts. This
    /// mirrors the domain-hashed dispatch that assigns one contention
    /// domain to one worker. This is the access pattern the engine
    /// actually generates.
    Partitioned,
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "idx stays under BLOCK_TXS (a small fixed constant); nanosecond and tx-count display values stay far under 2^52"
)]
fn run(threads: NonZeroUsize, with_sink: bool, access: Access) -> f64 {
    let threads = threads.get();
    let per_thread = BLOCK_TXS / threads;
    let started = std::time::Instant::now();
    for _ in 0..BLOCKS {
        // Use one cache per block, exactly as the pool builds one per block.
        let mv = MvCache::new();
        std::thread::scope(|s| {
            for w in 0..threads {
                let mv = &mv;
                s.spawn(move || {
                    for i in 0..per_thread {
                        // Interleave indices, so the version lists grow the
                        // way they do under real dispatch.
                        let idx = (i * threads + w) as u32;
                        let (sender, recipient) = match access {
                            Access::Shared => {
                                ((w * 7 + i) % ACCOUNTS, (w * 13 + i * 3 + 1) % ACCOUNTS)
                            }
                            Access::Partitioned => {
                                // Use only accounts congruent to this thread.
                                let own = |k: usize| (k % (ACCOUNTS / threads)) * threads + w;
                                (own(i), own(i * 3 + 1))
                            }
                        };
                        tx_pattern(mv, idx, sender, recipient, with_sink);
                    }
                });
            }
        });
    }
    let total_txs = (BLOCKS * per_thread * threads) as f64;
    started.elapsed().as_nanos() as f64 / total_txs
}

/// The parsed CLI options.
struct Opts {
    spec: String,
    with_sink: bool,
    access: Access,
}

impl Opts {
    fn apply(mut self, a: &str) -> Self {
        match a {
            "--with-sink" => self.with_sink = true,
            "--partitioned" => self.access = Access::Partitioned,
            other => self.spec = other.to_string(),
        }
        self
    }
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            spec: "1,2,4,6,8".to_string(),
            with_sink: false,
            access: Access::Shared,
        }
    }
}

fn main() {
    let Opts {
        spec,
        with_sink,
        access,
    } = std::env::args()
        .skip(1)
        .fold(Opts::default(), |o, a| o.apply(&a));
    let threads: Vec<NonZeroUsize> = spec
        .split(',')
        .map(|s| {
            let t: NonZeroUsize = s.trim().parse().expect("thread counts csv: 0 is not valid");
            // Only Access::Partitioned divides ACCOUNTS by the thread
            // count (`own`'s `ACCOUNTS / threads`); Access::Shared has
            // no such divisor, so a thread count over ACCOUNTS is fine
            // there, as it always was.
            assert!(
                access != Access::Partitioned || t.get() <= ACCOUNTS,
                "thread count {t} exceeds ACCOUNTS ({ACCOUNTS}); --partitioned divides ACCOUNTS by the thread count"
            );
            t
        })
        .collect();

    println!(
        "MvCache: {BLOCKS} blocks x {BLOCK_TXS} txs, {ACCOUNTS} accounts, fee sink {}, access {}",
        if with_sink { "PUBLISHED" } else { "skipped" },
        if access == Access::Partitioned {
            "PARTITIONED (as dispatched)"
        } else {
            "shared (worst case)"
        }
    );
    println!(
        "{:>3}  {:>10}  {:>10}  {:>12}",
        "t", "ns/tx", "vs t=1", "tx/s"
    );
    let mut base = 0f64;
    for (n, &t) in threads.iter().enumerate() {
        let ns = run(t, with_sink, access);
        if n == 0 {
            base = ns;
        }
        println!(
            "{t:>3}  {ns:>10.1}  {:>9.2}x  {:>12.0}",
            ns / base,
            1e9 / ns
        );
    }
    println!(
        "\nns/tx is per transaction of shared-structure work, wall clock.\n\
         Flat = the cache scales and the inflation is elsewhere.\n\
         Rising = each added worker makes every transaction more expensive."
    );
}
