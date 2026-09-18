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

/// One contention sweep's fixed knobs: the thread count, the
/// transactions each thread runs per block, whether to publish the fee
/// sink, and the access pattern. `run_one_block`'s worker threads each
/// hold a copy.
#[derive(Clone, Copy)]
struct Contention {
    threads: usize,
    per_thread: usize,
    with_sink: bool,
    access: Access,
}

impl Contention {
    fn new(threads: NonZeroUsize, with_sink: bool, access: Access) -> Self {
        let threads = threads.get();
        Self {
            threads,
            per_thread: BLOCK_TXS / threads,
            with_sink,
            access,
        }
    }

    /// Run `BLOCKS` blocks, and return the average ns/tx.
    #[allow(
        clippy::cast_precision_loss,
        reason = "nanosecond and tx-count display values stay far under 2^52"
    )]
    fn run(self) -> f64 {
        let started = std::time::Instant::now();
        for _ in 0..BLOCKS {
            self.run_one_block();
        }
        let total_txs = (BLOCKS * self.per_thread * self.threads) as f64;
        started.elapsed().as_nanos() as f64 / total_txs
    }

    /// Run one block: a fresh `MvCache`, exactly as the pool builds one
    /// per block, with `self.threads` worker threads each doing
    /// `self.per_thread` transactions, scoped so every worker joins
    /// before this returns.
    fn run_one_block(self) {
        let mv = MvCache::new();
        std::thread::scope(|s| {
            for w in 0..self.threads {
                let mv = &mv;
                s.spawn(move || self.run_one_worker(mv, w));
            }
        });
    }

    /// Run one worker's `self.per_thread` transactions against `mv`.
    fn run_one_worker(self, mv: &MvCache, w: usize) {
        for i in 0..self.per_thread {
            self.one_tx(mv, w, i);
        }
    }

    /// One transaction's shared-structure traffic: an interleaved
    /// index, so the version lists grow the way they do under real
    /// dispatch, and the access-pattern account pair.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "idx stays under BLOCK_TXS, a small fixed constant"
    )]
    fn one_tx(self, mv: &MvCache, w: usize, i: usize) {
        let idx = (i * self.threads + w) as u32;
        let (sender, recipient) = self.access_pair(w, i);
        tx_pattern(mv, idx, sender, recipient, self.with_sink);
    }

    /// The `(sender, recipient)` account pair for worker `w`'s `i`-th
    /// transaction, under `self.access`'s contention pattern.
    fn access_pair(self, w: usize, i: usize) -> (usize, usize) {
        match self.access {
            Access::Shared => ((w * 7 + i) % ACCOUNTS, (w * 13 + i * 3 + 1) % ACCOUNTS),
            Access::Partitioned => {
                // Use only accounts congruent to this thread.
                let own = |k: usize| (k % (ACCOUNTS / self.threads)) * self.threads + w;
                (own(i), own(i * 3 + 1))
            }
        }
    }
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
        base = report_contention_row(n, t, with_sink, access, base);
    }
    println!(
        "\nns/tx is per transaction of shared-structure work, wall clock.\n\
         Flat = the cache scales and the inflation is elsewhere.\n\
         Rising = each added worker makes every transaction more expensive."
    );
}

/// Run and print one thread-count row. `n` is this row's index in the
/// sweep: row 0 sets the baseline. Returns the baseline to use for the
/// next row (unchanged after row 0).
fn report_contention_row(
    n: usize,
    t: NonZeroUsize,
    with_sink: bool,
    access: Access,
    base: f64,
) -> f64 {
    let ns = Contention::new(t, with_sink, access).run();
    let base = if n == 0 { ns } else { base };
    println!(
        "{t:>3}  {ns:>10.1}  {:>9.2}x  {:>12.0}",
        ns / base,
        1e9 / ns
    );
    base
}
