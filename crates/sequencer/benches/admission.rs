//! Throughput of the sequencer v2 admission filter on one core.
//!
//! The steady state of a busy lane: senders take turns with contiguous
//! nonces, and each block of transactions gets its account rows back, which
//! confirm the forwarded runs. Each block also runs the sweep. The bench
//! measures transactions per second through `offer`, `on_rows` and `sweep`.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kardamom_sequencer::admission::{
    AccountState, Admission, AdmissionConfig, Candidate, ImagePos, Verdict,
};

/// The senders of the lane.
const SENDERS: usize = 100_000;
/// The transactions of one block.
const BLOCK: usize = 10_000;
const COST: u64 = 21_000 * 1_000_000_000;

fn cfg() -> AdmissionConfig {
    AdmissionConfig {
        max_parked: NonZeroUsize::new(64).unwrap(),
        max_forwarded: NonZeroUsize::new(64).unwrap(),
        loss_blocks: NonZeroU64::new(8).unwrap(),
        park_width: NonZeroU64::new(1 << 18).unwrap(),
        park_ttl: Duration::from_secs(60),
        relook_after: Duration::from_millis(500),
        max_senders: NonZeroUsize::new(1 << 20).unwrap(),
    }
}

/// The filter, warm: every sender has a state.
struct Lane {
    admission: Admission<u64>,
    senders: Vec<Address>,
    nonces: Vec<u64>,
    out: Vec<Verdict<u64>>,
    rows: Vec<(Address, AccountState)>,
    unit: u64,
    block: u64,
}

impl Lane {
    fn new() -> Self {
        let senders: Vec<Address> = (0..SENDERS)
            .map(|i| Address::left_padding_from(&(i as u64 + 1).to_be_bytes()))
            .collect();
        let mut admission = Admission::new(cfg(), Instant::now());
        let mut out = Vec::new();
        for (i, s) in senders.iter().enumerate() {
            admission.on_lookup(*s, state(0, i as u64), &mut out);
        }
        Self {
            admission,
            nonces: vec![0; SENDERS],
            senders,
            out,
            rows: Vec::with_capacity(BLOCK),
            unit: 0,
            block: 0,
        }
    }

    /// One block: `BLOCK` transactions, then their rows and a sweep.
    /// Returns the transactions that went out.
    fn run_block(&mut self, first: usize) -> usize {
        self.out.clear();
        self.rows.clear();
        for k in 0..BLOCK {
            self.offer_one((first + k) % SENDERS);
        }
        let forwarded = self
            .out
            .iter()
            .filter(|v| matches!(v, Verdict::Forward(_)))
            .count();
        self.block += 1;
        let rows = std::mem::take(&mut self.rows);
        self.admission
            .on_rows(self.block, rows.iter().copied(), &mut self.out);
        self.rows = rows;
        self.admission.sweep(Instant::now(), &mut self.out);
        forwarded
    }

    fn offer_one(&mut self, i: usize) {
        let nonce = self.nonces[i];
        self.nonces[i] += 1;
        self.unit += 7;
        self.admission.offer(
            Candidate {
                sender: self.senders[i],
                nonce,
                tx_hash: alloy_primitives::B256::ZERO,
                cost: U256::from(COST),
                image: ImagePos {
                    lane: 0,
                    session: 1,
                    unit: self.unit,
                },
                ingress: 0,
                answered: false,
                payload: nonce,
            },
            &mut self.out,
        );
        self.rows.push((
            self.senders[i],
            state(nonce + 1, self.block * SENDERS as u64 + i as u64 + 1),
        ));
    }
}

fn state(nonce: u64, tx_idx: u64) -> AccountState {
    AccountState {
        nonce,
        balance: U256::from(COST) * U256::from(1_000_000u64),
        delegated: false,
        tx_idx,
    }
}

fn bench_steady_lane(c: &mut Criterion) {
    let mut group = c.benchmark_group("admission");
    group.throughput(Throughput::Elements(BLOCK as u64));
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("steady_lane_100k_senders", |b| {
        let mut lane = Lane::new();
        let mut first = 0usize;
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let sent = lane.run_block(first);
                assert_eq!(sent, BLOCK, "every transaction of the block goes out");
                first = (first + BLOCK) % SENDERS;
            }
            start.elapsed()
        });
    });
    group.finish();
}

criterion_group!(benches, bench_steady_lane);
criterion_main!(benches);
