//! The throughput of the v2 loop on one core: guard, decode, fee checks,
//! the admission filter, and the offer to the sealer.
//!
//! The input is the input of `benches/throughput.rs`: the proxy supplies
//! the sender, so the loop does no secp256k1 work. Each sender has a row,
//! so no transaction waits for a lookup. Two orders:
//! - `in_order`: each sender's nonces arrive in order, and each one goes
//!   out at once;
//! - `reversed`: each sender's nonces arrive last first, so each one parks
//!   until nonce 0 arrives and frees the run.
//!
//! A third case, `with_verdicts`, takes the `in_order` input, then the
//! receipts and the rows of each block, one block for each nonce. The loop
//! matches each receipt to the ingress of its transaction and queues one
//! verdict frame for each ingress and block. Eight ingress ids publish the
//! input.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use alloy_primitives::{Address, U256};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use kardamom_sequencer::admission::{AccountState, AdmissionConfig};
use kardamom_sequencer::testkit::{Rig, one_partition_cfg, signed_envelope, signer};
use kardamom_sequencer::v2::{AccountEvent, AdmissionLoop, V2Config, VerdictOut};
use kardamom_types::{AccountRow, BPosition, Receipt, TxDataLoc, TxEnvelope};

const SENDERS: u64 = 256;
const NONCES: u64 = 16;
const INGRESS_IDS: u64 = 8;

fn cfg() -> V2Config {
    V2Config {
        partition: 0,
        admission: AdmissionConfig {
            max_parked: NonZeroUsize::new(32).unwrap(),
            max_forwarded: NonZeroUsize::new(64).unwrap(),
            loss_blocks: NonZeroU64::new(16).unwrap(),
            park_width: NonZeroU64::new(1 << 17).unwrap(),
            park_ttl: Duration::from_secs(60),
            relook_after: Duration::from_millis(500),
            max_senders: NonZeroUsize::new(1 << 20).unwrap(),
        },
        sweep_every: Duration::from_millis(50),
        outbox_cap: NonZeroUsize::new(4096).unwrap(),
    }
}

/// The envelopes of every sender, sender by sender, each sender's nonces
/// in the order that `nonce_at` gives.
fn batch(nonce_at: fn(u64) -> u64) -> Vec<(TxDataLoc, TxEnvelope)> {
    let signers: Vec<_> = (1..=SENDERS).map(signer).collect();
    signers
        .iter()
        .enumerate()
        .flat_map(|(i, s)| (0..NONCES).map(move |k| (i, s, k)))
        .map(|(i, s, k)| {
            let correlation = u64::try_from(i).unwrap() * NONCES + k;
            let position = BPosition {
                term_id: 0,
                term_offset: i32::try_from(correlation).unwrap() * 64,
            };
            let ingress = u16::try_from(correlation % INGRESS_IDS).unwrap();
            let id = TxEnvelope::pack_correlation_id(ingress, correlation);
            (
                TxDataLoc::new(0, position),
                signed_envelope(s, nonce_at(k), id),
            )
        })
        .collect()
}

/// The rows of every sender at `nonce`, as of `block`.
fn rows(block: u64, nonce: u64) -> AccountEvent {
    let rows = (1..=SENDERS)
        .map(|n| {
            let state = AccountState {
                nonce,
                balance: U256::from(10u64).pow(U256::from(24u64)),
                delegated: false,
                tx_idx: block,
            };
            (signer(n).address(), state)
        })
        .collect();
    AccountEvent::Rows { block, rows }
}

/// The receipts of every sender at `nonce`, and their rows after it.
fn receipts(nonce: u64) -> AccountEvent {
    let senders: Vec<Address> = (1..=SENDERS).map(|n| signer(n).address()).collect();
    AccountEvent::Receipts {
        receipts: senders
            .iter()
            .map(|&from| Receipt {
                from,
                nonce,
                ..Receipt::default()
            })
            .collect(),
        rows: senders
            .iter()
            .map(|&address| AccountRow {
                address,
                nonce: nonce + 1,
                balance: U256::from(10u64).pow(U256::from(24u64)),
            })
            .collect(),
    }
}

/// A loop whose filter holds a rich row for every sender of the batch,
/// and the sender side of its account events.
fn seeded_loop() -> (AdmissionLoop, crossbeam_channel::Sender<AccountEvent>) {
    let (accounts_tx, accounts_rx) = crossbeam_channel::unbounded();
    let (lookups_tx, _) = tokio::sync::mpsc::channel(16);
    let vslots = one_partition_cfg().vslot_set().unwrap();
    accounts_tx.send(rows(1, 0)).unwrap();
    (
        AdmissionLoop::new(cfg(), vslots, accounts_rx, lookups_tx),
        accounts_tx,
    )
}

fn bench_order(c: &mut Criterion, name: &str, nonce_at: fn(u64) -> u64) {
    let batch = batch(nonce_at);
    let mut group = c.benchmark_group("v2_loop");
    group.throughput(Throughput::Elements(SENDERS * NONCES));
    group.bench_function(name, |b| {
        b.iter_batched(
            || {
                let mut rig = Rig::default();
                rig.tx_data.queue = batch.clone().into_iter().collect();
                (seeded_loop().0, rig)
            },
            run_to_completion,
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_in_order(c: &mut Criterion) {
    bench_order(c, "in_order", |k| k);
}

fn bench_reversed(c: &mut Criterion) {
    bench_order(c, "reversed", |k| NONCES - 1 - k);
}

/// The input of `in_order`, then the receipts and the rows of one block
/// for each nonce.
fn bench_with_verdicts(c: &mut Criterion) {
    let batch = batch(|k| k);
    let blocks: Vec<(AccountEvent, AccountEvent)> = (0..NONCES)
        .map(|k| (receipts(k), rows(k + 2, k + 1)))
        .collect();
    let mut group = c.benchmark_group("v2_loop");
    group.throughput(Throughput::Elements(SENDERS * NONCES));
    group.bench_function("with_verdicts", |b| {
        b.iter_batched(
            || {
                let mut rig = Rig::default();
                rig.tx_data.queue = batch.clone().into_iter().collect();
                let (mut l, accounts) = seeded_loop();
                let (verdicts_tx, verdicts) = tokio::sync::mpsc::channel(1024);
                l.enable_verdicts(verdicts_tx);
                let events = blocks.clone();
                VerdictRun {
                    l,
                    rig,
                    accounts,
                    events,
                    verdicts,
                }
            },
            VerdictRun::run,
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// One iteration of `with_verdicts`.
struct VerdictRun {
    l: AdmissionLoop,
    rig: Rig,
    accounts: crossbeam_channel::Sender<AccountEvent>,
    events: Vec<(AccountEvent, AccountEvent)>,
    verdicts: tokio::sync::mpsc::Receiver<VerdictOut>,
}

impl VerdictRun {
    /// Offer the batch, then apply each block, then check that every
    /// receipt went to the ingress of its transaction.
    fn run(mut self) {
        while self.l.run_once(&mut self.rig.ports()).unwrap() {}
        self.events
            .into_iter()
            .flat_map(|(receipts, rows)| [receipts, rows])
            .for_each(|e| self.accounts.send(e).unwrap());
        self.l.run_once(&mut self.rig.ports()).unwrap();
        let sent: usize = std::iter::from_fn(|| self.verdicts.try_recv().ok())
            .map(|frame| match frame {
                VerdictOut::Receipts { batch, .. } => batch.receipts.len(),
                VerdictOut::Rollback(_) => 0,
            })
            .sum();
        assert_eq!(sent, usize::try_from(SENDERS * NONCES).unwrap());
    }
}

/// Drain the batch through the loop, then check that every transaction
/// went out.
fn run_to_completion((mut l, mut rig): (AdmissionLoop, Rig)) {
    while l.run_once(&mut rig.ports()).unwrap() {}
    let published = rig.refs.offers.lock().unwrap().len();
    assert_eq!(published, usize::try_from(SENDERS * NONCES).unwrap());
}

criterion_group!(benches, bench_in_order, bench_reversed, bench_with_verdicts);
criterion_main!(benches);
