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
//! A third case, `with_verdicts`, takes the `in_order` input with the
//! verdicts on: the loop queues one verdict for each transaction, to the
//! ingress that published it. Eight ingress ids publish the input.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use alloy_primitives::U256;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use kardamom_obs::lifecycle::process;
use kardamom_sequencer::admission::{AccountState, AdmissionConfig};
use kardamom_sequencer::resync::{ResyncChannel, ResyncConfig, SealerRefusal};
use kardamom_sequencer::sealer_rules::SealerVersion;
use kardamom_sequencer::testkit::{Rig, one_partition_cfg, signed_envelope, signer};
use kardamom_sequencer::v2::{AccountEvent, AdmissionLoop, V2Config};
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};

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
        sweep_every: if std::env::var_os("KARDAMOM_V2_BENCH_FORCE_SWEEPS").is_some() {
            Duration::ZERO
        } else {
            Duration::from_millis(50)
        },
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

/// A loop whose filter holds a rich row for every sender of the batch,
/// and the sender side of its account events.
struct Inputs {
    _accounts: crossbeam_channel::Sender<AccountEvent>,
    _refusals: Option<crossbeam_channel::Sender<SealerRefusal>>,
}

fn seeded_loop() -> (AdmissionLoop, Inputs) {
    let (accounts_tx, accounts_rx) = crossbeam_channel::unbounded();
    let (lookups_tx, _) = tokio::sync::mpsc::channel(16);
    let vslots = one_partition_cfg().vslot_set().unwrap();
    accounts_tx.send(rows(1, 0)).unwrap();
    let mut admission = AdmissionLoop::new(cfg(), vslots, accounts_rx, lookups_tx);
    admission.enable_pause(process().subscribe());
    if std::env::var_os("KARDAMOM_V2_BENCH_CARRY").is_some() {
        admission.enable_positioned_refs(SealerVersion::starting_at(5));
    }
    let refusals = if std::env::var_os("KARDAMOM_V2_BENCH_TERMINAL_REFUSALS").is_some() {
        let channel = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
        admission.enable_sealer_refusals(channel.controller);
        Some(channel.deadline_tx)
    } else {
        None
    };
    (
        admission,
        Inputs {
            _accounts: accounts_tx,
            _refusals: refusals,
        },
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
                let (admission, inputs) = seeded_loop();
                (admission, rig, inputs)
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

/// The input of `in_order`, with the verdicts on.
fn bench_with_verdicts(c: &mut Criterion) {
    let batch = batch(|k| k);
    let total = usize::try_from(SENDERS * NONCES).unwrap();
    let mut group = c.benchmark_group("v2_loop");
    group.throughput(Throughput::Elements(SENDERS * NONCES));
    group.bench_function("with_verdicts", |b| {
        b.iter_batched(
            || {
                let mut rig = Rig::default();
                rig.tx_data.queue = batch.clone().into_iter().collect();
                let (mut l, inputs) = seeded_loop();
                let (verdicts_tx, verdicts) = tokio::sync::mpsc::channel(total);
                l.enable_verdicts(verdicts_tx);
                (l, rig, verdicts, inputs)
            },
            |(mut l, mut rig, mut verdicts, _inputs)| {
                while l.run_once(&mut rig.ports()).unwrap() {}
                let sent = std::iter::from_fn(|| verdicts.try_recv().ok()).count();
                assert_eq!(sent, total, "one verdict for each transaction");
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Drain the batch through the loop, then check that every transaction
/// went out.
fn run_to_completion((mut l, mut rig, _inputs): (AdmissionLoop, Rig, Inputs)) {
    while l.run_once(&mut rig.ports()).unwrap() {}
    let published = rig.refs.offers.lock().unwrap().len();
    assert_eq!(published, usize::try_from(SENDERS * NONCES).unwrap());
}

criterion_group!(benches, bench_in_order, bench_reversed, bench_with_verdicts);
criterion_main!(benches);
