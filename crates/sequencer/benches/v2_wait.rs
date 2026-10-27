//! CPU cost of a turn with queued suffixes and no canonical confirmations.
//!
//! Setup admits every input and publishes only each sender's first nonce.
//! Timed turns have no new input, account updates or confirmations. Sweeps
//! are outside the measurement window. Afterwards, canonical feedback must
//! release every queued transaction. Results measure loop turns, not TPS or
//! transport latency; the zero-delay `v2_loop` benchmark measures active intake.

use std::time::Duration;

use alloy_primitives::U256;
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main};
use kardamom_sequencer::admission::AccountState;
use kardamom_sequencer::sealer_rules::SealerVersion;
use kardamom_sequencer::testkit::{
    EnvelopeSpec, Rig, envelope_with, one_partition_cfg, pos, signer,
};
use kardamom_sequencer::v2::{AccountEvent, AdmissionLoop, AdmissionSettings};
use kardamom_types::TxDataLoc;

struct Waiting {
    admission: AdmissionLoop,
    rig: Rig,
    accounts: crossbeam_channel::Sender<AccountEvent>,
    senders: usize,
    nonces: u64,
    observed: usize,
}

impl Waiting {
    fn new(senders: u64, nonces: u64) -> Self {
        let mut cfg = AdmissionSettings::default().loop_config(Duration::from_secs(3600), 0);
        cfg.sweep_every = Duration::from_secs(3600);
        let (accounts, receiver) = crossbeam_channel::unbounded();
        let (lookups, _) = tokio::sync::mpsc::channel(16);
        let mut admission = AdmissionLoop::new(
            cfg,
            one_partition_cfg().vslot_set().unwrap(),
            receiver,
            lookups,
        );
        admission.enable_positioned_refs(SealerVersion::starting_at(5));
        let mut waiting = Self {
            admission,
            rig: Rig::default(),
            accounts,
            senders: usize::try_from(senders).unwrap(),
            nonces,
            observed: 0,
        };
        (1..=senders).for_each(|seed| waiting.add_sender(seed));
        waiting.drain();
        waiting.assert_waiting();
        waiting
    }

    fn add_sender(&mut self, seed: u64) {
        let signer = signer(seed);
        self.accounts
            .send(AccountEvent::Rows {
                block: 1,
                rows: vec![(
                    signer.address(),
                    AccountState {
                        nonce: 0,
                        balance: U256::MAX,
                        delegated: false,
                        tx_idx: 1,
                    },
                )],
            })
            .unwrap();
        (0..self.nonces).for_each(|nonce| {
            let index = seed
                .checked_mul(self.nonces)
                .unwrap()
                .checked_add(nonce)
                .unwrap();
            let offset = i32::try_from(index.checked_mul(64).unwrap()).unwrap();
            self.rig.push(
                TxDataLoc::new(0, pos(offset)),
                envelope_with(
                    &signer,
                    nonce,
                    index,
                    EnvelopeSpec {
                        real_hash: true,
                        ..EnvelopeSpec::default()
                    },
                ),
            );
        });
    }

    fn turn(&mut self) -> bool {
        self.admission.run_once(&mut self.rig.ports()).unwrap()
    }

    fn drain(&mut self) {
        while self.turn() {}
    }

    fn assert_waiting(&self) {
        assert!(
            self.rig.tx_data.queue.is_empty(),
            "the measurement cannot include input intake"
        );
        assert_eq!(
            self.rig.offers().len(),
            self.senders,
            "one unconfirmed offer per sender"
        );
        assert!(self.rig.errors().is_empty());
        assert!(self.rig.drops().is_empty());
    }

    fn confirm(&mut self) {
        let offers = self.rig.offers();
        offers[self.observed..].iter().for_each(|offer| {
            self.accounts
                .send(AccountEvent::Canonical(offer.tx_ref.tx_hash))
                .unwrap();
        });
        self.observed = offers.len();
        self.drain();
    }

    fn finish(mut self) {
        self.assert_waiting();
        (0..self.nonces).for_each(|_| self.confirm());
        assert_eq!(
            self.rig.offers().len(),
            self.senders
                .checked_mul(usize::try_from(self.nonces).unwrap())
                .unwrap()
        );
        assert!(self.rig.errors().is_empty());
        assert!(self.rig.drops().is_empty());
    }
}

impl Waiting {
    fn bench(c: &mut Criterion) {
        let mut group = c.benchmark_group("v2_wait");
        for (senders, nonces) in [(1, 2), (1, 64), (16, 64), (256, 16)] {
            Self::new(senders, nonces).measure(&mut group);
        }
        group.finish();
    }

    fn measure(mut self, group: &mut BenchmarkGroup<'_, WallTime>) {
        group.bench_function(
            BenchmarkId::new("blocked_turn", format!("{}x{}", self.senders, self.nonces)),
            |b| {
                b.iter(|| std::hint::black_box(self.turn()));
            },
        );
        self.finish();
    }
}

criterion_group!(benches, Waiting::bench);
criterion_main!(benches);
