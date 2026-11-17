//! This test runs the uniswap load workload on the real kardamom engine:
//! the setup in one block, then a few hundred mixed swaps and transfers
//! in the next block. Every receipt must succeed in two different orders
//! of the senders, as `execute_tx` with `MockStateDatabase` runs them in
//! the executor. The new block makes the first swap of each pair write
//! its price accumulators.

use alloy_primitives::{Address, U256};
use kardamom_bench::ANVIL_MNEMONIC;
use kardamom_bench::load::SwapShare;
use kardamom_bench::load::plan::{PlannedTx, TxClass, TxPlanParams, TxShape};
use kardamom_bench::load::uniswap::{UniswapMix, UniswapPlan, UniswapRun};
use kardamom_bench::signers::SignerSet;
use kardamom_bench::stm::{BlockAt, Executed, SeqExec};
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::state::MockStateDatabase;
use kardamom_types::BPosition;

const CHAIN_ID: u64 = 412_346;
const SENDERS: u32 = 8;
const PER_SENDER: usize = 40;

/// One signed run, and the signers that signed it.
struct Fixture {
    signers: SignerSet,
    run: UniswapRun,
}

impl Fixture {
    fn new() -> Self {
        let signers = SignerSet::derive(ANVIL_MNEMONIC, SENDERS).unwrap();
        let starts = vec![0; signers.len()];
        let params = TxPlanParams {
            chain_id: CHAIN_ID,
            nonce_start: 0,
            gas_price: 1_000_000_000,
        };
        let mix = UniswapMix {
            swap_share: SwapShare::new(50).unwrap(),
            pairs: std::num::NonZeroUsize::new(3).unwrap(),
        };
        let transfer = TxShape::plain(Address::repeat_byte(0xEE), U256::from(1u64));
        let run = UniswapPlan::new(&signers, &starts, params, mix)
            .unwrap()
            .sign(&transfer, PER_SENDER)
            .unwrap();
        Self { signers, run }
    }

    /// The setup in its landing order: the deploys, then each sender.
    fn setup(&self) -> impl Iterator<Item = &PlannedTx> {
        self.run
            .deploys
            .iter()
            .chain(self.run.sender_setup.iter().flatten())
    }

    /// The load, sender by sender, from the last sender to the first.
    fn senders_in_turn(&self) -> Vec<&PlannedTx> {
        self.run.queues.iter().rev().flatten().collect()
    }

    /// The load, one transaction of each sender in turn.
    fn round_robin(&self) -> Vec<&PlannedTx> {
        (0..PER_SENDER)
            .flat_map(|i| self.run.queues.iter().map(move |queue| &queue[i]))
            .collect()
    }

    /// Run the setup in block 0, then `load` in block 1, on fresh engine
    /// scopes. Returns the gas of each load receipt, with its class.
    fn execute(&self, load: &[&PlannedTx]) -> Vec<(TxClass, u64)> {
        let snapshot = kardamom_bench::stm::workload::funded_snapshot(&self.signers);
        let mut setup = Engine::new(&snapshot, None, BlockAt(0), &self.signers);
        let mut delta = PendingDelta::new();
        self.setup()
            .for_each(|tx| delta.apply(setup.run(tx).write_set));
        let mut block = Engine::new(&snapshot, Some(&delta), BlockAt(1), &self.signers);
        load.iter()
            .map(|tx| (tx.class, block.run(tx).receipt.gas_used))
            .collect()
    }
}

/// One engine scope, and the count of the transactions it ran.
struct Engine<'a> {
    exec: SeqExec<'a, MockStateDatabase>,
    signers: &'a SignerSet,
    index: u64,
}

impl<'a> Engine<'a> {
    fn new(
        snapshot: &'a MockStateDatabase,
        parent: Option<&PendingDelta>,
        block: BlockAt,
        signers: &'a SignerSet,
    ) -> Self {
        Self {
            exec: SeqExec::new(snapshot, parent, block.env(CHAIN_ID)).unwrap(),
            signers,
            index: 0,
        }
    }

    /// Run `tx`, and assert that its receipt succeeded.
    fn run(&mut self, tx: &PlannedTx) -> Executed {
        let i = self.index;
        let slot = kardamom_engine::exec_types::TxSlot {
            tx_idx: kardamom_engine::TxIndex(i),
            tx_position: BPosition::from_index(i),
            tx_index_in_block: i,
            cumulative_gas_used_before: 0,
        };
        let sender = self.signers[tx.sender].address;
        let out = self
            .exec
            .run(slot, &tx.to_envelope(sender, i), None)
            .unwrap_or_else(|e| panic!("tx {i} (sender {} nonce {}): {e:?}", tx.sender, tx.nonce));
        assert!(
            out.receipt.status,
            "{:?} tx failed (sender {} nonce {}): {:?}",
            tx.class, tx.sender, tx.nonce, out.receipt
        );
        self.index += 1;
        out
    }
}

#[test]
fn every_swap_and_transfer_succeeds_in_two_sender_orders() {
    let fx = Fixture::new();
    for load in [fx.senders_in_turn(), fx.round_robin()] {
        fx.check(&load);
    }
}

impl Fixture {
    /// Run the setup and `load`, and check the gas of each class. Each
    /// receipt must succeed, see [`Engine::run`].
    fn check(&self, load: &[&PlannedTx]) {
        let gas = self.execute(load);
        let swaps: Vec<u64> = gas
            .iter()
            .filter(|(class, _)| *class == TxClass::Swap)
            .map(|(_, gas)| *gas)
            .collect();
        assert_eq!(swaps.len(), PER_SENDER * SENDERS as usize / 2);
        assert!(
            swaps.iter().all(|gas| (60_000..200_000).contains(gas)),
            "a router swap with two token transfers: {swaps:?}"
        );
        assert!(
            gas.iter()
                .filter(|(class, _)| *class == TxClass::Transfer)
                .all(|(_, gas)| *gas == 21_000)
        );
    }
}
