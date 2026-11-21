use alloy_primitives::{Address, U256};
use crossbeam_channel::{Receiver, unbounded};
use kardamom_engine::actor::fixtures::{
    ChanReceiptsPub, ChanTxDataSub, ChanTxOrderingSub, Imm, LegacyTx, TestWiring, byte_signer,
};
use kardamom_engine::reader::NoExecStream;
use kardamom_engine::{
    BPosition, BlockBoundaryStart, CMessage, Either, Executor, ExecutorConfig, ExecutorError,
    Inbound, MockStateDatabase, MutatingSnapshotSource, Outbound, ResumePoint, RoleHooks,
    StateDatabase, TxOrderingMessage, TxRef, WriterApplyingQueue,
};
use revm::primitives::KECCAK_EMPTY;

use super::NoReceipts;

const SINK: Address = Address::repeat_byte(0x5c);

/// The wiring of the test: the channel fakes of the engine, with the
/// receipt output of the binary over the channel publication.
type Wiring = TestWiring<ChanTxDataSub, ChanTxOrderingSub, Either<ChanReceiptsPub, NoReceipts>>;

/// One run of the engine over one block with one transfer of 7 wei, with
/// the receipts on or off.
struct OneBlock {
    publish: bool,
}

impl OneBlock {
    /// The inputs of the block: the envelope on lane 0, and the reference
    /// and the boundary on the ordering. The senders end at return, so the
    /// readers see the end of each input after the block.
    fn inputs(tx: &kardamom_engine::TxEnvelope) -> (ChanTxDataSub, ChanTxOrderingSub) {
        let at = BPosition::from_index(0);
        let (data, data_rx) = unbounded();
        let (ordering, ordering_rx) = unbounded();
        data.send((at, tx.clone())).unwrap();
        ordering
            .send((
                at,
                TxOrderingMessage::TxRef(TxRef::new(tx.tx_hash, 0, at, 0)),
            ))
            .unwrap();
        let boundary = BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: BPosition::from_index(1),
            l2_timestamp: 1_700_000_001,
            l1_origin: 0,
        };
        ordering
            .send((
                boundary.end_tx_idx,
                TxOrderingMessage::BoundaryStart(boundary),
            ))
            .unwrap();
        (
            ChanTxDataSub {
                sequencer_id: 0,
                rx: data_rx,
            },
            ChanTxOrderingSub(ordering_rx),
        )
    }

    /// Run the block. Returns what reached the `tx_receipts` channel, and
    /// the balance of the sink after the run.
    fn run(self) -> (Vec<CMessage>, U256) {
        let signer = byte_signer(0x5a);
        let state = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                KECCAK_EMPTY,
            )
            .build();
        let tx = LegacyTx {
            chain_id: 1,
            to: SINK,
            nonce: 0,
            value: 7,
            gas_limit: 21_000,
            gas_price: 0,
            ..LegacyTx::default()
        }
        .sign(&signer);
        let (tx_data, tx_ordering) = Self::inputs(&tx);
        let (receipts, published) = Self::output(self.publish);
        let result = Executor::<Wiring>::new(
            ExecutorConfig::default(),
            Inbound {
                tx_data: vec![tx_data],
                tx_ordering,
                join_recovery: None,
                exec_stream: NoExecStream,
            },
            Outbound {
                tx_receipts: receipts,
                snapshots: MutatingSnapshotSource(state.clone()),
                writer_signal: Imm,
                writer_queue: WriterApplyingQueue::new(state.clone()),
            },
            ResumePoint::GENESIS,
            RoleHooks::none(),
        )
        .spawn()
        .join();
        assert!(
            matches!(result, Ok(()) | Err(ExecutorError::TxOrderingClosed)),
            "{result:?}"
        );
        let (_, balance, _) = state.basic(SINK).unwrap().expect("the sink");
        (published.try_iter().collect(), balance)
    }

    /// The receipt output as the binary chooses it, and the channel that a
    /// publication would write. Off, the channel sender ends here unused.
    fn output(publish: bool) -> (Either<ChanReceiptsPub, NoReceipts>, Receiver<CMessage>) {
        let (sender, published) = unbounded();
        let out = if publish {
            Either::Left(ChanReceiptsPub(sender))
        } else {
            Either::Right(NoReceipts)
        };
        (out, published)
    }
}

/// With the receipts off, the executor executes and commits the block, but
/// publishes nothing on `tx_receipts`: no receipt and no boundary.
#[test]
fn an_executor_with_the_receipts_off_publishes_nothing() {
    let (on, paid_on) = OneBlock { publish: true }.run();
    assert!(
        on.iter().any(|m| matches!(m, CMessage::Receipt(_))),
        "on: the receipt goes out"
    );
    assert!(
        on.iter().any(|m| matches!(m, CMessage::BlockBoundary(_))),
        "on: the boundary goes out"
    );
    let (off, paid_off) = OneBlock { publish: false }.run();
    assert!(
        off.is_empty(),
        "off: nothing on tx_receipts: {} messages",
        off.len()
    );
    assert_eq!(paid_off, U256::from(7), "off: the block commits");
    assert_eq!(paid_off, paid_on);
}
