//! Bootstrap integration test against `log::testing::FakeBus`.
//!
//! Publishes `BlockBoundaryStart`s on the boundary stream; the bootstrap
//! drain helper should return `max + 1`.

use log::testing::{FakeBus, FakePublication, FakeTypedSubscription};
use sealer::bootstrap::next_block_number_from_iter;
use types::{BPosition, BlockBoundaryStart};

const CHANNEL: &str = "aeron:udp?endpoint=fake:0";
const BOUNDARY_STREAM: i32 = 9001;

fn bs(n: u64, off: i32) -> BlockBoundaryStart {
    BlockBoundaryStart {
        block_number: n,
        end_tx_idx: BPosition {
            term_id: 0,
            term_offset: off,
        },
        l2_timestamp: 250 * n,
    }
}

fn drain(sub: &mut FakeTypedSubscription<BlockBoundaryStart>) -> Vec<BlockBoundaryStart> {
    let mut out = Vec::new();
    loop {
        let mut got = 0;
        let delivered = sub.poll(
            |v: BlockBoundaryStart, _pos| {
                out.push(v);
                got += 1;
            },
            64,
        );
        if delivered == 0 || got == 0 {
            break;
        }
    }
    out
}

#[test]
fn bootstrap_reads_max_block_number_from_tail() {
    let bus = FakeBus::new();
    let pubh = FakePublication::open(&bus, CHANNEL, BOUNDARY_STREAM);
    pubh.publish(&bs(100, 1_000)).unwrap();
    pubh.publish(&bs(101, 2_000)).unwrap();

    let mut sub: FakeTypedSubscription<BlockBoundaryStart> =
        FakeTypedSubscription::open(&bus, CHANNEL, BOUNDARY_STREAM);
    let scanned = drain(&mut sub);
    assert_eq!(scanned.len(), 2);
    assert_eq!(next_block_number_from_iter(scanned), 102);
}

#[test]
fn bootstrap_empty_tail_is_genesis() {
    let bus = FakeBus::new();
    let mut sub: FakeTypedSubscription<BlockBoundaryStart> =
        FakeTypedSubscription::open(&bus, CHANNEL, BOUNDARY_STREAM);
    let scanned = drain(&mut sub);
    assert!(scanned.is_empty());
    assert_eq!(next_block_number_from_iter(scanned), 1);
}
