//! HotStandbyTailer replay correctness.

use alloy_primitives::Address;
use kardamom_sequencer::config::{SequencerConfig, SequencerRole};
use kardamom_sequencer::inbound::BMessage;
use kardamom_sequencer::inbound::fakes::ScriptedB;
use kardamom_sequencer::partition::partition_for;
use kardamom_sequencer::standby::HotStandbyTailer;

#[test]
fn standby_replays_only_its_slice() {
    let cfg = SequencerConfig {
        partition_count: 8,
        partition_index: 3,
        role: SequencerRole::Standby,
        ..Default::default()
    };
    let mut tailer = HotStandbyTailer::new(cfg.clone());
    let mut b = ScriptedB::default();

    let mut in_slice: Option<Address> = None;
    let mut out_slice: Option<Address> = None;
    for i in 0u8..255 {
        let a = Address::repeat_byte(i);
        let p = partition_for(a, cfg.partition_count);
        if p == cfg.partition_index && in_slice.is_none() {
            in_slice = Some(a);
        } else if p != cfg.partition_index && out_slice.is_none() {
            out_slice = Some(a);
        }
        if in_slice.is_some() && out_slice.is_some() {
            break;
        }
    }
    let in_a = in_slice.expect("found in-slice address");
    let out_a = out_slice.expect("found out-of-slice address");

    b.queue.push_back(BMessage::Tx {
        sender: in_a,
        nonce: 0,
    });
    b.queue.push_back(BMessage::Tx {
        sender: out_a,
        nonce: 0,
    });
    b.queue.push_back(BMessage::Tx {
        sender: in_a,
        nonce: 1,
    });
    b.queue.push_back(BMessage::BlockBoundary);
    b.queue.push_back(BMessage::Tx {
        sender: in_a,
        nonce: 2,
    });

    while tailer.run_once(&mut b).unwrap() {}

    assert_eq!(tailer.next_nonce(in_a), 3);
    assert_eq!(
        tailer.next_nonce(out_a),
        0,
        "out-of-slice should be ignored"
    );
}

#[test]
fn standby_block_boundary_does_not_affect_nonce() {
    let cfg = SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        ..Default::default()
    };
    let mut tailer = HotStandbyTailer::new(cfg);
    let mut b = ScriptedB::default();
    let s = Address::repeat_byte(1);
    b.queue.push_back(BMessage::Tx {
        sender: s,
        nonce: 0,
    });
    b.queue.push_back(BMessage::BlockBoundary);
    b.queue.push_back(BMessage::BlockBoundary);
    b.queue.push_back(BMessage::Tx {
        sender: s,
        nonce: 1,
    });
    while tailer.run_once(&mut b).unwrap() {}
    assert_eq!(tailer.next_nonce(s), 2);
}
