use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_types::*;

fn roundtrip<T>(value: &T) -> T
where
    T: rkyv::Archive
        + for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
    T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(value).unwrap();
    rkyv::from_bytes::<T, rkyv::rancor::Error>(&bytes).unwrap()
}

#[test]
fn bposition_orders_by_term_then_offset() {
    let a = BPosition {
        term_id: 1,
        term_offset: 100,
    };
    let b = BPosition {
        term_id: 1,
        term_offset: 200,
    };
    let c = BPosition {
        term_id: 2,
        term_offset: 0,
    };
    assert!(a < b);
    assert!(b < c);
    assert_eq!(
        a,
        BPosition {
            term_id: 1,
            term_offset: 100
        }
    );
}

#[test]
fn tx_envelope_roundtrip() {
    let v = TxEnvelope {
        correlation_id: 0xDEAD_BEEF,
        raw_tx: Bytes::from_static(b"hello"),
        sender: Address::repeat_byte(0x11),
        tx_hash: B256::repeat_byte(0x22),
    };
    let back = roundtrip(&v);
    assert_eq!(v.correlation_id, back.correlation_id);
    assert_eq!(v.raw_tx, back.raw_tx);
    assert_eq!(v.sender, back.sender);
    assert_eq!(v.tx_hash, back.tx_hash);
}

#[test]
fn receipt_roundtrip() {
    let v = Receipt {
        tx_type: kardamom_types::TX_TYPE_LEGACY,
        tx_idx: BPosition {
            term_id: 3,
            term_offset: 4096,
        },
        tx_hash: B256::repeat_byte(0x44),
        status: true,
        gas_used: 21_000,
        logs: vec![WireLog {
            address: Address::repeat_byte(0x55),
            topics: vec![B256::repeat_byte(0x66)],
            data: Bytes::from_static(b"log-data"),
        }],
        write_set_hash: B256::repeat_byte(0xAB),
        nonce: 7,
        from: Address::repeat_byte(0x77),
        to: Some(Address::repeat_byte(0x88)),
        contract_address: None,
        effective_gas_price: 1_000_000_000_000,
        block_number: 42,
        transaction_index: 3,
        cumulative_gas_used: 84_000,
    };
    assert_eq!(roundtrip(&v), v);
}

#[test]
fn receipt_roundtrip_contract_creation() {
    // CREATE tx variant: `to` is None, `contract_address` is Some.
    let v = Receipt {
        tx_type: kardamom_types::TX_TYPE_LEGACY,
        tx_idx: BPosition {
            term_id: 0,
            term_offset: 0,
        },
        tx_hash: B256::repeat_byte(0x11),
        status: true,
        gas_used: 100_000,
        logs: vec![],
        write_set_hash: B256::ZERO,
        nonce: 0,
        from: Address::repeat_byte(0x22),
        to: None,
        contract_address: Some(Address::repeat_byte(0x33)),
        effective_gas_price: 0,
        block_number: 1,
        transaction_index: 0,
        cumulative_gas_used: 100_000,
    };
    assert_eq!(roundtrip(&v), v);
}

#[test]
fn boundary_roundtrip() {
    let start = BlockBoundaryStart {
        block_number: 7,
        end_tx_idx: BPosition {
            term_id: 1,
            term_offset: 999,
        },
        l2_timestamp: 1_700_000_000,
    };
    assert_eq!(roundtrip(&start), start);

    // BlockBoundary has NO state_root_commitment field.
    let end = BlockBoundary {
        block_number: 7,
        end_tx_idx: BPosition {
            term_id: 1,
            term_offset: 999,
        },
        l2_timestamp: 1_700_000_000,
    };
    assert_eq!(roundtrip(&end), end);
}

#[test]
fn watermark_roundtrip() {
    let w = FsyncWatermark {
        recorder_id: 2,
        position: BPosition {
            term_id: 4,
            term_offset: 1024,
        },
    };
    assert_eq!(roundtrip(&w), w);

    let q = QuorumWatermark {
        position: BPosition {
            term_id: 4,
            term_offset: 1024,
        },
    };
    assert_eq!(roundtrip(&q), q);
}

#[test]
fn tx_error_roundtrip() {
    let e = TxError {
        sender: Address::repeat_byte(0x55),
        nonce: 7,
        reason: TxErrorReason::DuplicatedTx { expected_nonce: 12 },
    };
    assert_eq!(roundtrip(&e), e);
}

#[test]
fn deposit_roundtrip_call() {
    let d = Deposit {
        source_hash: B256::repeat_byte(0xAA),
        from: Address::repeat_byte(0x11),
        to: Some(Address::repeat_byte(0x22)),
        mint: 1_000_000_000_000u128,
        value: alloy_primitives::U256::from(500_000_000_000u128),
        gas_limit: 200_000,
        is_system_transaction: false,
        input: Bytes::from_static(b"\xde\xad\xbe\xef"),
    };
    assert_eq!(roundtrip(&d), d);
}

#[test]
fn deposit_roundtrip_create() {
    let d = Deposit {
        source_hash: B256::repeat_byte(0xBB),
        from: Address::repeat_byte(0x33),
        to: None,
        mint: 0,
        value: alloy_primitives::U256::ZERO,
        gas_limit: 100_000,
        is_system_transaction: false,
        input: Bytes::new(),
    };
    assert_eq!(roundtrip(&d), d);
}

#[test]
fn deposit_ref_roundtrip() {
    let r = DepositRef {
        source_hash: B256::repeat_byte(0xCD),
        deposit_position: BPosition {
            term_id: 2,
            term_offset: 4096,
        },
    };
    assert_eq!(roundtrip(&r), r);
}

#[test]
fn tx_ordering_message_deposit_ref_variant_roundtrips() {
    let r = DepositRef {
        source_hash: B256::repeat_byte(0x99),
        deposit_position: BPosition {
            term_id: 0,
            term_offset: 64,
        },
    };
    let msg = TxOrderingMessage::DepositRef(r);
    assert_eq!(roundtrip(&msg), msg);
    assert!(msg.is_deposit_ref());
    assert_eq!(msg.as_deposit_ref(), Some(&r));
}

#[test]
fn tx_ref_roundtrip() {
    let r = TxRef {
        tx_hash: alloy_primitives::B256::ZERO,
        shard_id: 5,
        tx_data_position: BPosition {
            term_id: 3,
            term_offset: 4096,
        },
        tx_data_session_id: 0,
    };
    assert_eq!(roundtrip(&r), r);
}

#[test]
fn channel_b_message_tx_ref_roundtrip() {
    let m = TxOrderingMessage::TxRef(TxRef {
        tx_hash: alloy_primitives::B256::ZERO,
        shard_id: 1,
        tx_data_position: BPosition {
            term_id: 2,
            term_offset: 1024,
        },
        tx_data_session_id: 0,
    });
    assert_eq!(roundtrip(&m), m);
}

#[test]
fn channel_b_message_boundary_roundtrip() {
    let m = TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
        block_number: 11,
        end_tx_idx: BPosition {
            term_id: 0,
            term_offset: 8192,
        },
        l2_timestamp: 1_700_000_000,
    });
    assert_eq!(roundtrip(&m), m);
}

#[test]
fn channel_b_message_helpers() {
    let r = TxRef {
        tx_hash: alloy_primitives::B256::ZERO,
        shard_id: 7,
        tx_data_position: BPosition {
            term_id: 0,
            term_offset: 16,
        },
        tx_data_session_id: 0,
    };
    let m: TxOrderingMessage = r.into();
    assert!(m.is_tx_ref());
    assert!(!m.is_boundary());
    assert_eq!(m.as_tx_ref(), Some(&r));
    assert!(m.as_boundary().is_none());

    let b: TxOrderingMessage = BlockBoundaryStart::default().into();
    assert!(b.is_boundary());
    assert!(!b.is_tx_ref());
}

#[test]
fn block_delta_roundtrip() {
    use alloy_primitives::U256;
    use kardamom_types::delta::CodeEntry;
    let d = BlockDelta {
        block_number: 99,
        accounts: vec![AccountChange {
            address: Address::repeat_byte(0x10),
            nonce: 1,
            balance: U256::from(123u64),
            code_hash: B256::repeat_byte(0x11),
        }],
        storage: vec![StorageChange {
            address: Address::repeat_byte(0x12),
            key: B256::repeat_byte(0x13),
            value: U256::from(456u64),
        }],
        code: vec![CodeEntry {
            code_hash: B256::repeat_byte(0x14),
            code: Bytes::from_static(b"\x60\x80\x60\x40"),
        }],
        receipts: vec![],
    };
    assert_eq!(roundtrip(&d), d);
}
