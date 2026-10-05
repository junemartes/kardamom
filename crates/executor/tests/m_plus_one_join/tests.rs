use super::*;

#[test]
fn m4_canonical_b_order_drives_receipts() {
    const M: u8 = 4;
    const TXS_PER_SEQ: u64 = 50;
    const TOTAL: u64 = (M as u64) * TXS_PER_SEQ;

    // M signers, one per sequencer. Each publishes its own nonce-stream of
    // transfers. There are no inter-signer dependencies, so the executor's
    // sequential revm path can reorder them freely. The test checks one
    // constraint: receipts come out in tx_ordering canonical order.
    let (signers, snap) = fund_m_signers(M);
    let to = address!("00000000000000000000000000000000DEADBEEF");

    let bus = FakeBus::new();
    let M1Bus {
        a_pubs,
        tx_data_subs: tx_data_sub_handles,
        b_pub,
        tx_ordering_sub: tx_ordering_sub_handle,
    } = open_m_plus_one_bus(&bus, M);

    // Phase 1: every sequencer publishes its envelopes onto tx_data.
    let plan = publish_tx_data_plan(&a_pubs, &signers, to, M, TXS_PER_SEQ);
    assert_eq!(plan.len(), TOTAL as usize);

    // Phase 2: interleave the per-A plan into an arbitrary canonical
    // order, and publish refs and the closing boundary onto tx_ordering.
    let shuffled = shuffle_canonical_order(&plan, M, 0xD15C0_u64);
    publish_ordering_and_boundary(&b_pub, &shuffled, plan.len());

    // Phase 3: run the executor. The subscription adapters spin until
    // the test sets `closed=true` after the bus is fully drained.
    let RunHandles { c_rx, join } =
        spawn_m_plus_one_executor(M, snap, tx_data_sub_handles, tx_ordering_sub_handle);

    let deadline = Instant::now() + Duration::from_secs(30);
    let (got_hashes, boundaries) = collect_until_boundary(&c_rx, deadline);

    join.join().expect("no panic").expect("executor ok");

    assert_eq!(
        got_hashes.len() as u64,
        TOTAL,
        "expected {TOTAL} receipts, got {}",
        got_hashes.len()
    );
    assert_eq!(boundaries, 1);

    let expected: Vec<alloy_primitives::B256> = shuffled.iter().map(|r| r.hash).collect();
    assert_eq!(
        got_hashes, expected,
        "receipts must be in tx_ordering canonical order"
    );
}

#[test]
fn tx_ref_arriving_before_envelope_still_joins() {
    // Single-sequencer mini-scenario: publish the ref onto tx_ordering
    // immediately, and delay the envelope on tx_data by about 30 ms. The
    // join buffer's bounded wait should pick it up well within the
    // default 100 ms timeout.

    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            KECCAK_EMPTY,
        )
        .build();

    let bus = FakeBus::new();
    let tx_data_sub_handle = FakeTxDataSubscription::open(&bus, "aeron:ipc?alias=a-0", 2000);
    let b_pub = FakeTxOrderingPublication::open(&bus, "aeron:ipc?alias=b", 1001);
    let tx_ordering_sub_handle = FakeTxOrderingSubscription::open(&bus, "aeron:ipc?alias=b", 1001);

    let env = transfer(&signer, 0, to);
    let expected_hash = env.tx_hash;

    // Schedule the tx_data publish on a background thread so the
    // ref-then-envelope order is genuine.
    let bus_clone = bus.clone();
    let env_clone = env.clone();
    let a_inserter = thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        let a_pub_late = FakeTxDataPublication::open(&bus_clone, 0, "aeron:ipc?alias=a-0", 2000);
        let _ = a_pub_late.publish(&env_clone).expect("publish A late");
    });

    // Meanwhile, stake out immediately the tx_data_position that the ref
    // will claim. The fake's `publish` advances `next_offset` by the
    // payload length, so the test needs to know what `BPosition` the A
    // publish will land at. The fake bus is fresh, so the first
    // envelope's start position is 0: `BPosition { term_id: 0,
    // term_offset: 0 }`.
    //
    // (If the fake's offset convention changes, the test could instead
    // check what the A publish returned. This test needs a
    // deterministic position to reference before the publish happens,
    // so it relies on the fake's well-defined zero-init.)

    let tx_data_position = BPosition {
        term_id: 0,
        term_offset: 0,
    };
    b_pub
        .publish_ref(&TxRef::new(
            alloy_primitives::B256::ZERO,
            0,
            tx_data_position,
            0,
        ))
        .expect("publish ref");
    b_pub
        .publish_boundary(&BlockBoundaryStart {
            block_number: 1,
            // One canonical record applied, so the cumulative count is 1.
            end_tx_idx: BPosition::from_index(1),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        })
        .expect("publish boundary");

    let writer_q = WriterApplyingQueue::new(snap.clone());
    let snapshots = MutatingSnapshotSource(snap);

    let a_closed = Arc::new(AtomicBool::new(false));
    let b_closed = Arc::new(AtomicBool::new(false));
    let tx_data_subs = vec![FakeTxDataSubAdapter {
        sequencer_id: 0,
        sub: tx_data_sub_handle,
        closed: a_closed.clone(),
    }];
    let tx_ordering_sub = FakeTxOrderingSubAdapter {
        sub: tx_ordering_sub_handle,
        closed: b_closed.clone(),
    };
    let (c_tx, c_rx) = bounded::<CMessage>(8);

    // After the inserter has had time to fire, and the executor has had
    // time to consume, signal "EOF" on both subscriptions, so the
    // executor returns. This gives a generous 500 ms.
    let a_closed_signaler = a_closed.clone();
    let b_closed_signaler = b_closed.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(500));
        a_closed_signaler.store(true, Ordering::Release);
        b_closed_signaler.store(true, Ordering::Release);
    });

    // Give the tx_ordering reader's join wait enough headroom even on slow CI.
    let cfg = ExecutorConfig {
        chain_id: NonZeroU64::MIN,
        receipt_queue_depth: QUEUE_DEPTH_8,
        reader: ReaderConfig {
            join_timeout: Duration::from_millis(500),
            join_poll_interval: Duration::from_micros(100),
            ..ReaderConfig::default()
        },
        ..ExecutorConfig::default()
    };

    let join = thread::spawn(move || {
        Executor::<Wiring>::new(
            cfg,
            Inbound {
                tx_data: tx_data_subs,
                tx_ordering: tx_ordering_sub,
                join_recovery: None,
            },
            Outbound {
                tx_receipts: ChanReceiptsPub(c_tx),
                snapshots,
                writer_signal: Imm,
                writer_queue: writer_q,
            },
            ResumePoint::GENESIS,
            RoleHooks::none(),
        )
        .run()
    });

    let (got_hashes, boundaries) = collect_single_seq_until_boundary(&c_rx);

    a_inserter.join().unwrap();
    join.join().expect("no panic").expect("executor ok");
    assert_eq!(got_hashes, vec![expected_hash]);
    assert_eq!(boundaries, 1);
}

/// Poll one message from `c_rx`. A receipt appends its hash to
/// `got_hashes`; a boundary increments `boundaries`. Unlike
/// [`poll_one_c_message`], this does not assert the receipt status
/// (the single-tx caller has its own, coarser pass/fail check on the
/// final hash list) and counts boundaries as `u32`, not `usize`.
fn poll_one_single_seq(
    c_rx: &crossbeam_channel::Receiver<CMessage>,
    got_hashes: &mut Vec<alloy_primitives::B256>,
    boundaries: &mut u32,
) -> PollOutcome {
    let Ok(m) = c_rx.recv_timeout(Duration::from_secs(5)) else {
        return PollOutcome::Stop;
    };
    match m {
        CMessage::Receipt(r) => {
            got_hashes.push(r.tx_hash);
            PollOutcome::Continue
        }
        CMessage::BlockBoundary(_) => {
            *boundaries += 1;
            PollOutcome::Stop
        }
    }
}

/// Collect receipts from `c_rx` until the block boundary lands, or the
/// channel times out or closes. Returns the received hashes, in
/// arrival order, and the boundary count.
fn collect_single_seq_until_boundary(
    c_rx: &crossbeam_channel::Receiver<CMessage>,
) -> (Vec<alloy_primitives::B256>, u32) {
    let mut got_hashes = Vec::new();
    let mut boundaries = 0u32;
    while let PollOutcome::Continue = poll_one_single_seq(c_rx, &mut got_hashes, &mut boundaries) {}
    (got_hashes, boundaries)
}
