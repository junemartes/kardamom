//! The pipelined session shapes: speculative release, deferred
//! binding, and mv-cache layering, with wound-and-recover races.
#![allow(
    clippy::cast_possible_truncation,
    reason = "fixture indices (signer count, tx count) stay far below u8/u32::MAX in every test"
)]

mod common;

use alloy_primitives::TxKind;
use common::*;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_footprint::classifier::Stats;
use kardamom_stm::execute::execute_block_sequential;

/// Streaming release: `submit_streaming` ships the folded delta
/// before receipts, and in speculative mode, before the validation
/// verdict. Invariants pinned here, for each repetition and mode:
/// - wounds == 0 means exactly one release, not corrected;
/// - wounds > 0 means speculative mode sends two releases (a stale
///   speculative one, then a corrected one); conservative mode sends
///   one, already final;
/// - the last release always byte-equals the outcome's delta;
/// - receipts and the delta stay byte-identical to sequential execution
///   in every case.
///
/// The lying-stats generator makes wounds fire across repetitions, so
/// the correction leg is tested for real, not just in theory.

#[test]
fn streaming_release_and_wound_correction() {
    let sg = signers(4);
    let database = db(&sg);
    let lying_stats = lying_stats();
    let envs = vec![
        tx(&sg[0], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[1], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[2], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(&sg[3], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
    ];
    let recs = records(envs);
    let seq = execute_block_sequential(&database, None, env(), &recs).unwrap();

    for speculative in [true, false] {
        // Hunt for the wound leg: races are timing-dependent, and a fast
        // machine may win every one, as noted in the sibling lying-stats
        // test. Repeat until a wound appears or the budget runs out,
        // asserting the protocol on every repetition either way.
        let (wound_attempts, attempts) = hunt_wounds(200, 24, |rep| {
            let (out, releases) = kardamom_stm::execute::with_pool(
                kardamom_stm::execute::PoolConfig {
                    workers: nz(4),
                    ..Default::default()
                },
                |pool| {
                    let mut sess = pool
                        .begin_block(database.clone(), PendingDelta::new(), env(), &lying_stats)
                        .unwrap();
                    feed(&mut sess, &recs);
                    let (dtx, drx) = std::sync::mpsc::channel();
                    let ticket = sess.submit_streaming(dtx, speculative).unwrap();
                    let out = ticket.wait().unwrap();
                    let releases: Vec<kardamom_stm::execute::DeltaRelease> =
                        drx.try_iter().collect();
                    (out, releases)
                },
            );
            let label = format!("streaming spec={speculative} rep={rep}");
            assert_identical(&seq, &out.receipts, &out.delta, &label);
            let wounded = out.wounds > 0;
            if !wounded {
                assert_eq!(releases.len(), 1, "{label}: clean block, one release");
                assert!(!releases[0].corrected, "{label}: clean release");
            } else if speculative {
                assert_eq!(
                    releases.len(),
                    2,
                    "{label}: wound must re-issue the release"
                );
                assert!(!releases[0].corrected);
                assert!(releases[1].corrected, "{label}: second release corrects");
            } else {
                assert_eq!(
                    releases.len(),
                    1,
                    "{label}: conservative releases once, post-verdict"
                );
                assert!(!releases[0].corrected);
            }
            let last = releases.last().expect("at least one release");
            assert_delta_eq(
                &last.delta,
                &out.delta,
                &format!("{label}: final release delta == outcome delta"),
            );
            wounded
        });
        // Not asserted, since a fast machine may win every race, but loud
        // when the correction leg never ran.
        eprintln!("streaming spec={speculative}: {wound_attempts}/{attempts} attempts wounded");
    }
}

/// The speculative-release adversarial case: block 2 is
/// built, fed, and submitted with deferred layers while block 1 still
/// executes. It binds on block 1's speculative release and runs while
/// block 1 is still validating. When the lying-stats race fires a wound
/// in block 1, the speculative release was wrong: the consumer aborts
/// block 2, rebuilds it on the `corrected` release, and both blocks must
/// come out byte-identical to the sequential chain. When no wound fires,
/// block 2's speculative run is the answer, also asserted identical.
/// Either way, this is what makes the gamble sound: the bytes match.

#[test]
fn speculative_pipeline_wound_aborts_and_recovers() {
    let sg = signers(4);
    let database = db(&sg);
    let lying_stats = lying_stats();
    // Block 1: the wound-prone racing increments. Block 2: four more
    // increments from the same senders. Every one of its receipts
    // depends on block 1's final counter value, so a stale layer cannot
    // pass the byte-identical assert.
    let recs1 = records(counter_block(&sg, 0));
    let recs2 = records(counter_block(&sg, 1));
    let seq1 = execute_block_sequential(&database, None, env(), &recs1).unwrap();
    let env2 = env_at(2);
    let seq2 = execute_block_sequential(&database, Some(&seq1.1), env2, &recs2).unwrap();

    let (wound_reps, reps) = hunt_wounds(200, 24, |rep| {
        let label = format!("spec-pipeline rep={rep}");
        kardamom_stm::execute::with_pool(
            kardamom_stm::execute::PoolConfig {
                workers: nz(4),
                ..Default::default()
            },
            |pool| {
                let submit_block2 = |layer: std::sync::Arc<PendingDelta>| {
                    let mut sess = pool
                        .begin_block_layered(
                            vec![database.clone(); 4],
                            PendingDelta::new(),
                            vec![layer],
                            env2,
                            &lying_stats,
                        )
                        .unwrap();
                    feed(&mut sess, &recs2);
                    let (d2tx, _d2rx) = std::sync::mpsc::channel();
                    sess.submit_streaming(d2tx, true).unwrap()
                };
                // Block 1: streaming speculative.
                let mut sess1 = pool
                    .begin_block(database.clone(), PendingDelta::new(), env(), &lying_stats)
                    .unwrap();
                feed(&mut sess1, &recs1);
                let (d1tx, d1rx) = std::sync::mpsc::channel();
                let ticket1 = sess1.submit_streaming(d1tx, true).unwrap();
                // The production sequencing (late-bound layers): block 2
                // is built, fed, and submitted while block 1 still
                // executes, before its read base exists. Workers wait
                // for the bind.
                let (mut sess2, binder2) = pool
                    .begin_block_deferred(
                        vec![database.clone(); 4],
                        PendingDelta::new(),
                        env2,
                        &lying_stats,
                    )
                    .unwrap();
                feed(&mut sess2, &recs2);
                let (d2tx, _d2rx) = std::sync::mpsc::channel();
                let ticket2 = sess2.submit_streaming(d2tx, true).unwrap();
                // The speculative release: block 1 is still validating.
                let rel1 = d1rx.recv().expect("speculative release");
                assert!(!rel1.corrected, "{label}: first release is speculative");
                binder2
                    .bind(kardamom_stm::execute::ReadBase::Deltas(vec![
                        rel1.delta.clone(),
                    ]))
                    .unwrap();
                // Block 1's verdict.
                let out1 = ticket1.wait().unwrap();
                assert_identical(&seq1, &out1.receipts, &out1.delta, &label);
                let wounded = out1.wounds > 0;
                if out1.wounds == 0 {
                    // The gamble held: block 2's speculative run is final.
                    let out2 = ticket2.wait().unwrap();
                    assert_identical(&seq2, &out2.receipts, &out2.delta, &label);
                    assert!(d1rx.try_recv().is_err(), "{label}: no extra release");
                } else {
                    // The release was wrong. Unwind block 2 entirely:
                    // abort, discard its ticket (Ok or Err, either is
                    // garbage), and rebuild on the corrected delta.
                    let corrected = d1rx.recv().expect("corrected release");
                    assert!(corrected.corrected, "{label}: wound re-issues");
                    pool.abort_active();
                    let _ = ticket2.wait();
                    let ticket2b = submit_block2(corrected.delta.clone());
                    let out2 = ticket2b.wait().unwrap();
                    assert_identical(&seq2, &out2.receipts, &out2.delta, &label);
                }
                wounded
            },
        )
    });
    eprintln!("spec-pipeline: {wound_reps}/{reps} reps wounded");
}

/// A deferred session whose consumer never binds must not hang: abort
/// resolves its ticket (error or stale Ok), loudly and promptly.

#[test]
fn deferred_never_bound_aborts_cleanly() {
    let sg = signers(2);
    let database = db(&sg);
    let recs = records(vec![
        tx(&sg[0], 0, TxKind::Call(sg[1].address()), 5, &[]),
        tx(&sg[1], 0, TxKind::Call(sg[0].address()), 3, &[]),
    ]);
    kardamom_stm::execute::with_pool(
        kardamom_stm::execute::PoolConfig {
            workers: nz(2),
            ..Default::default()
        },
        |pool| {
            let stats = Stats::default();
            let (mut sess, _binder) = pool
                .begin_block_deferred(
                    vec![database.clone(); 2],
                    PendingDelta::new(),
                    env(),
                    &stats,
                )
                .unwrap();
            feed(&mut sess, &recs);
            let (dtx, _drx) = std::sync::mpsc::channel();
            let ticket = sess.submit_streaming(dtx, true).unwrap();
            pool.abort_active();
            let r = ticket.wait();
            assert!(
                r.is_err(),
                "never-bound block must resolve to the abort error"
            );
        },
    );
}

/// Mv-as-layer: block 2 binds on block 1's early release, the
/// mv cache itself, shipped before block 1's fold, hash, or validation
/// ran. A wound in block 1 means the corrected delta release arrives,
/// block 2 aborts, and it rebuilds on the delta layer, never the stale
/// mv. Byte-identical to the sequential chain on every path.

#[test]
fn mv_as_layer_pipeline_wound_aborts_and_recovers() {
    let sg = signers(4);
    let database = db(&sg);
    let lying_stats = lying_stats();
    let recs1 = records(counter_block(&sg, 0));
    let recs2 = records(counter_block(&sg, 1));
    let seq1 = execute_block_sequential(&database, None, env(), &recs1).unwrap();
    let env2 = env_at(2);
    let seq2 = execute_block_sequential(&database, Some(&seq1.1), env2, &recs2).unwrap();

    let (wound_reps, reps) = hunt_wounds(200, 24, |rep| {
        let label = format!("mv-layer rep={rep}");
        kardamom_stm::execute::with_pool(
            kardamom_stm::execute::PoolConfig {
                workers: nz(4),
                ..Default::default()
            },
            |pool| {
                // Block 1: early mv release, then fold delta release.
                let mut sess1 = pool
                    .begin_block(database.clone(), PendingDelta::new(), env(), &lying_stats)
                    .unwrap();
                feed(&mut sess1, &recs1);
                let (mv1_tx, mv1_rx) = std::sync::mpsc::channel();
                let (d1tx, d1rx) = std::sync::mpsc::channel();
                let ticket1 = sess1.submit_streaming_mv(mv1_tx, d1tx).unwrap();
                // Block 2: deferred, submitted before block 1's release.
                let (mut sess2, binder2) = pool
                    .begin_block_deferred(
                        vec![database.clone(); 4],
                        PendingDelta::new(),
                        env2,
                        &lying_stats,
                    )
                    .unwrap();
                feed(&mut sess2, &recs2);
                let (d2tx, _d2rx) = std::sync::mpsc::channel();
                let ticket2 = sess2.submit_streaming(d2tx, true).unwrap();
                // The early release: block 1's mv, before the fold and
                // before the verdict.
                let rel1 = mv1_rx.recv().expect("early mv release");
                binder2
                    .bind(kardamom_stm::execute::ReadBase::Mv {
                        layers: vec![rel1.mv.clone()],
                        sink_final: rel1.sink_final.clone(),
                        deltas: Vec::new(),
                    })
                    .unwrap();
                let out1 = ticket1.wait().unwrap();
                assert_identical(&seq1, &out1.receipts, &out1.delta, &label);
                let wounded = out1.wounds > 0;
                if out1.wounds == 0 {
                    let out2 = ticket2.wait().unwrap();
                    assert_identical(&seq2, &out2.receipts, &out2.delta, &label);
                } else {
                    // The mv layer is stale. Unwind block 2 onto the
                    // corrected delta.
                    let first = d1rx.recv().expect("first delta release");
                    let corrected = if first.corrected {
                        first
                    } else {
                        d1rx.recv().expect("corrected delta release")
                    };
                    assert!(corrected.corrected, "{label}: wound re-issues");
                    pool.abort_active();
                    let _ = ticket2.wait();
                    let mut sess = pool
                        .begin_block_layered(
                            vec![database.clone(); 4],
                            PendingDelta::new(),
                            vec![corrected.delta.clone()],
                            env2,
                            &lying_stats,
                        )
                        .unwrap();
                    feed(&mut sess, &recs2);
                    let (d2btx, _d2brx) = std::sync::mpsc::channel();
                    let out2 = sess.submit_streaming(d2btx, true).unwrap().wait().unwrap();
                    assert_identical(&seq2, &out2.receipts, &out2.delta, &label);
                }
                wounded
            },
        )
    });
    eprintln!("mv-layer: {wound_reps}/{reps} reps wounded");
}
