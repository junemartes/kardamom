//! Property tests over [`kardamom_sealer::election::elect`].
//!
//! Properties asserted:
//! - **P1 determinism:** same inputs always yield the same winner.
//! - **P2 min-id eligibility:** if `elect` returns `Some(h)`, no recorder
//!   with id < h satisfied both predicates (lag <= threshold AND fresh).
//! - **P3 winner satisfies the predicates:** the returned winner is itself
//!   eligible (sanity: we don't return an ineligible candidate).

use kardamom_sealer::election::{
    bpos_to_abs, CaughtUpSet, RecorderState, elect,
};
use kardamom_types::BPosition;
use proptest::prelude::*;

prop_compose! {
    fn arb_recorder()(
        recorder_id in 1u8..200,
        term in 0i32..10,
        off in 0i32..1_000_000,
        last_seen_ms in 0u64..1_000_000,
    ) -> RecorderState {
        RecorderState {
            recorder_id,
            fsynced: BPosition { term_id: term, term_offset: off },
            last_seen_ms,
        }
    }
}

proptest! {
    #[test]
    fn deterministic(
        recs in proptest::collection::vec(arb_recorder(), 0..20),
        cur_term in 0i32..10,
        cur_off in 0i32..1_000_000,
        now_ms in 0u64..2_000_000,
        lag in 0u64..1_000_000,
        stale in 0u64..1_000_000,
    ) {
        let set = CaughtUpSet::from_iter(recs);
        let cur = BPosition { term_id: cur_term, term_offset: cur_off };
        let a = elect(&set, cur, now_ms, lag, stale);
        let b = elect(&set, cur, now_ms, lag, stale);
        prop_assert_eq!(a, b);
    }

    #[test]
    fn winner_is_min_id_among_eligible(
        recs in proptest::collection::vec(arb_recorder(), 1..20),
        cur_term in 0i32..10,
        cur_off in 0i32..1_000_000,
        now_ms in 0u64..2_000_000,
        lag in 0u64..1_000_000,
        stale in 0u64..1_000_000,
    ) {
        let set = CaughtUpSet::from_iter(recs);
        let cur = BPosition { term_id: cur_term, term_offset: cur_off };
        if let Some(winner) = elect(&set, cur, now_ms, lag, stale) {
            let cur_abs = bpos_to_abs(cur);
            // The CaughtUpSet only retains one state per recorder_id (the
            // last one inserted). All assertions must work against `set`,
            // not the raw input vec.
            let winner_state = set.states()
                .find(|r| r.recorder_id == winner)
                .copied()
                .expect("winner must be in the set");
            let w_lag = cur_abs - bpos_to_abs(winner_state.fsynced);
            let w_fresh = now_ms.saturating_sub(winner_state.last_seen_ms) <= stale;
            prop_assert!(w_lag <= lag as i64);
            prop_assert!(w_fresh);

            // No eligible recorder has a smaller id than the winner.
            for r in set.states() {
                if r.recorder_id < winner {
                    let lag_b = cur_abs - bpos_to_abs(r.fsynced);
                    let caught_up = lag_b <= lag as i64;
                    let fresh = now_ms.saturating_sub(r.last_seen_ms) <= stale;
                    prop_assert!(!(caught_up && fresh),
                        "recorder {} (lag={}, fresh={}) was eligible but did not win", r.recorder_id, lag_b, fresh);
                }
            }
        }
    }
}
