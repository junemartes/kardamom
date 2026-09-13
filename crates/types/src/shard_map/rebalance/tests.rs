use super::*;

#[test]
fn every_resize_is_balanced_and_moves_only_the_surplus() {
    (1..=8).for_each(|source| {
        let current = ShardMap::identity(1).unwrap().rebalance(source).unwrap();
        (1..=8).for_each(|target| check_resize(&current, target));
    });
}

fn check_resize(current: &ShardMap, target: u32) {
    let next = current.rebalance(target).unwrap();
    let count = usize::try_from(target).unwrap();
    let want = |lane: usize| {
        if lane < count {
            256 / count + usize::from(lane < 256 % count)
        } else {
            0
        }
    };
    let surplus: usize = (0..LANE_COUNT)
        .map(|lane| {
            current
                .vslot_set(lane)
                .len()
                .saturating_sub(want(usize::from(lane)))
        })
        .sum();
    assert_eq!(next.version(), current.version() + 1);
    assert_eq!(next, current.rebalance(target).unwrap());
    assert_eq!(
        next.table()
            .iter()
            .zip(current.table())
            .filter(|(a, b)| a != b)
            .count(),
        surplus
    );
    (0..LANE_COUNT)
        .for_each(|lane| assert_eq!(next.vslot_set(lane).len(), want(usize::from(lane))));
}

#[test]
fn scale_out_preserves_the_existing_assignment_order() {
    let current = ShardMap::identity(2).unwrap();
    let next = current.rebalance(3).unwrap();
    assert!(next.table()[..84].iter().all(|lane| *lane == 2));
    assert_eq!(next.table()[84], 0);
    assert_eq!(next.table()[85], 2);
    assert_eq!(&next.table()[86..], &current.table()[86..]);
}

#[test]
fn invalid_inputs_and_version_overflow_fail() {
    let current = ShardMap::identity(2).unwrap();
    assert!(current.rebalance(0).is_err());
    assert!(current.rebalance(9).is_err());
    assert!(ShardMap::from_table(0, [8; 256]).rebalance(2).is_err());
    assert_eq!(
        ShardMap::from_table(u32::MAX, [0; 256]).rebalance(2),
        Err(ShardMapError::VersionExhausted)
    );
}
