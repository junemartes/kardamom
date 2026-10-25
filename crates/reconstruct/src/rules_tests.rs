use alloy_primitives::{B256, U256, hex};
use kardamom_engine::{CanonicalEnd, ReplayBlock};
use kardamom_types::upgrades::{
    FEATURE_CARRY_WINDOW, FEATURE_SEALER_V4, FEATURE_SEALER_V5, GenesisFeatures,
};
use kardamom_types::{EpochRecord, epoch::UpgradeLog, epoch::upgrade_from_log};

use super::{Activation, Boundary, Outcome, SealerRules};

/// The time of the first block of the tests, in ms.
const T0: u64 = 1_700_000_000_000;
/// The block interval of the tests, in ms.
const TICK: u64 = 250;
/// The time of `2^64 - 1`: an activation that keeps its feature off.
const NEVER: u64 = u64::MAX;

/// The rebuilt blocks of a test chain: each block has the next time, and
/// leads with the next epoch when it has one.
#[derive(Default)]
struct Chain {
    blocks: Vec<ReplayBlock>,
    origin: u64,
    end: u64,
}

impl Chain {
    /// A block with `records` canonical records, led by the epoch at the
    /// next L1 origin with the upgrade transactions `upgrades` when
    /// `epoch` is set.
    fn block(mut self, epoch: bool, upgrades: &[(u64, u64)], records: u64) -> Self {
        let number = u64::try_from(self.blocks.len()).unwrap() + 1;
        let l1_epochs = if epoch {
            self.origin = if self.origin == 0 {
                40
            } else {
                self.origin + 1
            };
            vec![epoch_record(self.origin, upgrades)]
        } else {
            Vec::new()
        };
        self.end += records + u64::try_from(l1_epochs.len()).unwrap();
        self.blocks.push(ReplayBlock {
            block_number: number,
            l2_timestamp: T0 + number * TICK,
            canonical_end: Some(CanonicalEnd {
                end_tx_idx: self.end,
                l1_origin: self.origin,
            }),
            l1_epochs,
            remote_epochs: Vec::new(),
            txs: Vec::new(),
        });
        self
    }

    /// A block led by the next epoch with `upgrades`, and one record.
    fn epoch(self, upgrades: &[(u64, u64)]) -> Self {
        self.block(true, upgrades, 1)
    }

    /// A block of one record and no epoch.
    fn records(self) -> Self {
        self.block(false, &[], 1)
    }

    /// A block with no record.
    fn empty(self) -> Self {
        self.block(false, &[], 0)
    }

    fn rules(&self, features: GenesisFeatures) -> SealerRules {
        SealerRules::at_head(features, &self.blocks).unwrap()
    }
}

/// The epoch of L1 block `number` with an upgrade transaction for each
/// `(feature, time_ms)` of `upgrades`.
fn epoch_record(number: u64, upgrades: &[(u64, u64)]) -> EpochRecord {
    let deposits = upgrades
        .iter()
        .zip(0..)
        .map(|(&(feature, at), log_index)| {
            upgrade_from_log(&UpgradeLog {
                block_number: number,
                block_hash: B256::repeat_byte(0x11),
                log_index,
                feature_id: U256::from(feature),
                activation_timestamp: at,
            })
        })
        .collect();
    EpochRecord {
        l1_number: number,
        l1_hash: B256::repeat_byte(0x11),
        deposits,
    }
}

#[test]
fn a_chain_with_no_genesis_feature_holds_no_rules() {
    let rules = Chain::default()
        .epoch(&[])
        .records()
        .rules(GenesisFeatures::None);
    assert_eq!(
        (
            rules.version(),
            rules.carry_window(),
            rules.holds_activation()
        ),
        (3, false, false)
    );
    assert_eq!(rules, {
        let mut start = SealerRules::genesis(GenesisFeatures::None);
        start.sealed_origin = 40;
        start
    });
}

#[test]
fn each_genesis_set_starts_its_version() {
    let at = |features| {
        let rules = SealerRules::genesis(features);
        (
            rules.version(),
            rules.carry_window(),
            rules.holds_activation(),
        )
    };
    assert_eq!(at(GenesisFeatures::None), (3, false, false));
    assert_eq!(at(GenesisFeatures::SealerV4), (4, false, true));
    assert_eq!(at(GenesisFeatures::SealerV5), (5, false, true));
    assert_eq!(at(GenesisFeatures::CarryWindow), (5, true, true));
}

/// The rules section of a carry-born chain at origin 42: one genesis
/// activation of each feature, in the layout of the sealer snapshot.
#[test]
fn the_rules_of_a_carry_born_chain_have_the_snapshot_layout() {
    let rules = Chain::default()
        .epoch(&[])
        .epoch(&[])
        .epoch(&[])
        .rules(GenesisFeatures::CarryWindow);
    let genesis = format!("01{:016x}{:016x}", 0, 1);
    assert_eq!(
        hex::encode(rules.encode().collect::<Vec<_>>()),
        format!("05000000000000002a{genesis}{genesis}01{genesis}")
    );
}

/// Feature 3 switches at the boundary of the first block of its epoch.
/// Feature 4 in the same epoch comes while version 3 runs: `EARLY`.
#[test]
fn an_upgrade_follows_the_rules_of_the_switch() {
    let chain = Chain::default().epoch(&[]).records();
    let same_epoch = chain
        .epoch(&[(FEATURE_SEALER_V4, 0), (FEATURE_SEALER_V5, 0)])
        .records()
        .rules(GenesisFeatures::None);
    assert_eq!((same_epoch.version(), same_epoch.carry.0.len()), (4, 0));
    let later = Chain::default()
        .epoch(&[])
        .records()
        .epoch(&[(FEATURE_SEALER_V4, 0)])
        .records()
        .epoch(&[(FEATURE_SEALER_V5, 0)])
        .records()
        .epoch(&[(FEATURE_CARRY_WINDOW, 0)])
        .records()
        .rules(GenesisFeatures::None);
    assert_eq!((later.version(), later.carry_window()), (5, true));
}

/// A real activation replaces the genesis one: feature 5 off with a time
/// ahead, on at that time; feature 4 off runs version 4.
#[test]
fn a_real_activation_replaces_the_genesis_one() {
    let base = || Chain::default().epoch(&[]).records();
    let ahead = T0 + 6 * TICK;
    let off = base()
        .epoch(&[(FEATURE_CARRY_WINDOW, ahead)])
        .records()
        .rules(GenesisFeatures::CarryWindow);
    assert_eq!((off.version(), off.carry_window()), (5, false));
    assert_eq!(off.window.0.len(), 1, "the genesis activation goes");
    let on = base()
        .epoch(&[(FEATURE_CARRY_WINDOW, ahead)])
        .records()
        .records()
        .records()
        .rules(GenesisFeatures::CarryWindow);
    assert_eq!((on.version(), on.carry_window()), (5, true));
    let four_off = base()
        .epoch(&[(FEATURE_SEALER_V5, NEVER)])
        .records()
        .rules(GenesisFeatures::CarryWindow);
    assert_eq!((four_off.version(), four_off.carry_window()), (4, true));
}

/// The record of feature 4 comes in front of the block after the switch
/// to version 4. A tick before the record takes it, and the boundary that
/// the epoch forces before it does not: the walk refuses the head.
#[test]
fn an_activation_at_a_switch_boundary_is_refused() {
    let chain = Chain::default()
        .epoch(&[])
        .records()
        .epoch(&[(FEATURE_SEALER_V4, 0)])
        .epoch(&[(FEATURE_SEALER_V5, 0)]);
    let err = SealerRules::at_head(GenesisFeatures::None, &chain.blocks)
        .unwrap_err()
        .to_string();
    assert!(err.contains("block 4"), "{err}");
}

/// The same activation after an empty block: no epoch can force the
/// boundary of an empty block, so a tick took it, and the rules are known.
#[test]
fn an_activation_after_an_empty_switch_block_is_taken() {
    let chain = Chain::default()
        .epoch(&[])
        .records()
        .block(true, &[(FEATURE_SEALER_V4, 0)], 0)
        .empty()
        .epoch(&[(FEATURE_SEALER_V5, 0)]);
    let rules = chain.rules(GenesisFeatures::None);
    assert_eq!(rules.version(), 5);
}

#[test]
fn the_walk_needs_every_block_from_block_one() {
    let mut chain = Chain::default().epoch(&[]).records().records();
    chain.blocks.remove(1);
    let err = SealerRules::at_head(GenesisFeatures::None, &chain.blocks)
        .unwrap_err()
        .to_string();
    assert!(err.contains("block 3 comes where block 2 belongs"), "{err}");
}

/// The outcomes of `VersionSwitch.onActivation`, one rule each.
#[test]
fn a_record_gets_the_outcome_of_the_sealer() {
    let record = |feature_id, l1_origin| Activation {
        l1_origin,
        feature_id,
        time_ms: 0,
    };
    let mut rules = SealerRules::genesis(GenesisFeatures::None);
    assert_eq!(rules.on_activation(record(2, 41), 40, T0), Outcome::Ignored);
    assert_eq!(rules.on_activation(record(4, 41), 40, T0), Outcome::Early);
    assert_eq!(rules.on_activation(record(3, 39), 40, T0), Outcome::Missed);
    assert_eq!(rules.on_activation(record(3, 43), 40, T0), Outcome::Ignored);
    assert_eq!(rules.on_activation(record(3, 41), 40, T0), Outcome::Stored);
    assert_eq!(rules.on_activation(record(3, 41), 40, T0), Outcome::Ignored);
    rules.at_boundary(&Boundary {
        l1_origin: 41,
        sealed_ms: T0,
        l2_timestamp: T0 + TICK,
    });
    assert_eq!(rules.version(), 4);
    assert_eq!(
        rules.on_activation(record(4, 41), 41, T0),
        Outcome::StoredLate
    );
    assert_eq!(
        rules.carry.0[0].time_ms, T0,
        "a late time of 0 takes the open block"
    );
    assert_eq!(rules.on_activation(record(5, 42), 41, T0), Outcome::Early);
    assert_eq!(rules.on_activation(record(3, 42), 41, T0), Outcome::Stored);
    assert_eq!(rules.on_activation(record(3, 0), 41, T0), Outcome::Ignored);
}

#[test]
fn a_schedule_holds_at_most_three_activations() {
    let mut rules = SealerRules::genesis(GenesisFeatures::None);
    let take = |rules: &mut SealerRules, l1_origin| {
        rules.on_activation(
            Activation {
                l1_origin,
                feature_id: 3,
                time_ms: NEVER,
            },
            0,
            T0,
        )
    };
    assert_eq!(
        [41, 42, 43, 44].map(|origin| take(&mut rules, origin)),
        [
            Outcome::Stored,
            Outcome::Stored,
            Outcome::Stored,
            Outcome::Full
        ]
    );
}

/// The genesis config names the set as `genesis_features`.
#[test]
fn the_genesis_config_names_the_genesis_features() {
    let parse = |extra: &str| {
        toml::from_str::<kardamom_types::Genesis>(&format!("chain_id = 1\n{extra}"))
            .map(|genesis| genesis.genesis_features)
    };
    assert_eq!(parse("").unwrap(), GenesisFeatures::None);
    assert_eq!(
        parse("genesis_features = [3, 4, 5]").unwrap(),
        GenesisFeatures::CarryWindow
    );
    let err = parse("genesis_features = [4]").unwrap_err().to_string();
    assert!(err.contains("genesis_features"), "{err}");
}
