use super::*;
use kardamom_types::shard_map::{ShardMap, vslot_for};

const MAP: &str = include_str!("../../../deploy/cluster/config/shard-map.toml");

#[test]
fn funded_accounts_match_the_routing_contract() {
    let map: ShardMap = toml::from_str(MAP).unwrap();
    let signers =
        kardamom_bench::mnemonic::derive_signers(kardamom_bench::ANVIL_MNEMONIC, 16).unwrap();
    signers.iter().enumerate().for_each(|(index, signer)| {
        assert_eq!(vslot_for(signer.address), ACCT_VSLOT[index]);
        assert_eq!(map.lane_for(signer.address), ACCT_SHARD[index]);
    });
}

#[test]
fn the_sequencer_shard_has_enough_funded_accounts() {
    let map: ShardMap = toml::from_str(MAP).unwrap();
    let next = map.rebalance(3).unwrap();
    let moves = |account: u32| {
        let slot = ACCT_VSLOT[usize::try_from(account).unwrap()];
        map.lane_of_vslot(slot) != next.lane_of_vslot(slot)
    };
    let shard = crate::shard::Shard::Sequencer;
    assert!(shard.env().contains(&("RUN_LOAD", "0")));
    let mut accounts = Accounts::new(7, false);
    shard.cases().iter().for_each(|name| {
        let case = crate::cases::Case::parse(name).unwrap();
        accounts.take(case.pin(), name, moves).unwrap();
    });
}
