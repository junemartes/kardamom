use super::*;

use crate::ANVIL_MNEMONIC as ANVIL_PHRASE;

#[test]
fn op_mix_covers_all_contracts_and_is_deterministic() {
    let c = DefiContracts::at(Address::repeat_byte(9), 0).unwrap();
    let mut hit = std::collections::HashSet::new();
    (0..4)
        .flat_map(|sender| (1..64).map(move |seq| (sender, seq)))
        .for_each(|(sender, seq)| {
            let call = op(&c, sender, seq);
            let again = op(&c, sender, seq);
            assert_eq!(call.to, again.to, "deterministic");
            assert_eq!(call.input, again.input, "deterministic");
            hit.insert(call.to);
            assert!(call.input.len() >= 4);
        });
    assert!(hit.contains(&c.pool) && hit.contains(&c.vault) && hit.contains(&c.clob));
}

#[test]
fn deployment_addresses_match_planned_nonces() {
    let signers = SignerSet::derive(ANVIL_PHRASE, 2).unwrap();
    let params = TxPlanParams {
        chain_id: 412_346,
        nonce_start: 5,
        gas_price: 1_000_000_000,
    };
    let dep = deployment_txs(&signers, params).unwrap();
    assert_eq!(dep.txs.len(), 3);
    assert_eq!(dep.txs[0].nonce, 5);
    assert_eq!(dep.txs[2].nonce, 7);
    let expect = DefiContracts::at(signers[0].signer.address(), 5).unwrap();
    assert_eq!(dep.contracts.pool, expect.pool);
    assert_eq!(dep.contracts.clob, expect.clob);
}

#[test]
fn sender_zero_queue_starts_after_deployments() {
    let signers = SignerSet::derive(ANVIL_PHRASE, 2).unwrap();
    let c = DefiContracts::at(signers[0].signer.address(), 0).unwrap();
    let params = TxPlanParams {
        chain_id: 412_346,
        nonce_start: 0,
        gas_price: 1_000_000_000,
    };
    let q = pregenerate_defi(&signers, &c, 4, params).unwrap();
    assert_eq!(q[0][0].nonce, 3, "sender 0 shifted past deployments");
    assert_eq!(q[1][0].nonce, 0, "other senders start at nonce_start");
    // Every sender's first operation is the pool seed, which funds
    // swap balances.
    for queue in &q {
        assert!(!queue.is_empty());
    }
}
