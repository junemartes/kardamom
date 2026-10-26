use alloy_primitives::{Address, U256};

use crate::rpc::AccountState;

#[test]
fn a_present_row_does_not_prove_correct_coverage() {
    let account = AccountState {
        address: Address::ZERO,
        nonce: 7,
        balance: U256::from(123),
    };
    assert!(account.matches_projection("7\n123\n00000000000000000042\n"));
    for row in [
        "6\n123\n42\n",
        "7\n1\n42\n",
        "7\n123\n\n",
        "\n\n\n",
        "7\n123\ninvalid\n",
    ] {
        assert!(!account.matches_projection(row), "{row:?}");
    }
}

#[test]
fn untouched_funded_accounts_keep_full_width_balances() {
    let account = AccountState {
        address: Address::ZERO,
        nonce: 0,
        balance: U256::MAX,
    };
    assert!(account.matches_projection(&format!("0\n{}\n0\n", U256::MAX)));
    assert!(!account.matches_projection("0\n0\n0\n"));
}
