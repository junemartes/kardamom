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

struct Replica {
    query: crate::rpc::Rpc,
    nonce: std::sync::Arc<std::sync::atomic::AtomicU64>,
    server: jsonrpsee::server::ServerHandle,
}

impl Replica {
    async fn start(nonce: u64) -> Self {
        let nonce = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(nonce));
        let mut module = jsonrpsee::RpcModule::new(nonce.clone());
        module
            .register_method("eth_getTransactionCount", |_, state, _| {
                format!("0x{:x}", state.load(std::sync::atomic::Ordering::Acquire))
            })
            .unwrap();
        module
            .register_method("eth_getBalance", |_, _, _| "0x7b")
            .unwrap();
        let service = jsonrpsee::server::Server::builder()
            .build("127.0.0.1:0")
            .await
            .unwrap();
        let query =
            crate::rpc::Rpc::new(&format!("http://{}", service.local_addr().unwrap()), 1).unwrap();
        Self {
            query,
            nonce,
            server: service.start(module),
        }
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        let _ = self.server.stop();
    }
}

#[tokio::test]
async fn a_receipted_transfer_waits_for_the_lagging_executor_before_comparing_redis() {
    let first = Replica::start(1).await;
    let second = Replica::start(2).await;
    let third = Replica::start(2).await;
    let snapshot = super::ReplicaAccount {
        queries: vec![
            first.query.clone(),
            second.query.clone(),
            third.query.clone(),
        ],
        account: 0,
        floor: 2,
    };
    let ready = std::cell::Cell::new(false);
    let waiting = async {
        let value = snapshot
            .wait(crate::poll::Budget::new(
                std::time::Duration::from_secs(2),
                std::time::Duration::from_millis(10),
            ))
            .await
            .unwrap();
        ready.set(true);
        value
    };
    let release = async {
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        assert!(
            !ready.get(),
            "a lagging first replica cannot establish the baseline"
        );
        first.nonce.store(2, std::sync::atomic::Ordering::Release);
    };
    let (state, ()) = tokio::join!(waiting, release);
    assert_eq!(state.nonce, 2);
    assert_eq!(state.balance, U256::from(123));
}

#[test]
fn an_executor_baseline_requires_full_state_agreement_and_the_receipted_nonce() {
    let old = AccountState {
        address: Address::ZERO,
        nonce: 1,
        balance: U256::from(100),
    };
    let current = AccountState {
        nonce: 2,
        balance: U256::from(90),
        ..old.clone()
    };
    assert!(AccountState::agreed(&[], 2).is_none());
    assert!(AccountState::agreed(&[old.clone(), old.clone()], 2).is_none());
    assert!(AccountState::agreed(&[old, current.clone()], 2).is_none());
    assert!(
        AccountState::agreed(
            &[
                current.clone(),
                AccountState {
                    balance: U256::from(91),
                    ..current.clone()
                }
            ],
            2
        )
        .is_none()
    );
    assert_eq!(
        AccountState::agreed(&[current.clone(), current.clone()], 2),
        Some(current)
    );
}
