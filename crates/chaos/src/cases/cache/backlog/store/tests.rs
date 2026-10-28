use std::cell::Cell;
use std::time::Duration;

use alloy_primitives::{Address, U256};

use super::*;
use crate::cases::cache::backlog::observe::Observe;
use crate::poll::{self, Budget};

struct Fixture {
    name: String,
    _root: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        Self {
            name: format!(
                "chaos-backlog-{}",
                root.path().file_name().unwrap().to_str().unwrap()
            ),
            _root: root,
        }
    }

    async fn docker(&self, args: &[&str]) -> String {
        let output = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::process::Command::new("docker")
                .kill_on_drop(true)
                .args(args)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }

    async fn start(&self) -> Store {
        self.docker(&[
            "run",
            "--rm",
            "--detach",
            "--name",
            &self.name,
            "--publish",
            "127.0.0.1::6379",
            "redis:7-alpine",
        ])
        .await;
        let address = self
            .docker(&["port", &self.name, "6379/tcp"])
            .await
            .trim()
            .parse()
            .unwrap();
        poll::until(
            Budget::new(Duration::from_secs(10), Duration::from_millis(50)),
            |_| async { Ok(Discovery::connect_primary(address).await.ok()) },
        )
        .await
        .unwrap()
        .or_fail(|_| anyhow::anyhow!("Redis did not start"))
        .unwrap();
        Store { address }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::process::Command::new("timeout")
            .args(["10s", "docker", "rm", "-f", &self.name])
            .output();
    }
}

#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn direct_backlog_reads_preserve_state_retry_a_pause_and_reject_a_demoted_primary() {
    let fixture = Fixture::new();
    let store = fixture.start().await;
    let account = AccountState {
        address: Address::ZERO,
        nonce: 7,
        balance: U256::MAX,
    };
    let heads =
        Heads::parse("00000000000000000010\n00000000000000000020\n00000000000000000030").unwrap();
    assert!(store.heads().await.is_err());
    assert!(!account.matches_projection(&store.row(&account).await.unwrap()));
    store
        .command::<u64>(
            redis::cmd("HSET")
                .arg(format!("acct:{}", account.address))
                .arg("nonce")
                .arg(7)
                .arg("balance")
                .arg(U256::MAX.to_string())
                .arg("tx_idx")
                .arg(42),
        )
        .await
        .unwrap();
    store.hold(heads).await.unwrap();
    assert_eq!(store.heads().await.unwrap().0, heads.0);
    assert!(account.matches_projection(&store.row(&account).await.unwrap()));
    let ttl: u64 = store
        .command(redis::cmd("TTL").arg("head:0"))
        .await
        .unwrap();
    assert!((590..=600).contains(&ttl), "heartbeat lease {ttl}");
    store
        .command::<String>(redis::cmd("CLIENT").arg("PAUSE").arg(3500).arg("ALL"))
        .await
        .unwrap();
    let calls = Cell::new(0);
    Observe::new(
        "paused Redis heads",
        Budget::new(Duration::from_secs(10), Duration::from_millis(50)),
    )
    .wait(|| async {
        calls.set(calls.get() + 1);
        let actual = store.heads().await?;
        anyhow::ensure!(actual.0 == heads.0, "heads changed during pause");
        Ok(())
    })
    .await
    .unwrap();
    assert!(calls.get() >= 2, "the first bounded read must time out");
    store.clear().await.unwrap();
    assert!(store.heads().await.is_err());
    assert!(account.matches_projection(&store.row(&account).await.unwrap()));
    store.hold(heads).await.unwrap();
    store
        .command::<String>(redis::cmd("REPLICAOF").arg("127.0.0.1").arg(1))
        .await
        .unwrap();
    let error = store.heads().await.unwrap_err();
    assert!(error.to_string().contains("not a primary"), "{error}");
}
