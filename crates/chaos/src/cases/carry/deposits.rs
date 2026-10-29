//! L1-derived credits keep their identity and order across a provider outage.

use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256};
use anyhow::Context;
use kardamom_l1_fault_proxy::Fault;
use serde::Serialize;

use crate::cases::l1::{Followers, await_followers_halted, await_resume};
use crate::harness::Harness;
use crate::l1::{Deposit, L1};
use crate::poll::{self, Budget};
use crate::rpc::{AccountState, ReceiptPosition, Rpc};

const CTX: &str = "carry-deposits-provider-fault";
const RECIPIENT: Address = Address::repeat_byte(0xd4);
const RECEIPT_WAIT: Budget = Budget {
    timeout: Duration::from_secs(120),
    interval: Duration::from_secs(2),
};

#[derive(Serialize)]
struct Record {
    value: u64,
    l1: Deposit,
    l2: Option<ReceiptPosition>,
}

struct Deposits {
    lockbox: Address,
    initial: AccountState,
    total: U256,
    records: Vec<Record>,
}

impl Deposits {
    async fn new(h: &Harness) -> anyhow::Result<Self> {
        let lockbox = h
            .lifecycle
            .vars()
            .carry
            .lockbox
            .as_deref()
            .context("deposit recovery has no configured lockbox")?
            .parse()?;
        Ok(Self {
            lockbox,
            initial: Self::executor(h, 0)?.account_state_at(RECIPIENT).await?,
            total: U256::ZERO,
            records: Vec::new(),
        })
    }

    fn executor(h: &Harness, index: usize) -> anyhow::Result<Rpc> {
        Rpc::new(
            &format!("http://{}:9024", h.probes.executors[index].ip),
            h.knobs.chain_id,
        )
    }

    async fn send(&mut self, l1: &L1, value: u64) -> anyhow::Result<usize> {
        let deposit = l1.deposit(self.lockbox, RECIPIENT, value).await?;
        self.total = self
            .total
            .checked_add(U256::from(value))
            .context("deposit total overflows")?;
        let index = self.records.len();
        self.records.push(Record {
            value,
            l1: deposit,
            l2: None,
        });
        Ok(index)
    }

    async fn await_credit(&mut self, h: &Harness, index: usize) -> anyhow::Result<()> {
        let source = self.records[index].l1.source_hash;
        let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
        let (position, _) = poll::until(RECEIPT_WAIT, |_| async {
            rpc.deposit_position(source).await
        })
        .await?
        .or_fail(|_| {
            crate::chaos_fail!("{CTX}: deposit {source} has no successful source-hash receipt")
        })?;
        self.records[index].l2 = Some(position);
        self.await_balance(h, self.total).await
    }

    async fn balances(&self, h: &Harness) -> anyhow::Result<Vec<AccountState>> {
        let mut accounts = Vec::new();
        for index in 0..h.probes.executors.len() {
            accounts.push(
                Self::executor(h, index)?
                    .account_state_at(RECIPIENT)
                    .await?,
            );
        }
        Ok(accounts)
    }

    async fn await_balance(&self, h: &Harness, credit: U256) -> anyhow::Result<()> {
        let expected = self
            .initial
            .balance
            .checked_add(credit)
            .context("credited balance overflows")?;
        poll::until(RECEIPT_WAIT, |_| async move {
            let accounts = self.balances(h).await?;
            Ok::<_, anyhow::Error>(accounts.iter().all(|a| a.nonce == self.initial.nonce && a.balance == expected).then_some(accounts))
        }).await?.or_fail(|_| crate::chaos_fail!("{CTX}: every executor must hold balance {expected} and unchanged recipient nonce; a missing or duplicate credit is possible"))?;
        Ok(())
    }

    async fn during(&mut self, h: &mut Harness, l1: &L1, base: Followers) -> anyhow::Result<usize> {
        let started = Instant::now();
        await_followers_halted(h, base, CTX).await?;
        let credited = self.total;
        let index = self.send(l1, 2_003).await?;
        let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
        self.assert_missing(&rpc, index).await?;
        h.assert_progress().await?;
        tokio::time::sleep(h.knobs.l1_fault.saturating_sub(started.elapsed())).await;
        self.assert_missing(&rpc, index).await?;
        self.await_balance(h, credited).await?;
        Ok(index)
    }

    async fn assert_missing(&self, rpc: &Rpc, index: usize) -> anyhow::Result<()> {
        let source = self.records[index].l1.source_hash;
        anyhow::ensure!(
            rpc.deposit_position(source).await?.is_none(),
            "{CTX}: deposit {source} lands while all providers are unavailable"
        );
        Ok(())
    }

    async fn outage(&mut self, h: &mut Harness, l1: &L1) -> anyhow::Result<usize> {
        let base = Followers::ready(h, CTX).await?;
        let armed = l1
            .set_faults(&[Fault::Unresponsive { all_sources: true }])
            .await;
        let held = match armed {
            Ok(()) => self.during(h, l1, base).await,
            Err(error) => Err(error),
        };
        let cleared = Followers::at_clear(h, l1, base).await;
        if let Err(error) = &cleared {
            crate::log(format!("{CTX}: clearing provider faults fails: {error:#}"));
        }
        let index = held?;
        await_resume(h, cleared?, CTX).await?;
        Ok(index)
    }

    async fn phases(&mut self, h: &mut Harness, l1: &L1) -> anyhow::Result<()> {
        let before = self.send(l1, 1_001).await?;
        self.await_credit(h, before).await?;
        let during = self.outage(h, l1).await?;
        self.await_credit(h, during).await?;
        let after = self.send(l1, 4_007).await?;
        self.await_credit(h, after).await?;
        anyhow::ensure!(
            self.records.array_windows::<2>().all(Record::ordered),
            "{CTX}: L1 deposit order or source-hash receipt order changes across recovery"
        );
        crate::log(format!(
            "{CTX}: three distinct source-hash receipts preserve L1 order and credit exactly {} wei on every executor",
            self.total
        ));
        Ok(())
    }

    fn report(&self, context: &str, result: &anyhow::Result<()>) -> anyhow::Result<()> {
        let report = serde_json::json!({
            "case": context, "recipient": RECIPIENT,
            "initial": format!("{:?}", self.initial), "credit": self.total.to_string(),
            "deposits": self.records,
            "failure": result.as_ref().err().map(|error| format!("{error:#}")),
        });
        std::fs::write(
            format!("/tmp/chaos-{context}-deposits.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;
        Ok(())
    }
}

impl Record {
    fn ordered([before, after]: &[Self; 2]) -> bool {
        before.l1.source_hash != after.l1.source_hash
            && before.l1.nonce < after.l1.nonce
            && (before.l1.l1_block, before.l1.log_index) < (after.l1.l1_block, after.l1.log_index)
            && before
                .l2
                .zip(after.l2)
                .is_some_and(|(before, after)| before < after)
    }
}

pub(super) async fn across_provider_fault(
    h: &mut Harness,
    context: &'static str,
) -> anyhow::Result<()> {
    let l1 = L1::new(h).await?;
    let mut deposits = Deposits::new(h).await?;
    let result = deposits.phases(h, &l1).await;
    let report = deposits.report(context, &result);
    result?;
    report
}
