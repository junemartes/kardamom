//! `canary-da-lag`: the transaction canary sees a DA-lag halt the way a
//! user does, and its page waits behind the halt's page. The batcher is
//! frozen until the sealed head passes the DA-lag budget. The canary's
//! `transfer` probe must then report `rpc_error{code="-32010"}`, and
//! Alertmanager must hold a canary page as inhibited by
//! `KardamomHaltDaLag`. After the thaw the canary's transfers succeed
//! again.
//!
//! The case needs a cluster deployed with a small DA-lag budget
//! (`KARDAMOM_DA_LAG_BUDGET_BLOCKS`), as `da-lag-halt` does, so it runs by
//! name and in no shard: a small budget for a whole shard refuses the
//! load of the other cases.

use std::time::Duration;

use super::da_lag::{FrozenBatcher, await_halt};
use crate::harness::Harness;
use crate::metrics::Target;
use crate::poll::{self, Budget};

/// The canary's exporter on the monitoring node.
const CANARY_PORT: u16 = 9012;
/// The monitoring job's Alertmanager on the same node.
const ALERTMANAGER_PORT: u16 = 9093;
const PROBE_TOTAL: &str = "kardamom_canary_probe_total";
/// The canary page fires after two minutes of failed transfers; the
/// Alertmanager evaluation adds a little.
const PAGE_PATIENCE: Duration = Duration::from_mins(6);

/// The canary's transfer runs whose labels hold `fragment`.
async fn transfers(h: &Harness, fragment: &str) -> Option<i64> {
    let target = Target::bridged(
        h.probes.validator.ip,
        &h.probes.validator.container,
        CANARY_PORT,
    );
    let body = h.probes.scrape().fetch(&target).await?;
    Some(
        body.lines()
            .filter(|l| l.starts_with(PROBE_TOTAL))
            .filter(|l| l.contains("probe=\"transfer\"") && l.contains(fragment))
            .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
            .map(|v| {
                #[allow(
                    clippy::cast_possible_truncation,
                    reason = "a run counter, far under 2^53"
                )]
                let n = v as i64;
                n
            })
            .sum(),
    )
}

/// Wait until the transfer runs with `fragment` pass `floor`.
async fn await_runs(
    h: &Harness,
    ctx: &str,
    fragment: &str,
    floor: i64,
    patience: Duration,
) -> anyhow::Result<()> {
    let outcome =
        poll::until(
            Budget::new(patience, Duration::from_secs(5)),
            |_| async move {
                Ok::<_, anyhow::Error>(transfers(h, fragment).await.filter(|n| *n > floor))
            },
        )
        .await?;
    outcome.or_fail(|elapsed| {
        crate::chaos_fail!(
            "{ctx}: the canary reported no transfer {fragment} within {}s",
            elapsed.as_secs()
        )
    })?;
    Ok(())
}

/// Wait until Alertmanager holds a canary page inhibited.
async fn await_inhibited_page(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let url = format!(
        "http://{}:{ALERTMANAGER_PORT}/api/v2/alerts?filter=alertname%3D~%22KardamomCanary.%2A%22",
        h.probes.validator.ip
    );
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let outcome = poll::until(Budget::new(PAGE_PATIENCE, Duration::from_secs(10)), |_| {
        let http = http.clone();
        let url = url.clone();
        async move {
            let alerts: serde_json::Value = match http.get(&url).send().await {
                Ok(r) => r.json().await.unwrap_or_default(),
                Err(_) => serde_json::Value::Null,
            };
            let inhibited = alerts.as_array().into_iter().flatten().find(|a| {
                a["labels"]["severity"] == "critical"
                    && a["status"]["inhibitedBy"]
                        .as_array()
                        .is_some_and(|by| !by.is_empty())
            });
            Ok::<_, anyhow::Error>(inhibited.map(|a| a["labels"]["alertname"].to_string()))
        }
    })
    .await?;
    let (page, _) = outcome.or_fail(|elapsed| {
        crate::chaos_fail!(
            "{ctx}: no canary page was inhibited within {}s; Alertmanager must mute it behind the halt",
            elapsed.as_secs()
        )
    })?;
    crate::log(format!("{ctx}: {page} is inhibited by the halt"));
    Ok(())
}

/// # Errors
///
/// Returns the first assertion that fails.
pub(crate) async fn canary_da_lag(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "canary-da-lag";
    let budget = i64::try_from(
        h.knobs
            .da_lag_budget_blocks
            .ok_or_else(|| crate::chaos_fail!("{ctx}: KARDAMOM_DA_LAG_BUDGET_BLOCKS is not set — this case only means something on a cluster deployed with a small -Dkardamom.cluster.daLagBudgetBlocks"))?
            .get(),
    )
    .unwrap_or(i64::MAX);
    let refused = "code=\"-32010\"";
    let success = "outcome=\"success\"";
    let refused_before = transfers(h, refused)
        .await
        .ok_or_else(|| crate::chaos_fail!("{ctx}: the canary exports no transfer runs"))?;
    let frozen = FrozenBatcher::freeze(h, ctx).await?;
    let verdict = halted_view(h, ctx, budget, refused, refused_before).await;
    frozen.thaw(h, ctx).await;
    verdict?;
    await_halt(h, ctx, budget, false).await?;
    let success_after = transfers(h, success).await.unwrap_or(0);
    await_runs(h, ctx, success, success_after, Duration::from_mins(2)).await?;
    h.assert_executor_progress(Duration::from_mins(2)).await
}

/// While the batcher is frozen: the halt, the canary's refused
/// transfers, and its inhibited page.
async fn halted_view(
    h: &Harness,
    ctx: &str,
    budget: i64,
    refused: &str,
    refused_before: i64,
) -> anyhow::Result<()> {
    await_halt(h, ctx, budget, true).await?;
    await_runs(h, ctx, refused, refused_before, Duration::from_mins(2)).await?;
    crate::log(format!("{ctx}: the canary reports rpc_error -32010"));
    await_inhibited_page(h, ctx).await
}
