//! An inhibition names the fingerprint of the active DA-lag root alert.

use std::collections::BTreeSet;

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(transparent)]
pub(super) struct Alerts(Vec<Alert>);

#[derive(Deserialize)]
struct Alert {
    labels: Labels,
    fingerprint: String,
    status: Status,
}

#[derive(Deserialize)]
struct Labels {
    alertname: String,
    severity: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Status {
    state: State,
    inhibited_by: Vec<String>,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum State {
    Active,
    Suppressed,
    Unprocessed,
}

impl Alerts {
    pub(super) async fn read(http: &reqwest::Client, url: &str) -> Option<String> {
        let response = http.get(url).send().await.ok()?.error_for_status().ok()?;
        let alerts = response.json::<Self>().await.ok()?;
        alerts.inhibited_by_da_guard().map(str::to_string)
    }

    fn inhibited_by_da_guard(&self) -> Option<&str> {
        let roots: BTreeSet<_> = self
            .0
            .iter()
            .filter(|alert| {
                alert.labels.alertname == "KardamomHaltDaLag"
                    && alert.labels.severity.as_deref() == Some("critical")
                    && alert.status.state == State::Active
            })
            .map(|alert| alert.fingerprint.as_str())
            .collect();
        self.0
            .iter()
            .find(|alert| {
                alert.labels.alertname.starts_with("KardamomCanary")
                    && alert.labels.severity.as_deref() == Some("critical")
                    && alert.status.state == State::Suppressed
                    && alert
                        .status
                        .inhibited_by
                        .iter()
                        .any(|source| roots.contains(source.as_str()))
            })
            .map(|alert| alert.labels.alertname.as_str())
    }
}

#[cfg(test)]
mod tests;
