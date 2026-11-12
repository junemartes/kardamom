//! The detail of one Nomad client node: its status, when the status
//! last changed, and its events. A client that misses its heartbeat
//! goes `down`, logs a node event, and loses its allocations, which
//! Nomad then replaces on other nodes.

use serde::Deserialize;

use super::Nomad;

/// How many of the last node events the detail shows.
const NODE_EVENTS: usize = 10;

/// One client node, as `/v1/node/<id>` returns it.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct NodeDetail {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "StatusDescription", default)]
    status_description: Option<String>,
    /// Unix seconds of the last status change.
    #[serde(rename = "StatusUpdatedAt", default)]
    status_updated_at: i64,
    /// Nomad lists `null` for a node with no event.
    #[serde(rename = "Events", default)]
    events: Option<Vec<NodeEvent>>,
}

#[derive(Debug, Clone, Deserialize)]
struct NodeEvent {
    #[serde(rename = "Timestamp", default)]
    timestamp: Option<String>,
    #[serde(rename = "Subsystem", default)]
    subsystem: Option<String>,
    #[serde(rename = "Message", default)]
    message: String,
}

impl NodeDetail {
    /// The status line, then one line for each of the last node events,
    /// such as a missed heartbeat.
    pub(crate) fn render(&self) -> String {
        let updated = u64::try_from(self.status_updated_at)
            .ok()
            .and_then(|secs| {
                std::time::SystemTime::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(secs))
            })
            .map_or_else(
                || self.status_updated_at.to_string(),
                |t| humantime::format_rfc3339_seconds(t).to_string(),
            );
        let header = format!(
            "  node {}: status={} since={updated} {}",
            self.name,
            self.status,
            self.status_description.as_deref().unwrap_or("")
        );
        let events = self.events.as_deref().unwrap_or_default();
        let last = &events[events.len().saturating_sub(NODE_EVENTS)..];
        std::iter::once(header)
            .chain(last.iter().map(NodeEvent::line))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// The message of the event that marks a node down.
const HEARTBEAT_MISSED: &str = "Node heartbeat missed";

impl NodeDetail {
    /// Each missed heartbeat of the node, as `<name>@<timestamp>`.
    fn missed_heartbeats(&self) -> impl Iterator<Item = String> + '_ {
        self.events
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|e| e.message == HEARTBEAT_MISSED)
            .map(|e| format!("{}@{}", self.name, e.timestamp.as_deref().unwrap_or("-")))
    }
}

impl NodeEvent {
    fn line(&self) -> String {
        format!(
            "    {} [{}] {}",
            self.timestamp.as_deref().unwrap_or("-"),
            self.subsystem.as_deref().unwrap_or("-"),
            self.message
        )
    }
}

impl Nomad {
    /// The detail of the client node `id`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the body is not a node.
    pub(crate) async fn node(&self, id: &str) -> anyhow::Result<NodeDetail> {
        self.read(&format!("/v1/node/{id}")).await
    }

    /// Every missed heartbeat that the servers recorded, on every client
    /// node, as `<name>@<timestamp>`. A missed heartbeat marks the node
    /// down.
    ///
    /// # Errors
    ///
    /// Returns an error if a request fails.
    pub(crate) async fn missed_heartbeats(&self) -> anyhow::Result<Vec<String>> {
        let mut missed = Vec::new();
        for node in self.nodes().await? {
            missed.extend(self.node(&node.id).await?.missed_heartbeats());
        }
        Ok(missed)
    }
}

#[cfg(test)]
mod tests {
    use super::NodeDetail;

    /// A node that missed its heartbeat shows its status, the time of
    /// the change, and the event.
    #[test]
    fn a_down_node_shows_its_status_and_last_events() {
        let node: NodeDetail = serde_json::from_value(serde_json::json!({
            "Name": "control-0", "Status": "down",
            "StatusDescription": "node heartbeat missed",
            "StatusUpdatedAt": 1_761_301_845,
            "Events": [
                {"Timestamp": "2026-10-24T10:00:00Z", "Subsystem": "Cluster", "Message": "Node registered", "Details": null},
                {"Timestamp": "2026-10-24T10:30:45Z", "Subsystem": "Cluster", "Message": "Node heartbeat missed", "Details": null}
            ]
        }))
        .unwrap();
        assert_eq!(
            node.render(),
            "  node control-0: status=down since=2025-10-24T10:30:45Z node heartbeat missed\n\
             \x20   2026-10-24T10:00:00Z [Cluster] Node registered\n\
             \x20   2026-10-24T10:30:45Z [Cluster] Node heartbeat missed"
        );
        assert_eq!(
            node.missed_heartbeats().collect::<Vec<_>>(),
            ["control-0@2026-10-24T10:30:45Z"]
        );
        let quiet: NodeDetail = serde_json::from_value(serde_json::json!({
            "Name": "exec-0", "Status": "ready", "StatusUpdatedAt": 0, "Events": null
        }))
        .unwrap();
        assert_eq!(
            quiet.render(),
            "  node exec-0: status=ready since=1970-01-01T00:00:00Z "
        );
    }
}
