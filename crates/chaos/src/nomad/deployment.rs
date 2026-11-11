//! The latest deployment of a job, as Nomad reports it.

use std::collections::HashMap;

use serde::Deserialize;

use super::Nomad;

/// One deployment: the job version it rolls out, its status, and the
/// health of the allocations of each task group.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Deployment {
    #[serde(rename = "JobVersion")]
    pub(crate) job_version: u64,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "StatusDescription", default)]
    pub(crate) description: String,
    #[serde(rename = "TaskGroups", default)]
    groups: HashMap<String, GroupState>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct GroupState {
    #[serde(rename = "HealthyAllocs", default)]
    healthy: u64,
    #[serde(rename = "UnhealthyAllocs", default)]
    unhealthy: u64,
}

impl Deployment {
    /// Whether Nomad gave up the deployment.
    pub(crate) fn failed(&self) -> bool {
        self.status == "failed"
    }

    /// Whether every allocation of the deployment became healthy.
    pub(crate) fn successful(&self) -> bool {
        self.status == "successful"
    }

    /// The healthy and the unhealthy allocations of `group`. A group
    /// that the deployment does not touch has none.
    pub(crate) fn health(&self, group: &str) -> (u64, u64) {
        self.groups
            .get(group)
            .map_or((0, 0), |g| (g.healthy, g.unhealthy))
    }
}

impl Nomad {
    /// The latest deployment of `job`, or `None` when the job has none.
    ///
    /// # Errors
    ///
    /// Returns an error if the request or the decode fails.
    pub(crate) async fn latest_deployment(&self, job: &str) -> anyhow::Result<Option<Deployment>> {
        self.read(&format!("/v1/job/{job}/deployment")).await
    }
}

#[cfg(test)]
mod tests {
    use super::Deployment;

    #[test]
    fn a_failed_deployment_reports_its_unhealthy_group() {
        let body = r#"{"JobVersion":7,"Status":"failed","StatusDescription":"Failed due to unhealthy allocations",
            "TaskGroups":{"executor-0":{"HealthyAllocs":0,"UnhealthyAllocs":1,"PlacedAllocs":1}}}"#;
        let deployment: Deployment = serde_json::from_str(body).unwrap();
        assert!(deployment.failed() && !deployment.successful());
        assert_eq!(deployment.job_version, 7);
        assert_eq!(deployment.health("executor-0"), (0, 1));
        assert_eq!(deployment.health("executor-1"), (0, 0));
        let none: Option<Deployment> = serde_json::from_str("null").unwrap();
        assert!(none.is_none());
    }
}
