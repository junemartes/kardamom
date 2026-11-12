//! Full available allocation logs retain events between the printed head and tail.

use std::path::Path;

use super::Diagnostics;
use crate::nomad::Alloc;

impl Diagnostics {
    pub(super) fn preserve_alloc_logs(job: &str, alloc: &Alloc, logs: &str) -> std::io::Result<()> {
        let directory = Path::new("/tmp/chaos-diagnostics");
        std::fs::create_dir_all(directory)?;
        let name = format!("{job}-{}", alloc.id);
        std::fs::write(directory.join(format!("{name}.log")), logs)?;
        let metadata = serde_json::json!({
            "job": job,
            "allocation": alloc.id,
            "node": alloc.node_name,
            "task_group": alloc.task_group,
            "job_version": alloc.job_version,
            "client_status": alloc.client_status,
            "desired_status": alloc.desired_status,
            "task_states": alloc.task_states,
            "log_bytes": logs.len(),
            "scope": "stdout and stderr available from Nomad at collection; rotated or collected logs can be absent",
        });
        std::fs::write(
            directory.join(format!("{name}.json")),
            serde_json::to_vec_pretty(&metadata)?,
        )
    }
}
