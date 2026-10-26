//! Stream evidence separates a stopped merge from a reader that cannot replay.

use super::Diagnostics;
use crate::nomad::Alloc;

const STREAM_STATE: &str = r#"python3 - <<'PY'
import urllib.error
import urllib.request

for port in (9004, 9013):
    url = f"http://127.0.0.1:{port}/metrics"
    print(url, flush=True)
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            body = response.read().decode()
        for line in body.splitlines():
            if not line.startswith('#') and any(name in line for name in (
                'kardamom_egress_', 'tee_dropped', 'best_effort', 'publish_dropped',
            )):
                print(line)
    except Exception as error:
        print(type(error).__name__, str(error))

url = 'http://127.0.0.1:8548/v1/frames?from_index=0&from_block=1&limit=1'
print(url, flush=True)
try:
    try:
        response = urllib.request.urlopen(url, timeout=5)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        print('status', response.status)
        for name, value in response.headers.items():
            if name.lower().startswith('x-egress-'):
                print(name, value)
except Exception as error:
    print(type(error).__name__, str(error))
PY"#;

impl Diagnostics {
    pub(super) async fn stream_section(&self, job: &str, alloc: &Alloc) {
        if !matches!(job, "egress" | "executor") {
            return;
        }
        let Ok(node) = self.contract.node(&alloc.node_name) else {
            return;
        };
        println!("----- {job} alloc {}: stream state -----", alloc.short_id());
        println!("{}", self.node_script(node, STREAM_STATE).await);
    }
}
