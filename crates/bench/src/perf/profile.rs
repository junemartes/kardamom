//! This module attaches async-profiler to the sealer's JVM.
//!
//! The sealer runs as a Java Aeron Cluster node, inside an inner Docker
//! container run by the Nomad docker driver, inside the DinD node
//! container. So every interaction is a two-level `docker exec` and
//! `docker cp` chain. Profiling uses itimer mode: it samples on-CPU
//! time from a signal timer, and needs neither perf_events, which is
//! unavailable in the nested containers, nor kernel symbols.

use std::path::Path;
use std::process::Command;

use anyhow::{Context, bail};

use crate::perf::cluster::{docker_exec, sh};

/// The pinned async-profiler release. On an offline host, override the
/// download with
/// `KARDAMOM_ASYNC_PROFILER_TGZ=/path/to/async-profiler-<ver>-linux-x64.tar.gz`.
const AP_VERSION: &str = "3.0";
const AP_URL: &str = "https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-x64.tar.gz";

fn ap_dirname() -> String {
    format!("async-profiler-{AP_VERSION}-linux-x64")
}

/// Fetch the async-profiler tarball into `cache_dir`, or reuse it if
/// already there.
fn fetch_tarball(cache_dir: &Path) -> anyhow::Result<std::path::PathBuf> {
    if let Ok(local) = std::env::var("KARDAMOM_ASYNC_PROFILER_TGZ") {
        return Ok(local.into());
    }
    std::fs::create_dir_all(cache_dir)?;
    let tgz = cache_dir.join(format!("{}.tar.gz", ap_dirname()));
    if !tgz.exists() {
        println!("==> downloading async-profiler {AP_VERSION}");
        sh(
            "curl",
            &["-fsSL", "-o", tgz.to_str().context("path utf-8")?, AP_URL],
        )?;
    }
    Ok(tgz)
}

/// Copy async-profiler into the sealer's inner `cluster-*` container.
/// This is idempotent: re-staging over an existing copy is fine.
pub fn stage(node: &str, cache_dir: &Path) -> anyhow::Result<()> {
    let tgz = fetch_tarball(cache_dir)?;
    sh(
        "docker",
        &[
            "cp",
            tgz.to_str().context("path utf-8")?,
            &format!("{node}:/tmp/ap.tgz"),
        ],
    )?;
    docker_exec(
        node,
        r#"cid=$(docker ps -q --filter name=cluster | head -1)
[ -n "$cid" ] || { echo "no inner cluster container" >&2; exit 1; }
docker cp /tmp/ap.tgz "$cid":/tmp/ap.tgz
docker exec "$cid" sh -c 'cd /tmp && tar xzf ap.tgz'"#,
    )
    .with_context(|| format!("stage async-profiler into {node}"))?;
    Ok(())
}

/// Profile the sealer JVM on `node` for `secs`. Writes the interactive
/// HTML flame graph and the collapsed-stacks text into `out_dir`.
/// Returns the collapsed stacks, one `frame;frame;... count` line for
/// each unique stack.
pub fn run(node: &str, secs: u64, out_dir: &Path) -> anyhow::Result<String> {
    let ap = ap_dirname();
    println!("==> profiling {node} for {secs}s (itimer)");
    // asprof emits one output format per run. So this takes two passes
    // while the soak holds the rate steady: the full-length collapsed
    // capture, the report's source of truth, then a short HTML pass for
    // the interactive flame graph.
    docker_exec(
        node,
        &format!(
            r#"cid=$(docker ps -q --filter name=cluster | head -1)
pid=$(docker exec "$cid" sh -c 'pgrep -f java | head -1')
docker exec "$cid" /tmp/{ap}/bin/asprof -d {secs} -e itimer -o collapsed -f /tmp/perf.collapsed "$pid"
docker cp "$cid":/tmp/perf.collapsed /tmp/perf.collapsed"#
        ),
    )
    .with_context(|| format!("asprof collapsed pass on {node}"))?;
    sh(
        "docker",
        &[
            "cp",
            &format!("{node}:/tmp/perf.collapsed"),
            out_dir
                .join("stacks.collapsed")
                .to_str()
                .context("path utf-8")?,
        ],
    )?;

    docker_exec(
        node,
        &format!(
            r#"cid=$(docker ps -q --filter name=cluster | head -1)
pid=$(docker exec "$cid" sh -c 'pgrep -f java | head -1')
docker exec "$cid" /tmp/{ap}/bin/asprof -d 30 -e itimer -f /tmp/perf.html "$pid"
docker cp "$cid":/tmp/perf.html /tmp/perf.html"#
        ),
    )
    .with_context(|| format!("asprof html pass on {node}"))?;
    sh(
        "docker",
        &[
            "cp",
            &format!("{node}:/tmp/perf.html"),
            out_dir.join("flame.html").to_str().context("path utf-8")?,
        ],
    )?;

    let collapsed = std::fs::read_to_string(out_dir.join("stacks.collapsed"))?;
    if collapsed.trim().is_empty() {
        bail!("profiler produced no samples — was load flowing during the window?");
    }
    Ok(collapsed)
}

/// Render a static SVG flame graph from collapsed stacks, if
/// `flamegraph.pl`, or a local copy, is available. This is best-effort:
/// the collapsed file and HTML exist either way.
pub fn render_svg(out_dir: &Path, title: &str) -> anyhow::Result<Option<std::path::PathBuf>> {
    let script = out_dir.join("flamegraph.pl");
    if !script.exists() {
        let fetched = Command::new("curl")
            .args([
                "-fsSL",
                "-o",
                script.to_str().context("path utf-8")?,
                "https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl",
            ])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !fetched {
            return Ok(None);
        }
    }
    let svg = out_dir.join("flame.svg");
    let out = Command::new("perl")
        .arg(&script)
        .args(["--title", title, "--width", "1400", "--colors", "java"])
        .arg(out_dir.join("stacks.collapsed"))
        .output()?;
    if !out.status.success() {
        return Ok(None);
    }
    std::fs::write(&svg, &out.stdout)?;
    Ok(Some(svg))
}
