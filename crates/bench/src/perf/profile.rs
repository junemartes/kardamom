//! This module attaches async-profiler to the sealer's JVM.
//!
//! The sealer runs as a Java Aeron Cluster node, inside an inner Docker
//! container run by the Nomad docker driver, inside the `DinD` node
//! container. So every interaction is a two-level `docker exec` and
//! `docker cp` chain. Profiling uses itimer mode: it samples on-CPU
//! time from a signal timer, and needs neither `perf_events`, which is
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
///
/// # Errors
///
/// Returns an error if downloading or extracting the profiler archive
/// fails, or if `docker cp` into the container fails.
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

/// Run one asprof pass in the inner `cluster-*` container, then copy
/// the artifact it wrote at `remote` out to `out_dir.join(out_name)`.
/// `flags` is asprof's own argument list, for example `-d 30 -e itimer
/// -f /tmp/perf.html`; `remote` is the absolute path asprof writes
/// inside the inner container, which is also where this reads it back
/// from after the inner-to-outer `docker cp`.
///
/// # Errors
///
/// Returns an error if the profiler cannot attach inside the
/// container, or if either `docker cp` fails.
fn asprof_pass(
    node: &str,
    ap: &str,
    flags: &str,
    remote: &str,
    out_name: &str,
    out_dir: &Path,
) -> anyhow::Result<()> {
    docker_exec(
        node,
        &format!(
            r#"cid=$(docker ps -q --filter name=cluster | head -1)
pid=$(docker exec "$cid" sh -c 'pgrep -f java | head -1')
docker exec "$cid" /tmp/{ap}/bin/asprof {flags} "$pid"
docker cp "$cid":{remote} {remote}"#
        ),
    )
    .with_context(|| format!("asprof pass on {node} ({remote})"))?;
    sh(
        "docker",
        &[
            "cp",
            &format!("{node}:{remote}"),
            out_dir.join(out_name).to_str().context("path utf-8")?,
        ],
    )?;
    Ok(())
}

/// Profile the sealer JVM on `node` for `secs`. Writes the interactive
/// HTML flame graph and the collapsed-stacks text into `out_dir`.
/// Returns the collapsed stacks, one `frame;frame;... count` line for
/// each unique stack.
///
/// # Errors
///
/// Returns an error if the profiler cannot attach inside the
/// container, or if writing the output files fails.
pub fn run(node: &str, secs: u64, out_dir: &Path) -> anyhow::Result<String> {
    let ap = ap_dirname();
    println!("==> profiling {node} for {secs}s (itimer)");
    // asprof emits one output format per run. So this takes two passes
    // while the soak holds the rate steady: the full-length collapsed
    // capture, the report's source of truth, then a short HTML pass for
    // the interactive flame graph.
    asprof_pass(
        node,
        &ap,
        &format!("-d {secs} -e itimer -o collapsed -f /tmp/perf.collapsed"),
        "/tmp/perf.collapsed",
        "stacks.collapsed",
        out_dir,
    )?;
    asprof_pass(
        node,
        &ap,
        "-d 30 -e itimer -f /tmp/perf.html",
        "/tmp/perf.html",
        "flame.html",
        out_dir,
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
///
/// # Errors
///
/// Returns an error if the collapsed-stacks file cannot be read, or
/// the SVG cannot be written.
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
            .is_ok_and(|s| s.success());
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
