//! The Aeron archive cases: substrate loss, data loss with re-replication
//! from the mirror, and segment corruption with detect-and-heal.

use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::harness::Harness;
use crate::nodes::Pipe;
use crate::poll::{self, Budget};

mod heal;
mod segment;
mod staging;

#[cfg(test)]
use segment::SegmentHashes;
use segment::{Segment, pick_clean_segment};

const DRIVER_TASK: &str = "archiving-media-driver";
const ARCHIVE_DIR: &str = "/opt/kardamom/archive/dir";
const CRC_VERIFY: &str = "verify -a -checksum io.aeron.archive.checksum.Crc32";
/// The CRC-armed verify attempts of a restored archive.
const VERIFY_ATTEMPTS: usize = 3;

/// The running aeron allocation count, required non-zero: a hiccuping
/// query reads as zero, and injecting anyway would make the post-kill
/// count assert pass trivially.
async fn aeron_baseline(h: &Harness, case: &str) -> anyhow::Result<usize> {
    let base = h.nomad.count_running("aeron").await?;
    anyhow::ensure!(
        base > 0,
        "{}: {case}: no running aeron allocs at baseline — cannot assert driver recovery",
        crate::FAIL_PREFIX
    );
    Ok(base)
}

/// Hard-kill the Aeron substrate on ingress-0: every service on the
/// node shares that driver, so the local ingress loses its transport
/// and its recorder in one blow. The pipeline rides on ingress-1, Nomad
/// restarts the system task, and the collocated ingress recovers.
pub(crate) async fn driver_loss(h: &mut Harness) -> anyhow::Result<()> {
    let base = aeron_baseline(h, "archive-driver-loss").await?;
    let node = h.container("ingress-0")?;
    crate::log(format!(
        "archive-driver-loss: killing archiving-media-driver on {node} (aeron allocs baseline={base})"
    ));
    h.inject_hard(&[&node], DRIVER_TASK).await?;
    h.assert_progress().await?;
    h.assert_count("aeron", base, h.knobs.driver_restart_slo())
        .await?;
    h.assert_count("ingress", 2, h.knobs.reschedule_slo).await?;
    h.assert_ingress_pair_live("archive-driver-loss").await
}

/// The data-loss drill: wipe ingress-0's `tx_data` archive while its
/// driver is down, restore the catalog and the segments from
/// ingress-1's mirror, and verify the restored archive with Aeron's own
/// tool once the daemon is back. The mark file is never transplanted
/// from a live source: its heartbeat would look active to the
/// restarting daemon.
pub(crate) async fn tx_data_wipe(h: &mut Harness) -> anyhow::Result<()> {
    let base = aeron_baseline(h, "archive-tx-data-wipe").await?;
    let victim = h.container("ingress-0")?;
    let mirror = h.container("ingress-1")?;
    crate::log(format!(
        "archive-tx-data-wipe: killing aeron substrate on {victim} + wiping its tx_data archive volume"
    ));
    h.inject_hard(&[&victim], DRIVER_TASK).await?;
    h.nodes
        .exec(
            &victim,
            &format!("rm -f {ARCHIVE_DIR}/*.rec {ARCHIVE_DIR}/archive.catalog"),
        )
        .await
        .map_err(|e| {
            crate::chaos_fail!("archive-tx-data-wipe: could not wipe ingress-0 archive: {e}")
        })?;
    crate::log(format!(
        "archive-tx-data-wipe: re-replicating archive from {mirror} mirror"
    ));
    copy_stable_catalog(h, &mirror, &victim).await?;
    copy_segments(h, &mirror, &victim).await?;
    h.assert_progress().await?;
    h.assert_count("aeron", base, h.knobs.driver_restart_slo())
        .await?;
    verify_restored(h, &victim).await?;
    h.assert_count("ingress", 2, h.knobs.reschedule_slo).await?;
    h.assert_ingress_pair_live("archive-tx-data-wipe").await
}

/// Stream the mirror's segments into the victim's archive directory,
/// with no copy in memory. The victim's daemon must not start before
/// the copy ends: it would adopt a partial archive.
async fn copy_segments(h: &Harness, mirror: &str, victim: &str) -> anyhow::Result<()> {
    let pipe = Pipe {
        from: mirror,
        read: "tar -C /opt/kardamom/archive --sparse --warning=no-file-changed --exclude='dir/archive-mark.dat' --exclude='dir/archive.catalog' -cf - dir",
        to: victim,
        write: "tar -C /opt/kardamom/archive -xf -",
        max_read_status: 1,
    };
    let streamed = h.nodes.stream(&pipe).await.map_err(|e| {
        crate::chaos_fail!("archive-tx-data-wipe: re-replication copy failed: {e:#}")
    })?;
    let ended = SystemTime::now();
    crate::log(format!(
        "archive-tx-data-wipe: re-replicated {} bytes from {mirror} in {:.1}s",
        streamed.bytes,
        streamed.elapsed.as_secs_f64()
    ));
    let Some(daemon) = h.nodes.inner_container(victim, DRIVER_TASK).await else {
        return Ok(());
    };
    let started = h
        .nodes
        .started_at(victim, &daemon)
        .await
        .unwrap_or_default();
    match humantime::parse_rfc3339_weak(started.trim()) {
        Ok(at) if at < ended => Err(crate::chaos_fail!(
            "archive-tx-data-wipe: the {DRIVER_TASK} on {victim} restarted at {started}, before the re-replication copy ended ({} bytes in {:.1}s): its daemon read a partial archive",
            streamed.bytes,
            streamed.elapsed.as_secs_f64()
        )),
        Ok(_) => Ok(()),
        Err(e) => {
            crate::log(format!(
                "archive-tx-data-wipe: the start time {started:?} of {daemon} does not parse ({e}); the verify judges the archive"
            ));
            Ok(())
        }
    }
}

/// Copy the mirror's catalog with a stable read: the mirror's daemon
/// rewrites entries on recording lifecycle events, and this very
/// injection triggers some. Two identical hashes around the copy
/// guarantee a consistent image.
async fn copy_stable_catalog(h: &Harness, mirror: &str, victim: &str) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(10, 1), |_| async move {
        catalog_copy_once(h, mirror, victim).await
    })
    .await?;
    outcome
        .or_fail(|_| {
            crate::chaos_fail!(
                "archive-tx-data-wipe: mirror catalog never stabilized across 10 attempts"
            )
        })
        .map(|_| ())
}

async fn catalog_hash(h: &Harness, node: &str) -> anyhow::Result<String> {
    h.nodes
        .exec(
            node,
            &format!("sha256sum {ARCHIVE_DIR}/archive.catalog | cut -d' ' -f1"),
        )
        .await
}

async fn catalog_copy_once(h: &Harness, mirror: &str, victim: &str) -> anyhow::Result<Option<()>> {
    let before = catalog_hash(h, mirror).await?;
    if before.is_empty() || before != catalog_hash(h, mirror).await? {
        return Ok(None);
    }
    let catalog = h
        .nodes
        .exec_bytes(mirror, &format!("cat {ARCHIVE_DIR}/archive.catalog"), 0)
        .await?;
    h.nodes
        .exec_with_stdin(
            victim,
            &format!("cat > {ARCHIVE_DIR}/archive.catalog"),
            catalog,
        )
        .await
        .map_err(|e| crate::chaos_fail!("archive-tx-data-wipe: catalog copy failed: {e}"))?;
    Ok((before == catalog_hash(h, mirror).await?).then_some(()))
}

/// Verify a private archive copy inside the restarted daemon container.
/// The tool writes stop positions and invalidates descriptors, so it
/// must not open the live catalog or segment files. Stale catalog entry
/// checksums are tolerated and counted: the daemon's adoption path
/// rewrites entries without recomputing them.
async fn verify_restored(h: &Harness, victim: &str) -> anyhow::Result<()> {
    let daemon = h
        .nodes
        .inner_container(victim, DRIVER_TASK)
        .await
        .ok_or_else(|| {
            crate::chaos_fail!(
                "archive-tx-data-wipe: no aeron container on ingress-0 after restart"
            )
        })?;
    let script = format!(
        r#"docker exec {daemon} bash -lc 'set -eu; snapshot=$(mktemp -d); trap "rm -rf -- \"$snapshot\"" EXIT; cp -a --reflink=auto --sparse=always {ARCHIVE_DIR}/. "$snapshot/"; rm -f "$snapshot/archive-mark.dat"; java --add-opens java.base/java.util.zip=ALL-UNNAMED -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool "$snapshot" {CRC_VERIFY} 2>&1'"#
    );
    // Up to three attempts. Each verify runs under the streaming bound
    // of the exec, since a CRC pass over a large archive on a slow
    // runner takes well over the command bound. No poll deadline cancels
    // an attempt.
    let mut last = String::new();
    for _ in 0..VERIFY_ATTEMPTS {
        last = h.nodes.exec_long(victim, &script).await.unwrap_or_default();
        if verify_is_clean(&last) {
            break;
        }
    }
    if !verify_is_clean(&last) {
        crate::log(format!(
            "archive-tx-data-wipe: verify output tail\n{}",
            tail(&last, 20)
        ));
        return Err(crate::chaos_fail!(
            "archive-tx-data-wipe: restored archive failed CRC-armed verify after {VERIFY_ATTEMPTS} attempts"
        ));
    }
    let stale = last
        .lines()
        .filter(|l| l.contains("invalid Catalog checksum"))
        .count();
    if stale > 0 {
        crate::log(format!(
            "archive-tx-data-wipe: note — {stale} adoption-staled catalog entry checksum(s) tolerated"
        ));
    }
    crate::log(
        "archive-tx-data-wipe: restored archive verified OK on ingress-0 (2-copy redundancy recovered)",
    );
    Ok(())
}

fn verify_is_clean(out: &str) -> bool {
    let other_err = out
        .lines()
        .filter(|l| !l.contains("invalid Catalog checksum"))
        .filter(|l| {
            let u = l.to_ascii_uppercase();
            u.contains("ERR:") || u.contains("ERR ") || u.contains("FAILED")
        })
        .count();
    !out.contains("Exception") && has_ok_recording(out) && other_err == 0
}

/// A line like `recordingId=3 ... OK`: the id, then `OK` later on the
/// same line, wherever the tool puts it.
fn has_ok_recording(out: &str) -> bool {
    out.lines().any(|l| {
        l.find("recordingId=")
            .is_some_and(|at| l[at..].contains("OK"))
    })
}

fn tail(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

/// The corruption drill: flip 16 payload bytes mid-segment on ingress-0
/// while the node is drained, detect it with a CRC-armed verify, name
/// and heal exactly that segment from the mirror with the Rust tool,
/// then verify clean and undrain.
pub(crate) async fn corruption(h: &mut Harness) -> anyhow::Result<()> {
    let rerep = h
        .lifecycle
        .repo_root()
        .join("target/release/kardamom-archive-rereplicate");
    anyhow::ensure!(
        rerep.is_file(),
        "{}: archive-corruption: kardamom-archive-rereplicate not at {}",
        crate::FAIL_PREFIX,
        rerep.display()
    );
    let base = aeron_baseline(h, "archive-corruption").await?;
    let victim = h.container("ingress-0")?;
    let mirror = h.container("ingress-1")?;
    let node_id = h.nomad.node_id("ingress-0").await?;
    let image = h
        .nodes
        .exec(
            &victim,
            "docker ps -a --format '{{.Image}} {{.Names}}' | awk '/archiving/ {print $1; exit}'",
        )
        .await
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            crate::chaos_fail!("archive-corruption: could not resolve the aeron image on ingress-0")
        })?;
    crate::log(format!(
        "archive-corruption: draining ingress-0 node ({node_id})"
    ));
    // The undrain runs on every path after the drain post: a drained
    // node keeps the ingress job at one replica and the aeron system
    // job off the node for the rest of the shard.
    let drained = match h.nomad.drain(&node_id, true, DRAIN_DEADLINE).await {
        Ok(()) => {
            ArchiveTool {
                h,
                node: &victim,
                image: &image,
            }
            .corrupt_and_heal(&mirror, &rerep)
            .await
        }
        Err(e) => Err(crate::chaos_fail!(
            "archive-corruption: drain enable failed: {e}"
        )),
    };
    let undrained = h
        .nomad
        .drain(&node_id, false, Duration::ZERO)
        .await
        .map_err(|e| crate::chaos_fail!("archive-corruption: drain disable failed: {e}"));
    if let Err(e) = &undrained {
        crate::log(format!("archive-corruption: fault cleanup fails: {e:#}"));
    }
    drained?;
    undrained?;
    h.assert_count("aeron", base, h.knobs.driver_restart_slo())
        .await?;
    h.assert_count("ingress", 2, h.knobs.reschedule_slo).await?;
    h.assert_ingress_pair_live("archive-corruption").await
}

/// The deadline of the ingress-0 drain. Nomad stops every allocation
/// of the node by then. The ingress cannot move to another node, so the
/// drain always runs to its deadline, and only then does Nomad stop the
/// system jobs of the node, the archive daemon among them.
const DRAIN_DEADLINE: Duration = Duration::from_secs(10);

/// Aeron's `ArchiveTool` in a one-off container on the node, while the
/// daemon is down.
struct ArchiveTool<'a> {
    h: &'a Harness,
    node: &'a str,
    image: &'a str,
}

impl ArchiveTool<'_> {
    /// Wait until the archive daemon of the drained node runs no more,
    /// flip bytes in a clean segment, prove the detection, heal the
    /// segment from `mirror` with `rerep`, and prove a clean verify.
    async fn corrupt_and_heal(&self, mirror: &str, rerep: &Path) -> anyhow::Result<()> {
        self.await_daemon_down().await?;
        let seg = pick_clean_segment(self, mirror).await?;
        crate::log(format!(
            "archive-corruption: flipping payload bytes at {} in {} (recording {}; mirror {})",
            seg.flip_at, seg.name, seg.rid, seg.mirror_name
        ));
        self.h
            .nodes
            .exec(
                self.node,
                &format!(
                    "printf 'KARDAMOM-CHAOS!!' | dd of={ARCHIVE_DIR}/{} bs=1 seek={} count=16 conv=notrunc status=none",
                    seg.name, seg.flip_at
                ),
            )
            .await
            .map_err(|e| crate::chaos_fail!("archive-corruption: byte flip failed: {e}"))?;
        let pre = self.verify(&seg.rid).await;
        anyhow::ensure!(
            !pre.contains("Exception"),
            "{}: archive-corruption: verify tool crashed (not a detection)\n{}",
            crate::FAIL_PREFIX,
            tail(&pre, 20)
        );
        anyhow::ensure!(
            recording_err(&pre, &seg.rid),
            "{}: archive-corruption: CRC-armed verify did NOT flag recording {} (detection hole)\n{}",
            crate::FAIL_PREFIX,
            seg.rid,
            tail(&pre, 20)
        );
        crate::log("archive-corruption: corruption detected by CRC-armed verify");
        heal::Repair {
            harness: self.h,
            tool: rerep,
            victim: self.node,
            mirror,
            segment: &seg,
        }
        .run()
        .await?;
        self.mark_valid(&seg.rid).await;
        let post = self.verify(&seg.rid).await;
        anyhow::ensure!(
            !post.contains("Exception")
                && recording_ok(&post, &seg.rid)
                && !recording_err(&post, &seg.rid),
            "{}: archive-corruption: post-heal verify is not clean for recording {}\n{}",
            crate::FAIL_PREFIX,
            seg.rid,
            tail(&post, 20)
        );
        crate::log("archive-corruption: healed + CRC verify clean; undraining ingress-0");
        Ok(())
    }

    /// Wait until no archive daemon container runs on the node. The
    /// drain post only asks Nomad to stop the allocations, and the tool
    /// must not read or write a live archive.
    async fn await_daemon_down(&self) -> anyhow::Result<()> {
        let script = format!(
            "names=$(docker ps --format '{{{{.Names}}}}') && ! printf '%s\\n' \"$names\" | grep -q '^{DRIVER_TASK}'"
        );
        let script = script.as_str();
        let ((), elapsed) = poll::until(
            Budget::new(
                DRAIN_DEADLINE + Duration::from_secs(30),
                Duration::from_secs(2),
            ),
            |_| async move {
                Ok(self
                    .h
                    .nodes
                    .exec_status(self.node, script)
                    .await?
                    .then_some(()))
            },
        )
        .await?
        .or_fail(|t| {
            crate::chaos_fail!(
                "archive-corruption: the {DRIVER_TASK} on {} still runs {}s after the drain",
                self.node,
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "archive-corruption: the {DRIVER_TASK} on {} stopped {}s after the drain",
            self.node,
            elapsed.as_secs()
        ));
        Ok(())
    }

    async fn run(&self, args: &str) -> String {
        let script = format!(
            "docker run --rm -v /opt/kardamom/archive:/opt/kardamom/archive --entrypoint java {} \
             --add-opens java.base/java.util.zip=ALL-UNNAMED \
             -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool {ARCHIVE_DIR} {args} 2>&1",
            self.image
        );
        self.h
            .nodes
            .exec_long(self.node, &script)
            .await
            .unwrap_or_default()
    }

    async fn verify(&self, rid: &str) -> String {
        self.run(&format!(
            "verify {rid} -a -checksum io.aeron.archive.checksum.Crc32"
        ))
        .await
    }

    async fn mark_valid(&self, rid: &str) {
        let _ = self.run(&format!("mark-valid {rid}")).await;
    }
}

fn recording_ok(out: &str, rid: &str) -> bool {
    out.contains(&format!("recordingId={rid}) OK"))
}

fn recording_err(out: &str, rid: &str) -> bool {
    let prefix = format!("recordingId={rid}");
    out.lines().any(|l| {
        l.split(&prefix).nth(1).is_some_and(|rest| {
            (rest.starts_with(',') || rest.starts_with(')')) && rest.contains(" ERR")
        })
    })
}

#[cfg(test)]
mod tests;
