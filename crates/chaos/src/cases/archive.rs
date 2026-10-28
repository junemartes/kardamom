//! The Aeron archive cases: substrate loss, data loss with re-replication
//! from the mirror, and segment corruption with detect-and-heal.

use std::cell::RefCell;
use std::time::{Duration, SystemTime};

use crate::harness::Harness;
use crate::nodes::Pipe;
use crate::poll::{self, Budget};

mod heal;
mod staging;

const DRIVER_TASK: &str = "archiving-media-driver";
const ARCHIVE_DIR: &str = "/opt/kardamom/archive/dir";
const CRC_VERIFY: &str = "verify -a -checksum io.aeron.archive.checksum.Crc32";

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
    // Three attempts, as the bash case ran: the verify itself runs
    // under the streaming bound, since a CRC pass over a large archive
    // on a slow runner takes well over the command bound.
    let last = RefCell::new(String::new());
    let (last_ref, script_ref) = (&last, script.as_str());
    let outcome = poll::until(Budget::secs(60, 5), |_| async move {
        let out = h
            .nodes
            .exec_long(victim, script_ref)
            .await
            .unwrap_or_default();
        let ok = verify_is_clean(&out);
        *last_ref.borrow_mut() = out;
        Ok(ok.then_some(()))
    })
    .await?;
    let last = last.into_inner();
    if outcome.or_fail(|_| anyhow::anyhow!("verify")).is_err() {
        crate::log(format!(
            "archive-tx-data-wipe: verify output tail\n{}",
            tail(&last, 20)
        ));
        return Err(crate::chaos_fail!(
            "archive-tx-data-wipe: restored archive failed CRC-armed verify after retries"
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
    h.nomad
        .drain(&node_id, true, Duration::from_mins(2))
        .await
        .map_err(|e| crate::chaos_fail!("archive-corruption: drain enable failed: {e}"))?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let tool = ArchiveTool {
        h,
        node: &victim,
        image: &image,
    };
    let seg = pick_clean_segment(&tool, &mirror).await?;
    crate::log(format!(
        "archive-corruption: flipping payload bytes at {} in {} (recording {}; mirror {})",
        seg.flip_at, seg.name, seg.rid, seg.mirror_name
    ));
    h.nodes
        .exec(
            &victim,
            &format!(
                "printf 'KARDAMOM-CHAOS!!' | dd of={ARCHIVE_DIR}/{} bs=1 seek={} count=16 conv=notrunc status=none",
                seg.name, seg.flip_at
            ),
        )
        .await
        .map_err(|e| crate::chaos_fail!("archive-corruption: byte flip failed: {e}"))?;
    let pre = tool.verify(&seg.rid).await;
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
        harness: h,
        tool: &rerep,
        victim: &victim,
        mirror: &mirror,
        segment: &seg,
    }
    .run()
    .await?;
    tool.mark_valid(&seg.rid).await;
    let post = tool.verify(&seg.rid).await;
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
    h.nomad
        .drain(&node_id, false, Duration::ZERO)
        .await
        .map_err(|e| crate::chaos_fail!("archive-corruption: drain disable failed: {e}"))?;
    h.assert_count("aeron", base, h.knobs.driver_restart_slo())
        .await?;
    h.assert_count("ingress", 2, h.knobs.reschedule_slo).await?;
    h.assert_ingress_pair_live("archive-corruption").await
}

/// Aeron's `ArchiveTool` in a one-off container on the node, while the
/// daemon is down.
struct ArchiveTool<'a> {
    h: &'a Harness,
    node: &'a str,
    image: &'a str,
}

impl ArchiveTool<'_> {
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

/// A victim segment: clean at baseline, with a usable data frame to flip
/// inside, and the name of the mirror segment that holds the same bytes.
struct Segment {
    name: String,
    /// Recording ids are per archive. A recording that started after an
    /// adoption has a different id on each node, so the mirror can hold
    /// the victim's bytes under another name.
    mirror_name: String,
    rid: String,
    flip_at: i64,
}

/// The sha256 and name of archive segments on one node, in listing order.
struct SegmentHashes(Vec<(String, String)>);

impl SegmentHashes {
    /// Hash the segments `listing` names, a shell word list run in the
    /// archive directory. Hashing every preallocated segment takes longer
    /// than the short exec bound after a few cases (about 25 s for 4.6 GB
    /// on a host run), so this read uses the long bound.
    async fn read(h: &Harness, node: &str, listing: &str) -> Self {
        let script = format!("cd {ARCHIVE_DIR} && {listing} | xargs -r sha256sum");
        let out = h.nodes.exec_long(node, &script).await.unwrap_or_default();
        Self::parse(&out)
    }

    fn parse(out: &str) -> Self {
        Self(
            out.lines()
                .filter_map(|l| l.split_once("  "))
                .map(|(hash, name)| (hash.to_string(), name.to_string()))
                .collect(),
        )
    }

    /// The name of a segment with `hash`: `name` itself when it matches,
    /// or else the first other segment with those bytes.
    fn name_for(&self, hash: &str, name: &str) -> Option<String> {
        let same: Vec<&String> = self
            .0
            .iter()
            .filter(|(h, _)| h == hash)
            .map(|(_, n)| n)
            .collect();
        let exact = same.iter().find(|n| n.as_str() == name);
        exact.or(same.first()).map(|n| (*n).clone())
    }
}

/// The first victim segment that the mirror holds byte-identical, that
/// carries a data frame, and that verifies clean. Every segment is a
/// candidate: all segment files share one preallocated length, and a
/// restored segment is fully allocated on disk, so neither length nor disk
/// use ranks the segments that carry data. The mirror match is by content:
/// an adopted recording keeps its id but its catalog entry checksum is
/// stale, so it fails verify, and a recording made after the adoption
/// verifies clean under a different id on each node.
async fn pick_clean_segment(tool: &ArchiveTool<'_>, mirror: &str) -> anyhow::Result<Segment> {
    let victim = SegmentHashes::read(tool.h, tool.node, "ls *.rec 2>/dev/null").await;
    let mirror = SegmentHashes::read(tool.h, mirror, "ls *.rec 2>/dev/null").await;
    crate::log(format!(
        "archive-corruption: {} victim candidates, {} mirror segments",
        victim.0.len(),
        mirror.0.len()
    ));
    for (hash, name) in victim.0 {
        let Some(mirror_name) = mirror.name_for(&hash, &name) else {
            continue;
        };
        if let Some(seg) = qualify(tool, name, mirror_name).await {
            return Ok(seg);
        }
    }
    Err(crate::chaos_fail!(
        "archive-corruption: no recording verifies clean at baseline (inherited catalog damage too broad)"
    ))
}

/// Whether one candidate qualifies. Recording ids are per-archive: after
/// a wipe and a re-replication the mirror's `40-0.rec` can hold another
/// stream than the victim's, and a heal from it writes foreign frames
/// (seen in CI: post-heal verify found streamId=2000 where the victim's
/// recording is stream 2001). So the mirror's segment must be byte-identical
/// to the victim's before the flip; then the heal restores exactly the bytes
/// the flip destroyed. A candidate that fails the probe verify gets its
/// state put back.
/// `name` as a victim segment, when it carries a data frame and its
/// recording verifies clean. The frame check runs first: it is a cheap
/// read, and it skips the idle lanes before a verify starts a JVM.
async fn qualify(tool: &ArchiveTool<'_>, name: String, mirror_name: String) -> Option<Segment> {
    let flip_at = frame_payload_offset(tool.h, tool.node, &name).await;
    if flip_at < 0 {
        return None;
    }
    let rid = name.split('-').next().unwrap_or("").to_string();
    let out = tool.verify(&rid).await;
    if !recording_ok(&out, &rid) || out.contains(") ERR") {
        tool.mark_valid(&rid).await;
        return None;
    }
    Some(Segment {
        name,
        mirror_name,
        rid,
        flip_at,
    })
}

/// A byte offset inside the payload of the largest data frame of a
/// segment, walking the Aeron frame headers so the flip never lands in
/// a header. -1 when the segment has no usable frame.
async fn frame_payload_offset(h: &Harness, node: &str, name: &str) -> i64 {
    let script = format!(
        "python3 -c \"
b = open('{ARCHIVE_DIR}/{name}', 'rb').read()
pos = 0; best = -1; bestlen = 0
while pos + 32 <= len(b):
    ln = int.from_bytes(b[pos:pos+4], 'little', signed=True)
    if ln <= 0:
        break
    typ = int.from_bytes(b[pos+6:pos+8], 'little')
    if typ == 1 and ln >= 96 and pos + ln <= len(b) and ln > bestlen:
        best = pos; bestlen = ln
    pos += (ln + 31) // 32 * 32
print(best + 40 if best >= 0 else -1)
\""
    );
    h.nodes
        .exec(node, &script)
        .await
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mirror_segment_is_found_by_its_bytes() {
        let mirror =
            SegmentHashes::parse("aaa  0-0.rec\nzero  1-0.rec\nbbb  16-0.rec\nzero  2-0.rec\n");
        assert_eq!(
            mirror.name_for("aaa", "0-0.rec").as_deref(),
            Some("0-0.rec")
        );
        assert_eq!(
            mirror.name_for("bbb", "24-0.rec").as_deref(),
            Some("16-0.rec")
        );
        assert_eq!(
            mirror.name_for("zero", "2-0.rec").as_deref(),
            Some("2-0.rec")
        );
        assert_eq!(mirror.name_for("ccc", "24-0.rec"), None);
    }

    #[test]
    fn a_clean_verify_has_an_ok_recording_and_tolerates_stale_entries() {
        let out = "recordingId=3 (checksum: Crc32) OK, length=4096\n\
            ERR: recordingId=4 invalid Catalog checksum\n";
        assert!(verify_is_clean(out));
        assert!(!verify_is_clean(
            "recordingId=3 ... OK\nERR: recordingId=5 file missing\n"
        ));
        assert!(!verify_is_clean(
            "Exception in thread main\nrecordingId=3 OK\n"
        ));
        assert!(!verify_is_clean(""));
    }
}
