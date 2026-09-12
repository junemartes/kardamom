//! The Aeron archive cases: substrate loss, data loss with re-replication
//! from the mirror, and segment corruption with detect-and-heal.

use std::cell::RefCell;
use std::path::Path;
use std::time::Duration;

use anyhow::Context;

use crate::harness::Harness;
use crate::poll::{self, Budget};

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
    h.assert_count("aeron", base, h.knobs.restart_slo).await?;
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
    let segments = h
        .nodes
        .exec_bytes(
            &mirror,
            "tar -C /opt/kardamom/archive --warning=no-file-changed --exclude='dir/archive-mark.dat' --exclude='dir/archive.catalog' -cf - dir",
            1,
        )
        .await
        .map_err(|e| crate::chaos_fail!("archive-tx-data-wipe: re-replication copy failed: {e}"))?;
    h.nodes
        .exec_with_stdin(&victim, "tar -C /opt/kardamom/archive -xf -", segments)
        .await
        .map_err(|e| crate::chaos_fail!("archive-tx-data-wipe: re-replication copy failed: {e}"))?;
    h.assert_progress().await?;
    h.assert_count("aeron", base, h.knobs.restart_slo).await?;
    verify_restored(h, &victim).await?;
    h.assert_count("ingress", 2, h.knobs.reschedule_slo).await?;
    h.assert_ingress_pair_live("archive-tx-data-wipe").await
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

/// A CRC-armed verify inside the restarted daemon container: every
/// recording OK, no exception, no other error. Stale catalog entry
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
        "docker exec {daemon} bash -lc 'java --add-opens java.base/java.util.zip=ALL-UNNAMED -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool {ARCHIVE_DIR} {CRC_VERIFY} 2>&1'"
    );
    let last = RefCell::new(String::new());
    let (last_ref, script_ref) = (&last, script.as_str());
    let outcome = poll::until(Budget::secs(10, 5), |_| async move {
        let out = h.nodes.exec(victim, script_ref).await.unwrap_or_default();
        let ok = verify_is_clean(&out);
        *last_ref.borrow_mut() = out;
        Ok(ok.then_some(()))
    })
    .await?;
    let last = last.into_inner();
    if outcome.or_fail(|_| anyhow::anyhow!("verify")).is_err() {
        eprintln!("{}", tail(&last, 20));
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
            u.contains("ERR ") || u.contains("FAILED")
        })
        .count();
    !out.contains("Exception") && has_ok_recording(out) && other_err == 0
}

fn has_ok_recording(out: &str) -> bool {
    out.lines()
        .any(|l| l.contains("recordingId=") && l.trim_end().ends_with("OK"))
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
        .drain(&node_id, true, Duration::from_secs(120))
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
        "archive-corruption: flipping payload bytes at {} in {} (recording {})",
        seg.flip_at, seg.name, seg.rid
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
    heal_on_runner(h, &rerep, &victim, &mirror, &seg.name).await?;
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
    h.assert_count("aeron", base, h.knobs.restart_slo).await?;
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
            .exec(self.node, &script)
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

/// A victim segment: present on both archives, clean at baseline, with
/// a usable data frame to flip inside.
struct Segment {
    name: String,
    rid: String,
    flip_at: i64,
}

/// The 48 largest segments on the victim, by name. Only the active
/// lanes carry data frames, so the window covers idle lanes and every
/// per-restart session.
async fn candidates(h: &Harness, victim: &str) -> anyhow::Result<Vec<String>> {
    let listing = h
        .nodes
        .exec(
            victim,
            &format!("ls -S {ARCHIVE_DIR}/*.rec 2>/dev/null | head -48 | xargs -rn1 basename"),
        )
        .await
        .unwrap_or_default();
    Ok(listing.lines().map(str::to_string).collect())
}

async fn pick_clean_segment(tool: &ArchiveTool<'_>, mirror: &str) -> anyhow::Result<Segment> {
    let names = candidates(tool.h, tool.node).await?;
    for name in names {
        if let Some(seg) = qualify(tool, mirror, name).await? {
            return Ok(seg);
        }
    }
    Err(crate::chaos_fail!(
        "archive-corruption: no recording verifies clean at baseline (inherited catalog damage too broad)"
    ))
}

/// Whether one candidate qualifies. Recording ids are per-archive, so a
/// victim-only segment cannot be healed from the mirror. A candidate
/// that fails the probe verify gets its state put back.
async fn qualify(
    tool: &ArchiveTool<'_>,
    mirror: &str,
    name: String,
) -> anyhow::Result<Option<Segment>> {
    let rid = name.split('-').next().unwrap_or("").to_string();
    let on_mirror = tool
        .h
        .nodes
        .exec_status(mirror, &format!("test -f {ARCHIVE_DIR}/{name}"))
        .await?;
    if !on_mirror {
        return Ok(None);
    }
    let out = tool.verify(&rid).await;
    if !recording_ok(&out, &rid) || out.contains(") ERR") {
        tool.mark_valid(&rid).await;
        return Ok(None);
    }
    let flip_at = frame_payload_offset(tool.h, tool.node, &name).await;
    Ok((flip_at >= 0).then_some(Segment { name, rid, flip_at }))
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

/// Stage both archives on the runner, require `--diff` to name the
/// segment, heal it, and write only the healed segment back.
async fn heal_on_runner(
    h: &Harness,
    rerep: &Path,
    victim: &str,
    mirror: &str,
    seg: &str,
) -> anyhow::Result<()> {
    let tmp = tempfile::tempdir().context("staging dir")?;
    for (node, sub) in [(victim, "victim"), (mirror, "mirror")] {
        stage_archive(h, node, &tmp.path().join(sub)).await?;
    }
    let source = tmp.path().join("mirror/dir");
    let dest = tmp.path().join("victim/dir");
    let diff = run_tool(
        rerep,
        &[
            "--diff",
            "--source-dir",
            path(&source),
            "--dest-dir",
            path(&dest),
        ],
    )
    .await;
    anyhow::ensure!(
        diff.contains(seg),
        "{}: archive-corruption: --diff did not name the corrupted segment {seg}",
        crate::FAIL_PREFIX
    );
    let heal = run_tool(
        rerep,
        &[
            "--heal",
            "--segments",
            seg,
            "--no-verify",
            "--source-dir",
            path(&source),
            "--dest-dir",
            path(&dest),
        ],
    )
    .await;
    anyhow::ensure!(
        heal.contains("healed segments=1"),
        "{}: archive-corruption: --heal did not repair the segment\n{}",
        crate::FAIL_PREFIX,
        tail(&heal, 20)
    );
    let healed = std::fs::read(dest.join(seg)).context("read the healed segment")?;
    h.nodes
        .exec_with_stdin(victim, &format!("cat > {ARCHIVE_DIR}/{seg}"), healed)
        .await
        .map_err(|e| {
            crate::chaos_fail!("archive-corruption: writing healed segment back failed: {e}")
        })
}

fn path(p: &Path) -> &str {
    p.to_str().unwrap_or("")
}

async fn stage_archive(h: &Harness, node: &str, into: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(into).context("create staging dir")?;
    let tar = h
        .nodes
        .exec_bytes(node, "tar -C /opt/kardamom/archive -cf - dir", 1)
        .await
        .map_err(|e| {
            crate::chaos_fail!("archive-corruption: staging copy from {node} failed: {e}")
        })?;
    untar_into(into, tar).await
}

/// Extract a tar stream into a runner directory.
async fn untar_into(into: &Path, tar: Vec<u8>) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut child = tokio::process::Command::new("tar")
        .args(["-C", path(into), "-xf", "-"])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .context("spawn tar on the runner")?;
    let mut stdin = child.stdin.take().context("tar stdin")?;
    stdin.write_all(&tar).await.context("feed tar")?;
    stdin.shutdown().await.context("close tar stdin")?;
    let status = child.wait().await.context("wait for tar")?;
    anyhow::ensure!(
        status.success(),
        "tar extraction into {} failed: {status}",
        into.display()
    );
    Ok(())
}

async fn run_tool(tool: &Path, args: &[&str]) -> String {
    tokio::process::Command::new(tool)
        .args(args)
        .output()
        .await
        .map(|o| {
            format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            )
        })
        .unwrap_or_default()
}
