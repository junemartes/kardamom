//! The pick of the victim segment of `archive-corruption`: a segment that
//! the mirror holds byte-identical, that carries a data frame, and whose
//! recording verifies clean.

use super::{ARCHIVE_DIR, ArchiveTool, recording_ok};
use crate::harness::Harness;

/// A victim segment: clean at baseline, with a usable data frame to flip
/// inside, and the name of the mirror segment that holds the same bytes.
pub(super) struct Segment {
    pub(super) name: String,
    /// Recording ids are per archive. A recording that started after an
    /// adoption has a different id on each node, so the mirror can hold
    /// the victim's bytes under another name.
    pub(super) mirror_name: String,
    pub(super) rid: String,
    pub(super) flip_at: i64,
}

/// The sha256 and name of archive segments on one node, in listing order.
pub(super) struct SegmentHashes(Vec<(String, String)>);

impl SegmentHashes {
    /// Hash the segments `listing` names, a shell word list run in the
    /// archive directory. Hashing every preallocated segment takes longer
    /// than the short exec bound after a few cases (about 25 s for 4.6 GB
    /// on a host run), so this read uses the long bound.
    pub(super) async fn read(h: &Harness, node: &str, listing: &str) -> Self {
        let script = format!("cd {ARCHIVE_DIR} && {listing} | xargs -r sha256sum");
        let out = h.nodes.exec_long(node, &script).await.unwrap_or_default();
        Self::parse(&out)
    }

    pub(super) fn parse(out: &str) -> Self {
        Self(
            out.lines()
                .filter_map(|l| l.split_once("  "))
                .map(|(hash, name)| (hash.to_string(), name.to_string()))
                .collect(),
        )
    }

    /// The name of a segment with `hash`: `name` itself when it matches,
    /// or else the first other segment with those bytes.
    pub(super) fn name_for(&self, hash: &str, name: &str) -> Option<String> {
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
///
/// Most segments hold the same bytes under the same name on both nodes,
/// so the pick hashes the segments of one shared name at a time first,
/// and stops at the first one that qualifies. It hashes every segment of
/// both nodes only when no shared name qualifies.
pub(super) async fn pick_clean_segment(
    tool: &ArchiveTool<'_>,
    mirror: &str,
) -> anyhow::Result<Segment> {
    let listing = "ls *.rec 2>/dev/null";
    let (victim, mirrored) = tokio::join!(
        SegmentNames::read(tool.h, tool.node, listing),
        SegmentNames::read(tool.h, mirror, listing)
    );
    crate::log(format!(
        "archive-corruption: {} victim candidates, {} mirror segments",
        victim.0.len(),
        mirrored.0.len()
    ));
    let pick = SegmentPick { tool, mirror };
    for name in victim.0.iter().filter(|name| mirrored.0.contains(name)) {
        if let Some(seg) = pick.same_name(name).await {
            return Ok(seg);
        }
    }
    pick.by_content().await
}

/// The search for a victim segment on the node of `tool`, with `mirror`
/// as the node that holds the heal copy.
struct SegmentPick<'a, 't> {
    tool: &'a ArchiveTool<'t>,
    mirror: &'a str,
}

impl SegmentPick<'_, '_> {
    /// `name` as a victim segment when the mirror holds the same bytes
    /// under the same name.
    async fn same_name(&self, name: &str) -> Option<Segment> {
        let listing = format!("echo {name}");
        let (victim, mirrored) = tokio::join!(
            SegmentHashes::read(self.tool.h, self.tool.node, &listing),
            SegmentHashes::read(self.tool.h, self.mirror, &listing)
        );
        let hash = &victim.0.first()?.0;
        let mirror_name = mirrored.name_for(hash, name)?;
        qualify(self.tool, name.to_string(), mirror_name).await
    }

    /// The match by content over every segment of both nodes.
    async fn by_content(&self) -> anyhow::Result<Segment> {
        let listing = "ls *.rec 2>/dev/null";
        let (victim, mirror) = tokio::join!(
            SegmentHashes::read(self.tool.h, self.tool.node, listing),
            SegmentHashes::read(self.tool.h, self.mirror, listing)
        );
        for (hash, name) in victim.0 {
            if let Some(seg) = self.by_hash(&mirror, &hash, name).await {
                return Ok(seg);
            }
        }
        Err(crate::chaos_fail!(
            "archive-corruption: no recording verifies clean at baseline (inherited catalog damage too broad)"
        ))
    }

    /// `name` as a victim segment when the mirror holds its bytes under
    /// any name.
    async fn by_hash(&self, mirror: &SegmentHashes, hash: &str, name: String) -> Option<Segment> {
        let mirror_name = mirror.name_for(hash, &name)?;
        qualify(self.tool, name, mirror_name).await
    }
}

/// The segment names of one node, in listing order.
struct SegmentNames(Vec<String>);

impl SegmentNames {
    async fn read(h: &Harness, node: &str, listing: &str) -> Self {
        let script = format!("cd {ARCHIVE_DIR} && {listing}");
        let out = h.nodes.exec(node, &script).await.unwrap_or_default();
        Self(out.lines().map(str::to_string).collect())
    }
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
