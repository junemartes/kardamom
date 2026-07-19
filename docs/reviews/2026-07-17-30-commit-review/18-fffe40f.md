# 18 fffe40f — fix(ci): drop removed kardamom-recorder from cluster-e2e build

## Summary of change
One-line workflow fix: the cluster-e2e job's `cargo build --release` step still passed `-p kardamom-recorder` after the recorder crate was removed (durability moved to archive-at-the-sealer), so the build step failed with "package ID specification ... did not match any packages". The commit drops the flag; the remaining `-p` list matches the binaries the job actually deploys.

## Findings

### F18.1 [low] [quality] — deploy/cluster/README.md still documents the removed kardamom-recorder as live topology
- **Where**: deploy/cluster/README.md:21-28, 110-112, 143, 155-185 (same at the commit and at HEAD)
- **What**: Pre-existing drift adjacent to this cleanup: the README still describes r1/r2/r3 running `kardamom-recorder` processes, the recorder/quorum nomad specs (`recorder.system.nomad.hcl`, `quorum.nomad.hcl` — files that no longer exist under deploy/cluster/nomad/), and quorum watermark aggregation as the current architecture, while the Makefile, ci-cluster.sh, and this workflow all state the recorder was removed. This commit removed the last *build* reference but left the primary deployment doc describing a component that cannot be built.
- **Still present at HEAD**: yes
- **Suggested fix**: Update deploy/cluster/README.md's node table, nomad-spec listing, and status sections to the archive-at-the-sealer design (or point to the doc that describes it).

## Verdict
The change itself is minimal and correct — verified that no workflow, Makefile, or nomad spec still tries to build or run `kardamom-recorder` (only explanatory comments remain), and the executor's `--recorder-id` flag mentioned in the message is indeed an unrelated, still-valid knob. The one finding is pre-existing docs drift the commit chose not to touch, not a defect it introduced.
