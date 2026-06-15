# kardamom-batcher — OFFLINE / dry-run today. Modeled as a Nomad *periodic*
# (batch) job, NOT an always-on service.
#
# The batcher currently only inspects Aeron Archive segment files in --dry-run
# (the binary defaults dry_run=true and warns "live L1 broadcast path not yet
# wired; falling back to dry-run" — see crates/batcher/src/bin/kardamom-batcher.rs
# and README "Required service changes" item 2). The live L1-broadcast path is
# UNWIRED; wiring it is a follow-up.
#
# Invocation (offline, from the spec):
#   kardamom-batcher --channel-b-segment <path> \
#       --channel-a-archive "0=<path>,1=<path>" --dry-run
#
# Flag spelling: the binary uses clap `#[arg(long)]` on fields
# channel_b_segment / channel_a_archive, which clap renders as the kebab-case
# flags --channel-b-segment / --channel-a-archive (channel_b_segment also
# accepts the alias --segment). The archive segments live under
# paths.archive_dir (/opt/kardamom/archive) on the recorder nodes; this job is
# pinned to a recorder (r1) and mounts that archive dir read-only.
#
# ASSUMPTION / RISK: the exact .rec segment filenames under the archive dir are
# produced at runtime by the Aeron Archive and are not known statically. The
# paths below are PLACEHOLDERS pointing at the archive root; a real run must
# resolve the actual tx_ordering (channel B) and per-sequencer tx_data
# (channel A) segment files. Since the job is --dry-run only, this is inert
# today.

job "batcher" {
  datacenters = ["dc1"]
  type        = "batch"

  # Run hourly. The batcher is dry-run/offline, so this is just a periodic
  # archive inspection until the live L1 broadcast path is wired.
  periodic {
    crons            = ["0 * * * *"]
    prohibit_overlap = true
  }

  # Pin to recorder r1, which holds the Aeron Archive segment files.
  constraint {
    attribute = "${meta.role}"
    value     = "aux"
  }

  group "batcher" {
    count = 1

    network {
      mode = "host"
    }

    task "batcher" {
      driver = "docker"

      config {
        image        = "192.168.56.10:5000/kardamom-batcher:dev"
        # Always pull the freshly-built image: the mutable :dev tag would otherwise
        # let Nomad reuse a stale node-cached layer across rebuilds (caused a
        # crash-retry storm that stalled the deploy).
        force_pull    = true
        network_mode = "host"
        # Archive segments (read-only); the batcher only reads them.
        volumes = [
          "/opt/kardamom/archive:/opt/kardamom/archive:ro",
        ]
        args = [
          # PLACEHOLDER segment paths under the archive dir — see header note.
          "--channel-b-segment", "/opt/kardamom/archive/dir/tx-ordering.rec",
          "--channel-a-archive", "0=/opt/kardamom/archive/dir/tx-data-0.rec,1=/opt/kardamom/archive/dir/tx-data-1.rec",
          "--dry-run",
        ]
      }

      resources {
        cpu    = 300
        memory = 256
      }
    }
  }
}
