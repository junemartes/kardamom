# kardamom-validator — monolithic follower: subscribes to the same canonical
# streams the executors read (tx_data × M, tx_ordering from the Aeron Cluster
# egress, tx_deposits), re-executes every block through the shared engine,
# advances a canonical Ethereum MPT state root, and cross-checks itself against
# the executors' tx_receipts + per-block tx_bal (BAL). Fail-stops (exit 2) on a
# proven divergence — a dead validator alloc IS the divergence signal, so the
# job deliberately does NOT restart on failure.
#
# Placement: one validator on the AUX node (da-watcher/batcher tier). It only
# needs the node-local Aeron media driver + the cluster NIC — any worker-tier
# node works — and the aux node is deliberately OUTSIDE the chaos suite's blast
# radius: the executor chaos cases kill executor tasks/nodes (node-failure kills
# the executor-2 node outright), and a validator co-located there dies as
# collateral, indistinguishable from a fail-stop. Ports on the aux node:
# cluster egress 40230, metrics 9006 (no conflicts; no executor co-resident).
#
# NOTE: this job uses file() for its templates, so submit it from the
# deploy/cluster/ directory (scripts/deploy.sh does this).

# Digest-pinned image (attested-identity P0.1): scripts/deploy.sh passes the
# repo:tag@sha256:... reference captured at push time (deploy/cluster/
# images.digests). The empty default falls back to the mutable :dev tag in
# the task config — a dev affordance for manual `nomad job run` during
# debugging, NOT a production path.
variable "image_ref" {
  type        = string
  description = "Digest-pinned image reference (repo:tag@sha256:...) from the deploy's push manifest. Empty = mutable :dev tag fallback (dev-only)."
  default     = ""
}

# Epoch verification (phase D). BOTH must be set to enable it: the validator
# then re-derives every epoch from L1 and fail-stops on disagreement. Left
# empty, it still enforces the origin SEQUENCE (rules 1-2, which need no L1)
# but cannot check an epoch's contents.
#
# Point l1_rpc_url at the LIGHT CLIENT (l1-light-client.nomad.hcl), not at an
# upstream RPC: a plain endpoint is blindly trusted, and the whole value of
# deriving from L1 is that it is a source the sequencer cannot influence
# (issue #163).
variable "l1_rpc_url" {
  type        = string
  description = "Verified L1 JSON-RPC — the light client's endpoint. Empty disables epoch CONTENT verification."
  default     = ""
}

variable "lockbox_address" {
  type        = string
  description = "L1 ETHLockbox proxy, used to select DepositInitiated logs when re-deriving epochs. Empty disables epoch CONTENT verification."
  default     = ""
}

job "validator" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "aux"
  }

  group "validator" {
    count = 1

    # Restarts are the validator's RECOVERY loop, not a signal-eraser: a lost
    # tx_data envelope (multicast image lapse while lagging a load burst)
    # fail-stops via the bounded join timeout, and the restart resumes from
    # the persisted cursor through crash recovery — tx_ordering rides the
    # cluster replay, the envelope gap replays from the archive recorders.
    # Divergence fail-stops remain detectable across restarts by their
    # 'halted on divergence' line in the alloc log (the e2e verdict greps it);
    # they will crash-loop here, which is correct — a diverged validator must
    # not be quietly absorbed.
    #
    # mode=delay, NOT fail: the replay-unavailable recovery is a RACE against
    # the advancing retention floor — fetch checkpoint, exit(1), restart,
    # adopt, catch up — and it only wins when
    #   recovery_latency < retention_window (= retention_frames / frame_rate).
    # Measured on the dev host: recovery ≈ 42s; at retention 6144 that flips
    # to a losing race above ~150 tps of frames, and each losing cycle
    # (~80s/rev, reproduced 2026-08-05) burns one restart attempt while
    # resolving NOTHING — mode=fail then kills the job on the 5th, which is
    # how the 2026-08-04 CI retention shard produced a validator corpse ~8min
    # after a PASSING suite. The race self-resolves the moment load abates
    # (the window widens to minutes at idle) — a derived, off-hot-path
    # service should wait that out, not die. mode=delay parks for the rest of
    # the interval after the 5th attempt and keeps trying; treadmill cycles
    # stay visible via validator_resync_total{outcome="peer-checkpoint"}
    # (one increment per revolution — alert on its rate, not on job death).
    restart {
      attempts = 5
      interval = "10m"
      delay    = "15s"
      mode     = "delay"
    }

    reschedule {
      attempts  = 0
      unlimited = false
    }

    network {
      mode = "host"
    }

    task "validator" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "192.168.56.10:5000/kardamom-validator:dev"
        # force_pull kept for both paths (see the ingress job's comment):
        # the :dev fallback needs it; on the pinned path the 1.9.5 driver
        # pulls the tag but resolves the image by digest, so the pin holds.
        force_pull = true
        # Read-only rootfs (attested-identity P0.3): the validator's writable
        # surfaces — its state dir under the shared mount, the checkpoint
        # staging dir, the aeron dir — are all explicit bind mounts below,
        # plus Nomad's alloc/local/secrets mounts. Validated by cluster-e2e.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/state:/opt/kardamom/state",
          # Adoption-only checkpoint staging (#143): peer checkpoints fetched
          # here when the cluster refuses replay (cursor below the retention
          # floor); adopted on the next start. The validator never creates
          # checkpoints. Blocks through an adopted checkpoint are UNVERIFIED
          # by this validator (trust class of the #78 catch-up).
          "/opt/kardamom/checkpoints:/opt/kardamom/checkpoints",
        ]
        args = concat([
          # Seeded parallel batch re-execution from the EIP-7928 BAL
          # (falls back to sequential per block when claims are absent).
          "--parallel-validation",
          "--config", "/local/validator.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          # This node's cluster-egress (response) endpoint for the validator's
          # OWN cluster client session. Port 40230 — kept distinct from 40210,
          # the executors' egress port convention on THEIR nodes (nothing else
          # binds either port on the aux node; distinct ports keep captures /
          # debugging unambiguous).
          "--cluster-egress-endpoint", "${meta.node_ip}:40230",
          "--shards", "2",
          "--chain-id", "412346",
          "--chain", "/local/genesis.toml",
          # Own state dir UNDER the shared persistent mount — never the
          # executor's /opt/kardamom/state root (separate mdbx env).
          "--state-dir", "/opt/kardamom/state/validator",
          # Join-miss archive refetch (tx_data / tx_deposits): a canonical ref
          # whose envelope the live multicast missed is replayed in-band from
          # the durability archives listed in channels.toml. Replayed fragments
          # land on 40131, archive-control responses on 40141 (the executor
          # uses 40130/40140 on ITS nodes; no co-residence, but keep them
          # distinct anyway). tx_ordering recovery rides the cluster replay.
          "--replay-destination-endpoint", "${meta.node_ip}:40131",
          "--archive-control-response-endpoint", "${meta.node_ip}:40141",
          # Replay-unavailable fallback (#143): fetch a peer checkpoint from
          # the executors' serve endpoints and adopt it on restart, exactly
          # like the executors' recovery-D loop.
          "--checkpoint-dir", "/opt/kardamom/checkpoints",
          "--checkpoint-peers", "192.168.56.41:9014,192.168.56.42:9014,192.168.56.43:9014",
          # Shadow-check the node-incremental state trie against a full rebuild
          # every 8th block; a walker bug fail-stops the validator (dead alloc =
          # verdict failure) and bumps kardamom_state_trie_shadow_mismatch_total.
          # Every-block checking is exhaustive but a full rebuild per 250ms block
          # saturates a CI core continuously — on the 4-core DinD runner that
          # starves the Aeron cluster's timing-sensitive recovery paths (the
          # walker's per-block equivalence is separately unit-tested with 50
          # randomized blocks; cadence 8 still shadow-checks hundreds of blocks
          # per e2e run under real load).
          "--trie-shadow-check", "8",
          ],
          # Epoch verification against L1 (phase D). Appended ONLY when both
          # are configured — the validator requires --lockbox to parse as an
          # address, so passing it empty would break every deploy that has not
          # opted in. Unset, the validator still enforces the origin SEQUENCE;
          # only the content check is off.
          var.l1_rpc_url == "" || var.lockbox_address == "" ? [] : [
            "--l1-rpc-url", var.l1_rpc_url,
            "--lockbox", var.lockbox_address,
        ])
      }

      env {
        # Validator metrics on 9006 (executor holds 9004 on the same host).
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9006"
      }

      # Cluster LogConfig (UDP multicast channels), consumed via --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      # [cluster] ingress endpoints (same contract as the executor's config).
      template {
        destination = "local/validator.toml"
        data        = file("config/executor.toml")
      }

      # Chain genesis, single-sourced from config/genesis/dev.toml.
      template {
        destination = "local/genesis.toml"
        data        = file("config/genesis/dev.toml")
      }

      resources {
        cpu    = 800
        memory = 1024
      }
    }
  }
}
