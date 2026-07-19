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
    restart {
      attempts = 5
      interval = "10m"
      delay    = "15s"
      mode     = "fail"
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
        image        = "192.168.56.10:5000/kardamom-validator:dev"
        # Always pull the freshly-built image (mutable :dev tag; see executor job).
        force_pull   = true
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/state:/opt/kardamom/state",
        ]
        args = [
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
          # Archive replay-merge endpoint for tx_data / tx_deposits. When this
          # flag is set the streams are ALWAYS read via replay-merge (fresh
          # starts included) — the exec thread skip-counts any already-committed
          # prefix, and a fresh validator behind a loaded chain needs the
          # historical envelopes live multicast no longer carries. tx_ordering
          # recovery rides the cluster replay instead. Port 40131 on this
          # node's IP (the executor's replay endpoint uses 40130 on ITS nodes;
          # no co-residence, but keep them distinct anyway).
          "--replay-destination-endpoint", "${meta.node_ip}:40131",
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
        ]
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
