# kardamom-validator is a monolithic follower. It subscribes to the
# same canonical streams the executors read: tx_data (times M),
# tx_ordering from the Aeron Cluster egress, and tx_deposits. It
# re-executes every block through the shared engine, advances a
# canonical Ethereum MPT state root, and cross-checks itself against
# the executors' tx_receipts and per-block tx_bal (BAL). It fail-stops
# (exit 2) on a proven divergence. A dead validator alloc is the
# divergence signal, so the job does not restart on failure.
#
# Placement: one validator runs on the aux node (the da-watcher and
# batcher tier). It needs only the node-local Aeron media driver and
# the cluster NIC, so any worker-tier node would work. The aux node is
# deliberately outside the chaos suite's blast radius. The executor
# chaos cases kill executor tasks and nodes (node-failure kills the
# executor-2 node outright), and a validator co-located there would die
# as collateral, indistinguishable from a fail-stop. Ports on the aux
# node: cluster egress on a Nomad dynamic port, metrics 9006. No
# conflicts, since no executor runs on the same node.
#
# This job uses file() for its templates, so submit it from the
# deploy/cluster/ directory. ansible/deploy.yml does this.

# Digest-pinned image. ansible/deploy.yml
# passes the repo:tag@sha256:... reference captured at push time
# (deploy/cluster/images.digests). The empty default falls back to the
# mutable :dev tag in the task config. That fallback is a dev
# affordance for manual `nomad job run` during debugging, not a
# production path.
variable "image_ref" {
  type        = string
  description = "Digest-pinned image reference (repo:tag@sha256:...) from the deploy's push manifest. Empty = mutable :dev tag fallback (dev-only)."
  default     = ""
}

# Epoch verification (phase D). Both variables must be set to enable
# it. The validator then re-derives every epoch from L1, and
# fail-stops on disagreement. Left empty, it still enforces the origin
# sequence (rules 1-2, which need no L1), but cannot check an epoch's
# contents.
#
# Point l1_rpc_url at the light client (l1-light-client.nomad.hcl), not
# at an upstream RPC. A plain endpoint is blindly trusted, and the
# whole value of deriving from L1 is that it is a source the sequencer
# cannot influence.
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

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

variable "executor_count" {
  type        = number
  description = "The executor node count (node_classes.executor.count). The checkpoint peers are executor-<i>.node.<datacenter>.consul."
  default     = 3
}

job "validator" {
  datacenters = [var.datacenter]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "aux"
  }

  group "validator" {
    count = 1

    # Restarts are the validator's recovery loop, not a way to erase a
    # signal. A lost tx_data envelope, from a multicast image lapse
    # while lagging a load burst, fail-stops through the bounded join
    # timeout. The restart resumes from the persisted cursor through
    # crash recovery: tx_ordering rides the cluster replay, and the
    # envelope gap replays from the archive recorders. Divergence
    # fail-stops stay detectable across restarts, through their
    # 'halted on divergence' line in the alloc log, which the e2e
    # verdict greps for. They will crash-loop here, which is correct;
    # a diverged validator must not be quietly absorbed.
    #
    # This uses mode=delay, not mode=fail. The replay-unavailable
    # recovery races the advancing retention floor: fetch checkpoint,
    # exit(1), restart, adopt, catch up. It only wins when
    #   recovery_latency < retention_window (= retention_frames / frame_rate).
    # A refused replay costs one revolution of the treadmill: fetch a
    # peer checkpoint (about 4s for 256 MB), halt, wait the restart
    # delay, adopt, ask for replay from the checkpoint's index. The
    # checkpoint is up to 20s old (the executors' interval). At
    # retention 6144 and 230 tps the window is 26s, so a revolution with
    # a 15s delay lost the race every time, and the fifth loss parked
    # the validator for the rest of the interval (seen in CI: "Exceeded
    # allowed attempts, applying a delay - Task restarting in 7m36s",
    # while the race had already resolved once load eased). A 5s delay
    # keeps a revolution near 10s plus the checkpoint age, and ten
    # attempts give the race room to resolve before the park. mode=fail
    # would kill the job instead, even though the window widens to
    # minutes at idle; a derived, off-hot-path service waits that out.
    # Treadmill cycles stay visible through
    # validator_resync_total{outcome="peer-checkpoint"}, one increment
    # per revolution; alert on its rate, not on job death. The in-process
    # adoption that removes the restart from the revolution is issue #298.
    restart {
      attempts = 10
      interval = "10m"
      delay    = "5s"
      mode     = "delay"
    }

    reschedule {
      attempts  = 0
      unlimited = false
    }

    network {
      mode = "host"
      # The cluster egress (response) port, unique per allocation. A
      # fixed port sat in the node's ephemeral range, where the shared
      # media driver's port-0 discovery sockets could take it first.
      port "egress" {}
      # The join-miss refetch ports: replayed fragments and archive
      # control responses. Nomad picks them per allocation, below the
      # ephemeral range, so no other process on the node holds them.
      port "replay" {}
      port "archive_response" {}
    }

    task "validator" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-validator:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it. On the pinned path, the
        # 1.9.5 driver pulls the tag but resolves the image by digest,
        # so the pin holds.
        force_pull = true
        # Read-only rootfs. The validator's
        # writable surfaces are its state directory under the shared
        # mount, the checkpoint staging directory, and the aeron
        # directory. All are explicit bind mounts below, plus Nomad's
        # alloc, local, and secrets mounts. cluster-e2e validates this.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/state:/opt/kardamom/state",
          # Adoption-only checkpoint staging. Peer checkpoints land
          # here when the cluster refuses replay, because the cursor
          # is below the retention floor. They get adopted on the next
          # start. The validator never creates checkpoints. Blocks
          # through an adopted checkpoint are unverified by this
          # validator, the same trust class as the catch-up path.
          "/opt/kardamom/checkpoints:/opt/kardamom/checkpoints",
        ]
        args = concat([
          # Seed parallel batch re-execution from the EIP-7928 BAL.
          # Falls back to sequential per-block execution when claims
          # are absent.
          "--parallel-validation",
          "--config", "/local/validator.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          # This allocation's cluster-egress (response) endpoint, for
          # the validator's own cluster client session: the node IP and
          # a Nomad dynamic port.
          "--cluster-egress-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_egress}",
          "--chain-id", "412346",
          "--chain", "/local/genesis.toml",
          # Use the validator's own state directory under the shared
          # persistent mount, never the executor's /opt/kardamom/state
          # root. This is a separate mdbx environment.
          "--state-dir", "/opt/kardamom/state/validator",
          # Join-miss archive refetch (tx_data and tx_deposits). When
          # the live multicast misses a canonical ref's envelope, it
          # replays in-band from the durability archives listed in
          # channels.toml. Replayed fragments and archive-control
          # responses land on the allocation's dynamic ports, so they
          # never clash with the batcher's on the aux node. tx_ordering
          # recovery rides the cluster replay.
          "--replay-destination-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_replay}",
          "--archive-control-response-endpoint", "${meta.node_ip}:${NOMAD_HOST_PORT_archive_response}",
          # Replay-unavailable fallback: fetch a peer checkpoint from
          # the executors' serve endpoints, and adopt it on restart,
          # the same as the executors' recovery-D loop.
          "--checkpoint-dir", "/opt/kardamom/checkpoints",
          "--checkpoint-peers", join(",", [for i in range(var.executor_count) : "executor-${i}.node.${var.datacenter}.consul:9014"]),
          # Shadow-check the node-incremental state trie against a
          # full rebuild every 8th block. A walker bug fail-stops the
          # validator (a dead alloc is a verdict failure), and bumps
          # kardamom_state_trie_shadow_mismatch_total. Checking every
          # block is exhaustive, but a full rebuild per 250ms block
          # would saturate a CI core continuously, and on the 4-core
          # DinD runner that would starve the Aeron cluster's
          # timing-sensitive recovery paths. The walker's per-block
          # equivalence has its own unit tests, with 50 randomized
          # blocks. A cadence of 8 still shadow-checks hundreds of
          # blocks per e2e run under real load.
          "--trie-shadow-check", "8",
          ],
          # Epoch verification against L1 (phase D). This appends
          # only when both variables are configured. The validator
          # requires --lockbox to parse as an address, so passing it
          # empty would break every deploy that has not opted in. When
          # unset, the validator still enforces the origin sequence;
          # only the content check is off.
          var.l1_rpc_url == "" || var.lockbox_address == "" ? [] : [
            "--l1-rpc-url", var.l1_rpc_url,
            "--lockbox", var.lockbox_address,
        ])
      }

      env {
        # Validator metrics run on port 9006. The executor holds port
        # 9004 on the same host.
        KARDAMOM_METRICS_ADDR = "0.0.0.0:9006"
      }

      # Cluster LogConfig (UDP multicast channels), read through
      # --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
        # The template reads the archive records from Consul. A change
        # there re-renders the file; the process reads it once at start
        # and follows the catalog through discovery, so never restart.
        change_mode = "noop"
      }

      # [cluster] ingress endpoints. Same contract as the executor's
      # config.
      template {
        destination = "local/validator.toml"
        data        = file("config/executor.toml")
      }

      # The chain genesis comes from one source: config/genesis/dev.toml.
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
