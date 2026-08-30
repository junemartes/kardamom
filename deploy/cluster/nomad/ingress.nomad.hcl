# kardamom-ingress is the eth JSON-RPC proxy. It runs active/active:
# count=2, one per ingress-role node (ingress-0@192.168.56.31,
# ingress-1@192.168.56.32).
#
# Invocation:
#   kardamom-ingress --config <ingress.toml> --log-config <channels.toml> \
#       --aeron-dir <dir> --shards 2 --jsonrpc-bind 0.0.0.0:8545 \
#       --ack-policy on-quorum
#
# ingress.toml supplies the [cluster] Aeron Cluster (Raft) client
# connection, for the on-quorum watermark observer. Other runtime
# tuning goes through flags. channels.toml supplies the UDP multicast
# channels. The on-quorum ack gate's durable watermark is no longer an
# Aeron quorum_watermark stream. In the cluster-only topology, ingress
# derives it from Aeron Cluster egress progress; see
# crates/ingress/src/cluster.rs.
#
# tx_receipts MDS fan-in: channels.toml's tx_receipts_control_channel
# and tx_receipts_executor_count drive ingress to open one
# control-mode=manual subscription, and attach each executor replica's
# per-replica endpoint (0 through N). It dedups the N identical receipt
# copies by tx hash. executor_count comes from the log config; override
# it at runtime with --executor-count or KARDAMOM_EXECUTOR_COUNT.
# TODO(consul-watch): swap the static count for a Consul watch on an
# `executor-receipts` service, so membership changes add or remove
# destinations live.
#
# This shares the node's Aeron media driver, through the bind-mounted
# tmpfs aeron.dir. It uses host networking, so :8545 binds on the
# ingress1 VM IP.
#
# This job uses file() for its templates, so submit it from the
# deploy/cluster/ directory. scripts/deploy.sh does this.

variable "ack_policy" {
  type        = string
  description = "Ingress ack durability gate. Defaults to on-offer: a tx is acked once it is sequenced (offered), with no durable-watermark gate on the critical path. The strong durability boundary is L1 DA (via the batcher). on-quorum is now available: the Aeron Cluster (Raft) is the orderer, and ingress derives the durable watermark from cluster egress progress (a record/boundary on egress is a Raft-quorum-durability signal). Override with: nomad job run -var ack_policy=on-quorum ingress.nomad.hcl (needs the [cluster] section in ingress.toml + --cluster-egress-endpoint, both wired in this job)."
  default     = "on-offer"
}

# Digest-pinned image. scripts/deploy.sh
# passes the repo:tag@sha256:... reference captured at push time
# (deploy/cluster/images.digests), so the task runs exactly the bytes
# that deploy pushed. The empty default falls back to the mutable :dev
# tag in the task config. That fallback is a dev affordance for manual
# `nomad job run` during debugging, not a production path. A mutable
# tag lets anyone with registry push access change what the next
# restart runs.
variable "image_ref" {
  type        = string
  description = "Digest-pinned image reference (repo:tag@sha256:...) from the deploy's push manifest. Empty = mutable :dev tag fallback (dev-only)."
  default     = ""
}

job "ingress" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "ingress"
  }

  group "ingress" {
    # Active/active: 2 replicas, one per ingress-role node (.31,
    # .32). Both join the tx_receipts multicast group, and route to
    # all sequencer shards. The --ingress-id below namespaces each
    # replica's correlation_ids.
    count = 2

    constraint {
      distinct_hosts = true
    }

    # Resilience (chaos tests): restart a crashed task on the same
    # node, and reschedule on node loss. With count=2 and
    # distinct_hosts (active/active), a lost ingress node cannot
    # reschedule onto the other ingress node. It does not need to: the
    # surviving replica serves all traffic, since clients fail over,
    # and this alloc recovers when its node returns.
    restart {
      attempts = 3
      interval = "1m"
      delay    = "5s"
      mode     = "delay"
    }

    reschedule {
      delay          = "10s"
      delay_function = "exponential"
      max_delay      = "1m"
      unlimited      = true
    }

    update {
      max_parallel     = 1
      health_check     = "checks"
      min_healthy_time = "10s"
      healthy_deadline = "2m"
      auto_revert      = false
    }

    network {
      mode = "host"
      port "jsonrpc" {
        static = 8545
      }
    }

    task "ingress" {
      driver = "docker"

      config {
        # This budget covers 8192 rpc connections, the WS feed,
        # aeron, and docker overhead. The default container nofile
        # (1024 soft on some daemons) hit EMFILE under a 2,500 tps
        # blocking-submit burst, which holds one connection per
        # in-flight tx. That collapsed acceptance and starved receipt
        # polls.
        ulimit {
          nofile = "65536:65536"
        }
        image = var.image_ref != "" ? var.image_ref : "192.168.56.10:5000/kardamom-ingress:dev"
        # force_pull stays on for both paths. The mutable :dev
        # fallback needs it; a stale node-cached layer once caused a
        # crash-retry storm that stalled the deploy. On the
        # digest-pinned path, Nomad 1.9.5 pulls the ref's advisory tag,
        # then resolves the container image by digest. The pull
        # refreshes the tag, but the digest still pins what runs: a
        # moved tag fails the task, instead of ever running unpinned
        # bytes.
        force_pull = true
        # Read-only root filesystem. The
        # ingress writes only to the bind-mounted aeron directory and
        # Nomad's alloc, local, and secrets mounts, which stay
        # writable. So its docker diff should be empty. cluster-e2e
        # validates this.
        readonly_rootfs = true
        network_mode    = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
        ]
        args = [
          "--config", "/local/ingress.toml",
          "--log-config", "/local/channels.toml",
          "--aeron-dir", "/opt/kardamom/aeron-mount/dir",
          "--shards", "2",
          "--jsonrpc-bind", "0.0.0.0:8545",
          # Use a stable per-replica id (alloc index 0 or 1). This
          # namespaces correlation_id, so the two active/active
          # replicas never collide. See
          # docs/agents/resilient-ingress-spec.md D3.
          "--ingress-id", "${NOMAD_ALLOC_INDEX}",
          "--ack-policy", "${var.ack_policy}",
          # The interim 100-connection failover shed is retired. That
          # earlier bug is now fixed twice over: the sequencer
          # republishes unconfirmed offers until a receipt proves
          # commitment, and the sealer's per-sender contiguity guard
          # rejects any ref that would seal a nonce gap. A voided-offer
          # window now recovers instead of halting the executors, so
          # blocking submits no longer need throttling to protect the
          # pipeline.
          "--rpc-max-connections", "8192",
          # eth_chainId must report the real L2 chain id (group_vars
          # chain_id). Without this, the ingress serves the compiled-in
          # default (1), and every client would need to hardcode the
          # chain.
          "--chain-id", "412346",
          # Cluster mode: this node's cluster-egress (response)
          # endpoint, for the on-quorum watermark observer's Aeron
          # Cluster client. The port, 40210 (cluster_egress_port),
          # stays uniform; uniqueness comes from the ingress node_ip.
          # This is consulted only when --ack-policy gates on quorum.
          "--cluster-egress-endpoint", "${meta.node_ip}:40210",
          # Record each per-shard tx_data publication to the archive,
          # so a restarted executor can replay full transaction
          # envelopes (Phase 2 crash recovery).
          "--archive-durability",
        ]
      }

      # Presence-checked config. Content lives in config/ingress.toml.
      template {
        destination = "local/ingress.toml"
        data        = file("config/ingress.toml")
      }

      # Cluster LogConfig (UDP multicast channels). Comes from one
      # source, config/channels.toml.tpl, and is read through
      # --log-config.
      template {
        destination = "local/channels.toml"
        data        = file("config/channels.toml.tpl")
      }

      resources {
        cpu    = 500
        memory = 512
      }

      service {
        name     = "ingress-jsonrpc"
        port     = "jsonrpc"
        provider = "consul"

        # The JSON-RPC server only answers POSTs, so a TCP connect
        # check is the right liveness signal here.
        check {
          type     = "tcp"
          interval = "10s"
          timeout  = "2s"
        }
      }
    }
  }
}
