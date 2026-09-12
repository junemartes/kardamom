# kardamom-cluster is the 3-member Aeron Cluster (Raft) sealer. Each
# alloc is one Raft member. Members run on the sealer node class
# (sealer-0, sealer-1, sealer-2; constraint ${meta.role} == sealer), with
# distinct_hosts so one member lands on each sealer node. memberId
# comes from the node's own IP (${meta.node_ip}), not
# ${NOMAD_ALLOC_INDEX}. distinct_hosts spreads allocs across the 3
# nodes, but the alloc index is not guaranteed to match node-IP order.
# A static index-to-IP mapping could advertise the wrong endpoints and
# fail to form quorum.
#
# This replaces the single sealer (sealer.nomad.hcl) in cluster mode.
# Ordering and durability now fold into the Raft log and the
# per-member Aeron Archive, with no separate tx_ordering MDC publisher
# and no archive-at-the-sealer sidecar. The sequencer cluster-clients
# connect to the cluster ingress (port 40200). The cluster totally
# orders, dedups, and commits through Raft, and replays the committed
# stream out the client egress, which the executor consumes.
#

# Egress replay retention, in frames (-Dkardamom.cluster.retention).
# The default matches the sealer's own DEFAULT_RETENTION (65536, about
# 321s at 200 tps). The retention-overrun chaos case deploys a small
# window; Ansible deployment passes -var from KARDAMOM_CLUSTER_RETENTION. This
# lets a frozen consumer's cursor age out, and exercises recovery-D,
# inside one chaos case.
variable "cluster_retention" {
  type    = string
  default = "65536"
}

# Automatic Raft snapshot interval, in seconds
# (-Dkardamom.cluster.snapshotIntervalS; 0 disables). Every member runs
# the scheduler, but only the current leader's toggle fires, and the
# snapshot action replicates through the log, so all members snapshot
# at the same position. The chaos-cluster shard shortens this; Ansible deployment
# passes -var from KARDAMOM_CLUSTER_SNAPSHOT_S. This lets
# cluster-member-rejoin wait for a snapshot inside one case.
variable "cluster_snapshot_interval_s" {
  type    = string
  default = "300"
}

# Remote-origin allowlist (-Dkardamom.cluster.remoteOrigins): the peer
# chain ids whose cross-chain records (kind 5) this sealer seals. An
# empty list disables interop, and the sealer rejects every kind-5
# record. Every member must run the same list: it decides
# accept-or-reject in the replicated state machine, like the dedup
# window. The default names the dev-interop peers the e2e suite uses
# against the deployed 412346 chain: chain B (412347, S14) and the
# simulated origin (412399, S12/S13). Ansible deployment passes -var from
# KARDAMOM_REMOTE_ORIGINS when set.
variable "cluster_remote_origins" {
  type    = string
  default = "412347,412399"
}

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

# This is a pure JVM image. cluster.Dockerfile launches
# io.kardamom.sealer.cluster.ClusterNode.

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

variable "sealer_count" {
  type        = number
  description = "The sealer node count (node_classes.sealer.count and cluster_member_count). Member i runs on sealer-<i>."
  default     = 3
}

# One Raft member per sealer node, addressed by its Consul node record.
# The member id is the node index (meta.node_index), so a member never
# has to find itself by address. The port list mirrors cluster_ports in
# group_vars/all.yml: ingress, consensus, log, catchup, archive_control.
locals {
  member_ports = [40200, 40201, 40202, 40203, 40204]
  members = join("|", [
    for i in range(var.sealer_count) :
    "${i},${join(",", [for p in local.member_ports : "sealer-${i}.node.${var.datacenter}.consul:${p}"])}"
  ])
}

job "cluster" {
  datacenters = [var.datacenter]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "sealer"
  }

  group "cluster" {
    # Run one Raft member per sealer node, a 3-member quorum.
    # distinct_hosts spreads the members across the 3 sealer nodes.
    # memberId comes from the node IP, since the alloc index is not
    # guaranteed to match node-IP order.
    count = 3
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

    # Never give up restarting a Raft member. mode=delay retries past
    # exhausted attempts, instead of leaving the task dead. Nomad's
    # service defaults (attempts=2/30m, mode=fail) can silently strand
    # a member. A member restarted into the survivors' election window
    # (about a 10s leader-heartbeat timeout, plus the election itself)
    # self-terminates through Aeron's termination hook: cleanly, exit
    # 0, nothing in the error log. Reproduced locally: a kill -9'd
    # leader relaunched at +2s dies about 1s in, but relaunched after
    # the election it rejoins fine every time. Each such death burned a
    # default attempt. Once exhausted, the member stayed down, and the
    # cluster wedged at 2/3 (or 1/3 after the quorum-loss case). This
    # was the chaos suite's most common flake. The 15s delay also
    # spaces retries past the election window, so the second attempt
    # lands in the always-works rejoin path.
    restart {
      attempts = 5
      interval = "5m"
      delay    = "15s"
      mode     = "delay"
    }

    network {
      mode = "host"
      # The client-facing ingress endpoint, registered below.
      port "ingress" {
        static = 40200
      }
    }

    task "cluster" {
      driver = "docker"

      # The cluster member record of the discovery contract
      # (docs/aeron-discovery.md): every cluster client resolves the
      # member ingress endpoints from these records at startup. The
      # member id equals the node index of the sealer class, which is
      # the order ClusterNode derives its member id from the node IP.
      # Nomad owns this record. Discovering a member never changes the
      # voting set: the membership stays the static list in
      # JAVA_TOOL_OPTIONS below.
      service {
        name     = "kardamom-cluster-member"
        port     = "ingress"
        address  = "${meta.node_ip}"
        provider = "consul"
        meta {
          discovery_version = "1"
          cluster_id        = "${meta.cluster_id}"
          chain_id          = "412346"
          member_id         = "${meta.node_index}"
        }
      }

      # These are JVM options for the image ENTRYPOINT
      # (java -Xmx384m -cp ... ClusterNode). They must go through env,
      # not docker `args`. docker `args` land after the main class, so
      # they would become program arguments instead of -D system
      # properties. ClusterNode reads System.getProperty(...), so the
      # members and nodeIp properties would be null, and every member
      # would crash-loop on startup ("kardamom.cluster.members not
      # set"). JAVA_TOOL_OPTIONS is read by the JVM as VM options, the
      # same mechanism as the aeron job's _JAVA_OPTIONS. ${meta.node_ip}
      # interpolates in env exactly as it would in args.
      env {
        JAVA_TOOL_OPTIONS = "-Dkardamom.cluster.nodeIp=${meta.node_ip} -Dkardamom.cluster.memberId=${meta.node_index} -Dkardamom.cluster.members=${local.members} -Daeron.dir=/opt/kardamom/aeron-mount/cluster-dir -Dkardamom.cluster.dir=/opt/kardamom/cluster -Dkardamom.archive.dir=/opt/kardamom/archive -Dkardamom.cluster.ingressStreamId=101 -Dkardamom.cluster.tickMs=2000 -Dkardamom.cluster.retention=${var.cluster_retention} -Dkardamom.cluster.snapshotIntervalS=${var.cluster_snapshot_interval_s} -Dkardamom.cluster.remoteOrigins=${var.cluster_remote_origins}"
      }

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-cluster:dev"
        # force_pull stays on for both paths; see the ingress job's
        # comment. The :dev fallback needs it. On the pinned path, the
        # 1.9.5 driver pulls the tag but resolves the image by digest,
        # so the pin holds.
        force_pull = true
        # This skips readonly_rootfs on
        # purpose. This is a JVM task, and the JVM writes into the
        # rootfs outside the bind mounts, at least /tmp (hsperfdata,
        # JVM temp files). Turning this on needs a tmpfs mount for
        # /tmp, validated by a full cluster-e2e pass first. A wrong
        # guess here would wedge the Raft sealer, which is the whole
        # pipeline.
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/cluster:/opt/kardamom/cluster",
          "/opt/kardamom/archive:/opt/kardamom/archive",
        ]
        # The JVM -D system properties pass through the
        # JAVA_TOOL_OPTIONS env stanza above, not here as docker
        # `args`. args land after the main class, and would be parsed
        # as program arguments, leaving System.getProperty null.
      }

      resources {
        cpu    = 1000
        memory = 1024
      }
    }
  }
}
