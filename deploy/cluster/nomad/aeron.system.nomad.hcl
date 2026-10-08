# This is the Aeron substrate: a combined ArchivingMediaDriver (Media
# Driver and Archive in one JVM). It runs as a Nomad system job, so it
# lands on every client node, both recorders and workers. The media
# driver must be local to every service that shares the tmpfs
# aeron.dir.
#
# Image: registry.service.consul:5000/kardamom-aeron:dev, built from
# crates/log/docker/aeron/Dockerfile. Its entrypoint starts
# io.aeron.archive.ArchivingMediaDriver, with AERON_DIR=/aeron-mount/dir
# and the archive under /aeron-mount/archive; see the image's ENV.
#
# Host bind: the on-node tmpfs paths.aeron_mount
# (/opt/kardamom/aeron-mount) mounts at the container's /aeron-mount.
# So the CnC file and ring buffers the image writes under
# /aeron-mount/dir stay visible to every co-located service container,
# which bind /opt/kardamom/aeron-mount to /opt/kardamom/aeron-mount and
# use --aeron-dir /opt/kardamom/aeron-mount/dir. In other words: host
# /opt/kardamom/aeron-mount/dir, container /aeron-mount/dir, and
# service container /opt/kardamom/aeron-mount/dir are the same
# inode-backed mmap.
#
# Archive segments persist to paths.archive_dir
# (/opt/kardamom/archive), bind-mounted so recordings survive
# container restarts.
#
# Assumption: the same image runs on workers too. Workers do not
# strictly need the Archive half, but running ArchivingMediaDriver
# everywhere keeps one image and a uniform aeron.dir layout; the
# workers' archive simply goes unused. To use a media-driver-only image
# on workers instead, split this into two system jobs with role
# constraints. Host networking exposes the archive control, response
# and replication UDP ports directly, on Nomad dynamic ports. Consumers
# read the control port from the kardamom-aeron-archive record.

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

# The Aeron stall tolerance, in milliseconds: how long an Aeron party
# waits through a stalled peer before it declares the peer dead. Aeron's
# default is 10000, and production keeps it: a longer value delays the
# detection of a dead process. CI raises it to ride out host stalls.
variable "aeron_stall_tolerance_ms" {
  type        = number
  description = "The client liveness timeout of the media driver and the driver timeout of its Java clients, in milliseconds. Aeron's default is 10000."
  default     = 10000

  validation {
    condition     = var.aeron_stall_tolerance_ms >= 1000 && floor(var.aeron_stall_tolerance_ms) == var.aeron_stall_tolerance_ms
    error_message = "The Aeron stall tolerance must be a whole number of milliseconds, at least 1000."
  }
}

variable "datacenter" {
  type        = string
  description = "The Nomad datacenter of the job. A node record is <node>.node.<datacenter>.consul."
  default     = "dc1"
}

# The Aeron timeouts of the Java media driver and its Java clients, all
# from the one stall tolerance:
# - aeron.driver.timeout (ms): the clients in this JVM wait this long
#   for a stalled driver.
# - aeron.client.liveness.timeout (ns): the driver waits this long for a
#   stalled client before it evicts the client. A client sends a
#   keepalive every 500 ms, far below it.
# - aeron.publication.unblock.timeout (ns): Aeron requires it above the
#   client liveness timeout. It keeps Aeron's default ratio, 3/2.
locals {
  aeron_stall_opts = join(" ", [
    "-Daeron.driver.timeout=${var.aeron_stall_tolerance_ms}",
    "-Daeron.client.liveness.timeout=${var.aeron_stall_tolerance_ms * 1000000}",
    "-Daeron.publication.unblock.timeout=${floor(var.aeron_stall_tolerance_ms * 3 / 2) * 1000000}",
  ])
  # A new driver refuses to start ("active driver detected") while the
  # CnC heartbeat of a dead predecessor is younger than the driver
  # timeout. So Nomad restarts the driver 5 s after that window. At
  # Aeron's 10 s default, this is Nomad's own 15 s default delay.
  driver_restart_delay = "${ceil(var.aeron_stall_tolerance_ms / 1000) + 5}s"
}

# File sync level of the archive recordings and of the archive catalog
# (-Daeron.archive.file.sync.level and
# -Daeron.archive.catalog.file.sync.level): 0 leaves a write in the page
# cache until the kernel flushes it, 1 syncs the data of every write
# batch, 2 syncs data and metadata. At 1 the recording position of a
# tx_data, tx_deposits or exec_txs recording is durable on disk. Level 1 costs
# throughput on the recording path. Ansible deployment passes -var from
# KARDAMOM_ARCHIVE_FILE_SYNC_LEVEL.
variable "archive_file_sync_level" {
  type        = string
  description = "The archive file sync level, 0, 1 or 2."
  default     = "1"

  validation {
    condition     = contains(["0", "1", "2"], var.archive_file_sync_level)
    error_message = "The archive_file_sync_level value must be 0, 1 or 2."
  }
}

job "aeron" {
  datacenters = [var.datacenter]
  type        = "system"

  # Keep the media driver off the control-plane node. cp1 runs only
  # the consul and nomad servers, the registry, and anvil; no
  # kardamom pipeline service shares its aeron.dir, so a driver there
  # would waste JVM memory. Every other node (recorders, sequencers,
  # workers) runs a service that needs a local driver.
  constraint {
    attribute = "${meta.tier}"
    operator  = "!="
    value     = "control"
  }

  # Keep the shared media driver off the sealer nodes too. In cluster
  # mode, they run only the Aeron Cluster (cluster.nomad.hcl), which
  # boots its own embedded ClusteredMediaDriver on a private aeron.dir
  # (.../aeron-mount/cluster-dir), and never touches the shared
  # substrate (.../aeron-mount/dir). Running an unused
  # ArchivingMediaDriver on all 3 sealer nodes would add 3 more
  # concurrent image pulls from the single in-cluster registry at
  # deploy time, the main bring-up bottleneck under CI contention.
  # Ansible mounts the on-node tmpfs independently, so the cluster's
  # cluster-dir is unaffected.
  constraint {
    attribute = "${meta.role}"
    operator  = "!="
    value     = "sealer"
  }

  group "aeron" {
    # Only the delay differs from Nomad's defaults; see the locals.
    restart {
      delay = local.driver_restart_delay
    }

    # A system job rolls node by node: every pipeline process on a node
    # shares its driver, so two drivers must never restart together. The
    # driver's control channel is UDP, so no port check applies; the
    # task state plus the stagger is the gate.
    update {
      max_parallel     = 1
      stagger          = "30s"
      health_check     = "task_states"
      min_healthy_time = "15s"
      healthy_deadline = "3m"
    }

    network {
      mode = "host"
      # The archive control endpoint, registered below. Consumers
      # resolve it from the kardamom-aeron-archive record, so Nomad
      # picks the port per allocation.
      port "archive_control" {}
      # The response and replication channels of the archive's own
      # client context. Nothing outside the task connects to them.
      port "archive_control_response" {}
      port "archive_replication" {}
    }

    # Persistent archive segment volume on the VM disk. Recorders use
    # it; it is harmless on workers. Bound below into the container.
    task "archiving-media-driver" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "registry.service.consul:5000/kardamom-aeron:dev"
        # This has no force_pull, matching the pre-digest behavior.
        # The aeron image changes rarely, and the digest pin makes
        # staleness moot on the pinned path. The :dev fallback keeps
        # the historical reuse-cache behavior.
        #
        # This skips readonly_rootfs on
        # purpose. The ArchivingMediaDriver is a JVM, and writes /tmp
        # (hsperfdata, JVM temp files) in the rootfs, besides its
        # bind-mounted aeron and archive directories. This needs a
        # validated tmpfs /tmp before flipping the setting; a wrong
        # guess would take down the media driver on every worker node
        # at once.
        network_mode = "host"
        # The media driver and every service container must see
        # aeron.dir at the same absolute path. Aeron records absolute
        # paths in its CnC metadata, for example
        # publications/<id>.logbuffer. So a client that mounts the
        # same host directory at a different path cannot map the
        # buffers ("Failed to open file:
        # /aeron-mount/dir/publications/24.logbuffer"). The services
        # bind /opt/kardamom/aeron-mount to /opt/kardamom/aeron-mount,
        # and use --aeron-dir /opt/kardamom/aeron-mount/dir. So the
        # driver must use that exact path too, not the image's
        # /aeron-mount default.
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/archive:/opt/kardamom/archive",
        ]
      }

      # Override the image's /aeron-mount defaults, so the path
      # matches the services. See the volumes note above.
      env {
        AERON_DIR           = "/opt/kardamom/aeron-mount/dir"
        AERON_ARCHIVE_MOUNT = "/opt/kardamom/archive"
        AERON_ARCHIVE_DIR   = "/opt/kardamom/archive/dir"
        AERON_ARCHIVE_CLASS = "io.aeron.archive.ArchivingMediaDriver"
        # The archive's UDP ports, as Nomad allocated them.
        AERON_ARCHIVE_CONTROL_PORT          = "${NOMAD_HOST_PORT_archive_control}"
        AERON_ARCHIVE_CONTROL_RESPONSE_PORT = "${NOMAD_HOST_PORT_archive_control_response}"
        AERON_ARCHIVE_REPLICATION_PORT      = "${NOMAD_HOST_PORT_archive_replication}"
        AERON_TERM_BUFFER_LENGTH            = "4194304"
        AERON_IPC_TERM_BUFFER_LENGTH        = "4194304"
        # Cap the ArchivingMediaDriver JVM heap, so the task fits its
        # trimmed memory reservation below. The driver's hot data (4
        # MB term buffers) sits off-heap in the tmpfs aeron.dir, so a
        # small heap is plenty. The JVM honors _JAVA_OPTIONS
        # regardless of the image entrypoint.
        # The Aeron MTU: 1344, below a 1400-byte network path
        # (1400 - 20 IP - 8 UDP = 1372, then down to a multiple of 32).
        # The Aeron default is 1408, which fragments or drops on that
        # path. A datagram of 1344 also fits every other path (a Docker
        # bridge, a cloud network, the loopback).
        #
        # The archive sync levels go in the same way: the image
        # entrypoint has no setting for them, and the Aeron default is 0.
        _JAVA_OPTIONS = "-Xmx160m -Daeron.mtu.length=1344 ${local.aeron_stall_opts} -Daeron.archive.file.sync.level=${var.archive_file_sync_level} -Daeron.archive.catalog.file.sync.level=${var.archive_file_sync_level}"
      }

      # The archive record of the discovery contract
      # (docs/aeron-discovery.md): the consumers' refetch client reads
      # the archive control endpoints from these records, filtered by
      # the topics each node's archive records. `archive_topics` is
      # node meta the Nomad agent template stamps from the node role set
      # (ansible/roles/nomad/templates/nomad.hcl.j2): the ingress nodes
      # record tx_data, the da-watcher node records tx_deposits, each
      # executor node records the exec_txs stream of its own executor, every
      # other node records nothing and lists no topic. Nomad owns this record;
      # the runtime never registers an archive. The record outlives every
      # publisher, so retained recordings stay discoverable.
      service {
        name     = "kardamom-aeron-archive"
        port     = "archive_control"
        address  = "${meta.node_ip}"
        provider = "consul"
        # The topic the node records, so a template can select the
        # archives of one topic: config/channels.toml.tpl renders its
        # fallback archive lists from `tx_data.kardamom-aeron-archive`
        # and `tx_deposits.kardamom-aeron-archive`. A node records at
        # most one topic.
        tags = ["${meta.archive_topics}"]
        meta {
          discovery_version = "1"
          cluster_id        = "${meta.cluster_id}"
          chain_id          = "412346"
          archive_id        = "${node.unique.name}"
          topics            = "${meta.archive_topics}"
        }
      }

      # 768 MB. One media driver runs on every non-control node, so the
      # per-driver footprint is the main cluster-wide memory cost, and
      # 384 MB held the 160 MB heap plus the driver's own buffers. But
      # the term buffers in the tmpfs aeron.dir are charged to the cgroup
      # of the driver that creates them: on a recorder node (the ingress
      # and aux nodes, whose archive records a topic) they reach 350 MB,
      # and the kernel OOM-kills a driver limited to 384 MB. The service
      # containers of the node then fail on "aeron thread did not signal
      # start". An executor node also records a topic: its one exec_txs
      # IPC publication adds 3 terms of 4 MiB, which this size holds.
      resources {
        cpu    = 400
        memory = 768
      }
    }
  }
}
