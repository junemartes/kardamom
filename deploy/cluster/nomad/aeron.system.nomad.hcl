# This is the Aeron substrate: a combined ArchivingMediaDriver (Media
# Driver and Archive in one JVM). It runs as a Nomad system job, so it
# lands on every client node, both recorders and workers. The media
# driver must be local to every service that shares the tmpfs
# aeron.dir.
#
# Image: 192.168.56.10:5000/kardamom-aeron:dev, built from
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
# constraints. Host networking exposes the archive
# control/response/recording-events/replication UDP ports
# (8010/8011/8020/8021) directly.

# Digest-pinned image. scripts/deploy.sh
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

job "aeron" {
  datacenters = ["dc1"]
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
    network {
      mode = "host"
    }

    # Persistent archive segment volume on the VM disk. Recorders use
    # it; it is harmless on workers. Bound below into the container.
    task "archiving-media-driver" {
      driver = "docker"

      config {
        image = var.image_ref != "" ? var.image_ref : "192.168.56.10:5000/kardamom-aeron:dev"
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
        AERON_DIR                    = "/opt/kardamom/aeron-mount/dir"
        AERON_ARCHIVE_MOUNT          = "/opt/kardamom/archive"
        AERON_ARCHIVE_DIR            = "/opt/kardamom/archive/dir"
        AERON_ARCHIVE_CLASS          = "io.aeron.archive.ArchivingMediaDriver"
        AERON_TERM_BUFFER_LENGTH     = "4194304"
        AERON_IPC_TERM_BUFFER_LENGTH = "4194304"
        # Cap the ArchivingMediaDriver JVM heap, so the task fits its
        # trimmed memory reservation below. The driver's hot data (4
        # MB term buffers) sits off-heap in the tmpfs aeron.dir, so a
        # small heap is plenty. The JVM honors _JAVA_OPTIONS
        # regardless of the image entrypoint.
        _JAVA_OPTIONS = "-Xmx160m"
      }

      # Trimmed from 768 MB. One media driver runs on every
      # non-control node (the sequencer and worker tiers), so the
      # per-driver footprint is the main cluster-wide memory cost. 384
      # MB holds the 160 MB heap plus the driver's off-heap buffers,
      # metaspace, and threads.
      resources {
        cpu    = 400
        memory = 384
      }
    }
  }
}
