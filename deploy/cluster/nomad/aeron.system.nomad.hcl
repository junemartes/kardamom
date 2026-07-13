# Aeron substrate — combined ArchivingMediaDriver (Media Driver + Archive in one
# JVM), run as a Nomad *system* job so it lands on EVERY client node (recorders
# AND workers): the media driver must be local to every service that shares the
# tmpfs aeron.dir.
#
# Image: 192.168.56.10:5000/kardamom-aeron:dev (built from
# crates/log/docker/aeron/Dockerfile). Its entrypoint starts
# io.aeron.archive.ArchivingMediaDriver with AERON_DIR=/aeron-mount/dir and the
# archive under /aeron-mount/archive (see the image's ENV).
#
# Host bind: the on-node tmpfs paths.aeron_mount (/opt/kardamom/aeron-mount) is
# mounted at the container's /aeron-mount, so the CnC file + ring buffers the
# image writes under /aeron-mount/dir are visible to every co-located service
# container (which bind /opt/kardamom/aeron-mount -> /opt/kardamom/aeron-mount
# and use --aeron-dir /opt/kardamom/aeron-mount/dir). i.e. host
# /opt/kardamom/aeron-mount/dir == container /aeron-mount/dir == service
# container /opt/kardamom/aeron-mount/dir (same inode-backed mmap).
#
# Archive segments persist to paths.archive_dir (/opt/kardamom/archive),
# bind-mounted so recordings survive container restarts.
#
# ASSUMPTION: the same image runs on workers too (workers don't strictly need
# the Archive half, but running ArchivingMediaDriver everywhere keeps one image
# and a uniform aeron.dir layout; the workers' archive is simply unused). If a
# media-driver-only image is preferred on workers, split into two system jobs
# with role constraints. Host networking exposes the archive control/response/
# recording-events/replication UDP ports (8010/8011/8020/8021) directly.

job "aeron" {
  datacenters = ["dc1"]
  type        = "system"

  # Keep the media driver OFF the control-plane node: cp1 runs only the
  # consul/nomad servers + registry + anvil (no kardamom pipeline service shares
  # its aeron.dir), so a driver there is wasted JVM memory. Every other node
  # (recorders, sequencers, workers) runs a service that needs a local driver.
  constraint {
    attribute = "${meta.tier}"
    operator  = "!="
    value     = "control"
  }

  # Keep the shared media driver OFF the sealer nodes too: in cluster mode they run
  # ONLY the Aeron Cluster (cluster.nomad.hcl), which boots its OWN embedded
  # ClusteredMediaDriver on a private aeron.dir (.../aeron-mount/cluster-dir) and
  # never touches the shared substrate (.../aeron-mount/dir). Running an unused
  # ArchivingMediaDriver on all 3 sealer nodes just added 3 more concurrent image
  # pulls from the single in-cluster registry at deploy time (the dominant bring-up
  # bottleneck under CI contention). The on-node tmpfs is mounted by Ansible
  # independently, so the cluster's cluster-dir is unaffected.
  constraint {
    attribute = "${meta.role}"
    operator  = "!="
    value     = "sealer"
  }

  group "aeron" {
    network {
      mode = "host"
    }

    # Persistent archive segment volume on the VM disk (recorders use it;
    # harmless on workers). Bound below into the container.
    task "archiving-media-driver" {
      driver = "docker"

      config {
        image        = "192.168.56.10:5000/kardamom-aeron:dev"
        network_mode = "host"
        # CRITICAL: the media driver and every service container must see
        # aeron.dir at the SAME ABSOLUTE PATH. Aeron records absolute paths in
        # its CnC metadata (e.g. publications/<id>.logbuffer), so a client that
        # mounts the same host dir at a DIFFERENT path can't map the buffers
        # ("Failed to open file: /aeron-mount/dir/publications/24.logbuffer").
        # The services bind /opt/kardamom/aeron-mount -> /opt/kardamom/aeron-mount
        # and use --aeron-dir /opt/kardamom/aeron-mount/dir, so the driver must
        # use that exact path too (NOT the image's /aeron-mount default).
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/archive:/opt/kardamom/archive",
        ]
      }

      # Override the image's /aeron-mount defaults so the path matches the
      # services (see the volumes note above).
      env {
        AERON_DIR                   = "/opt/kardamom/aeron-mount/dir"
        AERON_ARCHIVE_MOUNT         = "/opt/kardamom/archive"
        AERON_ARCHIVE_DIR           = "/opt/kardamom/archive/dir"
        AERON_ARCHIVE_CLASS         = "io.aeron.archive.ArchivingMediaDriver"
        AERON_TERM_BUFFER_LENGTH    = "4194304"
        AERON_IPC_TERM_BUFFER_LENGTH = "4194304"
        # Cap the ArchivingMediaDriver JVM heap so the task fits its trimmed
        # memory reservation below. The driver's hot data (4 MB term buffers) is
        # off-heap in the tmpfs aeron.dir, so a small heap is plenty; _JAVA_OPTIONS
        # is honoured by the JVM regardless of the image entrypoint.
        _JAVA_OPTIONS = "-Xmx160m"
      }

      # Trimmed from 768 MB: one media driver runs on every non-control node
      # (the sequencer + worker tiers), so the per-driver footprint is the
      # dominant cluster-wide memory cost. 384 MB holds the 160 MB heap + the
      # driver's off-heap buffers/metaspace/threads.
      resources {
        cpu    = 400
        memory = 384
      }
    }
  }
}
