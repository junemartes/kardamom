# Aeron substrate — combined ArchivingMediaDriver (Media Driver + Archive in one
# JVM), run as a Nomad *system* job so it lands on EVERY client node (recorders
# AND workers): the media driver must be local to every service that shares the
# tmpfs aeron.dir.
#
# Image: 192.168.56.11:5000/kardamom-aeron:dev (built from
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

  group "aeron" {
    network {
      mode = "host"
    }

    # Persistent archive segment volume on the VM disk (recorders use it;
    # harmless on workers). Bound below into the container.
    task "archiving-media-driver" {
      driver = "docker"

      config {
        image        = "192.168.56.11:5000/kardamom-aeron:dev"
        network_mode = "host"
        # host tmpfs aeron.dir -> container /aeron-mount (matches image AERON_DIR
        # = /aeron-mount/dir), and persistent archive dir.
        volumes = [
          "/opt/kardamom/aeron-mount:/aeron-mount",
          "/opt/kardamom/archive:/aeron-mount/archive",
        ]
      }

      # Mirror the image defaults explicitly so the contract is visible here.
      env {
        AERON_DIR                   = "/aeron-mount/dir"
        AERON_ARCHIVE_MOUNT         = "/aeron-mount/archive"
        AERON_ARCHIVE_DIR           = "/aeron-mount/archive/dir"
        AERON_ARCHIVE_CLASS         = "io.aeron.archive.ArchivingMediaDriver"
        AERON_TERM_BUFFER_LENGTH    = "4194304"
        AERON_IPC_TERM_BUFFER_LENGTH = "4194304"
      }

      # Sized for the small test-tuned term buffers (4 MB); the per-node VM
      # memory budget in the Vagrantfile assumes these reservations.
      resources {
        cpu    = 500
        memory = 768
      }
    }
  }
}
