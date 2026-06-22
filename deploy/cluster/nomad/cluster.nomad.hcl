# kardamom-cluster — the 3-member Aeron Cluster (Raft) sealer. Each alloc is one
# Raft member; members run on the sealer node-class (192.168.56.51/.52/.53,
# constraint ${meta.role} == sealer) with distinct_hosts so one member lands on
# each sealer node. memberId is derived from the node's own IP (${meta.node_ip}),
# NOT ${NOMAD_ALLOC_INDEX}: distinct_hosts spreads allocs across the 3 nodes but
# the alloc index is NOT guaranteed to match node-IP order, so a static
# index→IP mapping could advertise the wrong endpoints and fail to form quorum.
#
# This REPLACES the single sealer (sealer.nomad.hcl) in cluster mode: ordering
# AND durability now fold into the Raft log + the per-member Aeron Archive (no
# separate tx_ordering MDC publisher and no archive-at-the-sealer sidecar). The
# sequencer cluster-clients connect to the cluster ingress (port 40200); the
# cluster totally orders + dedups + commits via Raft and replays the committed
# stream out the client egress, which the executor consumes.
#
# Pure JVM image (cluster.Dockerfile launches io.kardamom.sealer.cluster.ClusterNode).

job "cluster" {
  datacenters = ["dc1"]
  type        = "service"

  constraint {
    attribute = "${meta.role}"
    value     = "sealer"
  }

  group "cluster" {
    # One Raft member per sealer node (3-member quorum). distinct_hosts spreads
    # the members across the 3 sealer nodes; memberId is derived from the node IP
    # (the alloc index is not guaranteed to match node-IP order).
    count = 3
    constraint {
      operator = "distinct_hosts"
      value    = "true"
    }

    network {
      mode = "host"
    }

    task "cluster" {
      driver = "docker"

      config {
        image = "192.168.56.10:5000/kardamom-cluster:dev"
        # Always pull the freshly-built image: the mutable :dev tag would otherwise
        # let Nomad reuse a stale node-cached layer across rebuilds (caused a
        # crash-retry storm that stalled the deploy).
        force_pull   = true
        network_mode = "host"
        volumes = [
          "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount",
          "/opt/kardamom/cluster:/opt/kardamom/cluster",
          "/opt/kardamom/archive:/opt/kardamom/archive",
        ]
        # These are -D sysprops appended to the image ENTRYPOINT java command.
        args = [
          # memberId is resolved by matching this node's IP against the ingress host
          # of each member below (alloc index != node, so ${NOMAD_ALLOC_INDEX} can't
          # be used). ${meta.node_ip} is the proven per-node interpolation (same as
          # the sequencer's --tx-ordering-mdc-control ${meta.node_ip}:40110).
          "-Dkardamom.cluster.nodeIp=${meta.node_ip}",
          "-Dkardamom.cluster.members=0,192.168.56.51:40200,192.168.56.51:40201,192.168.56.51:40202,192.168.56.51:40203,192.168.56.51:40204|1,192.168.56.52:40200,192.168.56.52:40201,192.168.56.52:40202,192.168.56.52:40203,192.168.56.52:40204|2,192.168.56.53:40200,192.168.56.53:40201,192.168.56.53:40202,192.168.56.53:40203,192.168.56.53:40204",
          # aeron.dir is a DISTINCT subdir (.../cluster-dir) from the node's shared
          # Aeron substrate (.../dir): the cluster runs its OWN embedded
          # ClusteredMediaDriver, which must not clash with the node substrate's
          # media driver sharing /opt/kardamom/aeron-mount/dir.
          "-Daeron.dir=/opt/kardamom/aeron-mount/cluster-dir",
          "-Dkardamom.cluster.dir=/opt/kardamom/cluster",
          "-Dkardamom.archive.dir=/opt/kardamom/archive",
          "-Dkardamom.cluster.ingressStreamId=101",
          "-Dkardamom.cluster.tickMs=2000",
        ]
      }

      resources {
        cpu    = 1000
        memory = 1024
      }
    }
  }
}
