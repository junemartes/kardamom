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

    # NEVER give up restarting a Raft member (mode=delay retries past exhausted
    # attempts instead of leaving the task dead). Nomad's service defaults
    # (attempts=2/30m, mode=fail) silently strand members: a member restarted
    # into the survivors' election window (~10s leader-heartbeat timeout + the
    # election itself) SELF-TERMINATES via Aeron's termination hook — cleanly,
    # exit 0, nothing in the error log (reproduced locally: a kill -9'd leader
    # relaunched at +2s dies ~1s in; relaunched after the election it rejoins
    # fine every time). Each such death burned a default attempt; once
    # exhausted the member stayed down and the cluster wedged at 2/3 (or 1/3
    # after the quorum-loss case) — the chaos suite's dominant flake. The 15s
    # delay also spaces retries PAST the election window, so the second attempt
    # lands in the always-works rejoin path.
    restart {
      attempts = 5
      interval = "5m"
      delay    = "15s"
      mode     = "delay"
    }

    network {
      mode = "host"
    }

    task "cluster" {
      driver = "docker"

      # JVM options for the image ENTRYPOINT (java -Xmx384m -cp ... ClusterNode).
      # These MUST go through env, NOT docker `args`: docker `args` are appended
      # AFTER the main class, so they would become program arguments (args[]) rather
      # than -D system properties — ClusterNode reads System.getProperty(...), so the
      # members/nodeIp props would be null and EVERY member crash-loops on startup
      # ("kardamom.cluster.members not set"). JAVA_TOOL_OPTIONS is read by the JVM as
      # VM options (same mechanism as the aeron job's _JAVA_OPTIONS). ${meta.node_ip}
      # interpolates in env exactly as it would in args.
      env {
        JAVA_TOOL_OPTIONS = "-Dkardamom.cluster.nodeIp=${meta.node_ip} -Dkardamom.cluster.members=0,192.168.56.51:40200,192.168.56.51:40201,192.168.56.51:40202,192.168.56.51:40203,192.168.56.51:40204|1,192.168.56.52:40200,192.168.56.52:40201,192.168.56.52:40202,192.168.56.52:40203,192.168.56.52:40204|2,192.168.56.53:40200,192.168.56.53:40201,192.168.56.53:40202,192.168.56.53:40203,192.168.56.53:40204 -Daeron.dir=/opt/kardamom/aeron-mount/cluster-dir -Dkardamom.cluster.dir=/opt/kardamom/cluster -Dkardamom.archive.dir=/opt/kardamom/archive -Dkardamom.cluster.ingressStreamId=101 -Dkardamom.cluster.tickMs=2000"
      }

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
        # NOTE: the JVM -D system properties are passed via the JAVA_TOOL_OPTIONS env
        # stanza above, NOT here as docker `args` (args land after the main class and
        # would be parsed as program arguments, leaving System.getProperty null).
      }

      resources {
        cpu    = 1000
        memory = 1024
      }
    }
  }
}
