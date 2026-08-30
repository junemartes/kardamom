# shellcheck shell=bash
# =============================================================================
# ci-diagnostics.sh — failure diagnostics for ci-cluster.sh.
# =============================================================================
# This file is sourced into ci-cluster.sh's shell, never run as a child
# process. The dumpers read NODES and NODE_ROLE from lib-topology.sh's
# topology_load, and BRIDGE_NAME from the entry script. dump_diagnostics
# runs from the entry script's EXIT trap, on failure. This file must not
# install its own traps; ci-cluster.sh owns the single EXIT trap. It
# needs lib.sh (for log) and lib-topology.sh.

# On failure, dump every job's status and each allocation's stdout and
# stderr before teardown. Otherwise, the container removal in the entry
# script's cleanup erases the only evidence of why an alloc failed. The
# workflow's post-step runs after this script's EXIT trap, too late to
# help. This dump is best-effort; it must never mask the real exit code.
# This is a raw-UDP multicast reachability check, from sealer-0
# (192.168.56.51) to ingress-0 (192.168.56.31), on a throwaway group. It
# bypasses Aeron entirely. It prints how many of 30 sent packets
# ingress-0 received. 0 means the bridge drops cross-node multicast, a
# kernel or bridge issue. More than 0 means forwarding works, and any
# remaining failure is specific to Aeron. python3 ships in the node
# image.
multicast_probe() {
  local grp=239.192.99.99 port=45999 rip=192.168.56.31 sip=192.168.56.51
  local rnode=kardamom-ingress-0  # worker node on the segment (receiver)
  local snode=kardamom-sealer-0    # worker on the segment (sender)
  echo "===== multicast probe ${sip} -> ${rip} (grp ${grp}:${port}) ====="
  docker exec -d "${rnode}" python3 -c "
import socket,struct
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('',${port}))
mreq=struct.pack('4s4s',socket.inet_aton('${grp}'),socket.inet_aton('${rip}'))
s.setsockopt(socket.IPPROTO_IP,socket.IP_ADD_MEMBERSHIP,mreq)
s.settimeout(5)
n=0
try:
    while True:
        s.recvfrom(2048); n+=1
except socket.timeout:
    pass
open('/tmp/mcast_probe','w').write(str(n))
" >/dev/null 2>&1 || true
  sleep 1
  docker exec "${snode}" python3 -c "
import socket
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
s.setsockopt(socket.IPPROTO_IP,socket.IP_MULTICAST_IF,socket.inet_aton('${sip}'))
s.setsockopt(socket.IPPROTO_IP,socket.IP_MULTICAST_TTL,1)
for _ in range(30):
    s.sendto(b'probe',('${grp}',${port}))
print('probe: ${snode} sent 30 packets')
" 2>&1 || true
  sleep 5
  local got
  got="$(docker exec "${rnode}" cat /tmp/mcast_probe 2>/dev/null || echo '?')"
  echo "probe: ingress-0 received ${got}/30 packets (0 => bridge not forwarding multicast)"
}

dump_diagnostics() {
  log "FAILURE diagnostics — Nomad job + allocation logs"
  # Check host network state first. The cross-node Aeron data plane is
  # UDP multicast, and the usual failure is the bridge or netfilter
  # dropping it. These checks show whether bridged frames bypass
  # iptables, whether FORWARD is dropping packets (counters), and the
  # bridge's multicast flags and joined groups per node.
  echo "===== host: bridge-nf-call-iptables ====="
  cat /proc/sys/net/bridge/bridge-nf-call-iptables 2>/dev/null || echo "(br_netfilter not loaded)"
  echo "===== host: iptables FORWARD (counters) ====="
  sudo iptables -nvL FORWARD 2>/dev/null | head -25 || true
  echo "===== host: bridge ${BRIDGE_NAME:-kardamom-br0} ====="
  ip -d link show "${BRIDGE_NAME:-kardamom-br0}" 2>/dev/null || true
  cat "/sys/class/net/${BRIDGE_NAME:-kardamom-br0}/bridge/multicast_snooping" 2>/dev/null \
    | sed 's/^/  multicast_snooping=/' || true
  for n in "${NODES[@]}"; do
    [[ "${NODE_ROLE[$n]}" == "control" ]] && continue   # no media driver on cp
    echo "----- ${n}: joined multicast groups (ip maddr, 239.x only) -----"
    docker exec "kardamom-${n}" ip maddr show dev eth0 2>/dev/null \
      | awk '/inet 239\./{print "  "$2}' | sort || true
  done
  # Run a direct raw-UDP multicast probe from sealer-1 to r1, on a
  # throwaway group, independent of Aeron. If r1 receives 0 packets, the
  # bridge is not forwarding multicast at all, a kernel or bridge
  # problem. If it receives packets but Aeron still does not record,
  # the problem is specific to Aeron. This probe uses a distinct group
  # and port, so it cannot collide with the media driver's sockets. It
  # is fully best-effort.
  multicast_probe || true
  # Observe the actual Aeron multicast frames on the bridge. Do the
  # sealer's SETUP and DATA frames reach the tx_ordering DATA group
  # (239.192.56.13)? Do subscribers' Status Messages reach the derived
  # CONTROL group (.12)? Capture all cluster multicast for a few
  # seconds, and summarize it by source and destination, to see which
  # streams flow, and in which direction.
  echo "===== tcpdump ${BRIDGE_NAME:-kardamom-br0}: cluster multicast src->dst (≤8s) ====="
  # Show source and destination, to see both DATA frames (destination
  # .13/.21/.25, the odd data groups) and Status Messages (destination
  # .12/.20/.24, the even control groups). If publications never
  # connect, data goes out but no Status Messages come back to the
  # control groups.
  sudo timeout 8 tcpdump -i "${BRIDGE_NAME:-kardamom-br0}" -nn -t -c 250 \
    'udp and dst net 239.192.56.0/24' 2>/dev/null \
    | awk '{d=$4; sub(/:$/,"",d); print $2" -> "d}' | sort | uniq -c | sort -rn | head -40 \
    || echo "(tcpdump unavailable or no multicast captured)"
  export NOMAD_ADDR="http://192.168.56.10:4646"
  nomad job status 2>/dev/null || true
  # `validator` is included in this list. Its job going dead, when the
  # restart budget is exhausted after repeated fail-stops, is exactly
  # the failure this dump needs to explain. `nomad job allocs` and
  # `nomad alloc logs` still serve dead-but-not-garbage-collected
  # allocations. This job was missing from the list once, and a
  # validator failure left no evidence.
  for job in aeron anvil cluster sealer sequencer executor validator ingress batcher da-watcher; do
    local allocs
    allocs="$(nomad job allocs -t '{{range .}}{{.ID}}{{"\n"}}{{end}}' "${job}" 2>/dev/null || true)"
    [[ -z "${allocs}" ]] && continue
    while read -r alloc; do
      [[ -z "${alloc}" ]] && continue
      echo "----- ${job} alloc ${alloc}: status -----"
      nomad alloc status "${alloc}" 2>/dev/null | sed -n '1,40p' || true
      # stderr carries the tracing logs. Show the head (startup: channel
      # setup, "recording ready", "aggregating quorum watermark" — the
      # markers that show whether multicast carried data and quorum
      # advanced) and the tail (most recent state). The interesting
      # events happen at startup, but failures show up at the end.
      local err
      err="$(nomad alloc logs -stderr "${alloc}" 2>/dev/null || true)"
      echo "----- ${job} alloc ${alloc}: stderr (head 30) -----"
      printf '%s\n' "${err}" | head -30 || true
      echo "----- ${job} alloc ${alloc}: stderr (tail 40) -----"
      printf '%s\n' "${err}" | tail -40 || true
      # The cluster job needs its full role and election history, so it
      # gets a longer tail.
      local stdout_tail=40
      [[ "${job}" == "cluster" ]] && stdout_tail=200
      echo "----- ${job} alloc ${alloc}: stdout (tail ${stdout_tail}) -----"
      nomad alloc logs "${alloc}" 2>/dev/null | tail -"${stdout_tail}" || true
    done <<<"${allocs}"
  done

  # Aeron Cluster members log fatal errors to *-error.log files, in the
  # cluster and aeron directories. They do not log to the alloc stderr:
  # the JVM exits 0 through the termination hook after a
  # ConsensusModule or ServiceContainer error. Dump these logs from each
  # sealer node. The directories are bind-mounted from the node into the
  # inner task.
  for n in "${NODES[@]}"; do
    [[ "${NODE_ROLE[$n]}" == "sealer" ]] || continue
    echo "===== ${n}: Aeron cluster error logs ====="
    docker exec "kardamom-${n}" sh -c '
      for f in /opt/kardamom/cluster/*error*.log /opt/kardamom/aeron-mount/cluster-dir/*error*.log; do
        [ -f "$f" ] && { echo "--- $f ---"; cat "$f"; }
      done' 2>/dev/null || true
  done

  # Get the consensus-module state per member: election phase, log,
  # commit, and append positions, and the recovery plan. This is the
  # ground truth for "leader elected but nothing commits" stalls.
  # ClusterTool ships in the cluster-node jar. Run it inside each sealer
  # node's inner cluster task container. This is a Nomad docker task,
  # named by its alloc; find it with the inner docker ps.
  for n in "${NODES[@]}"; do
    [[ "${NODE_ROLE[$n]}" == "sealer" ]] || continue
    echo "===== ${n}: ClusterTool errors + members ====="
    docker exec "kardamom-${n}" sh -c '
      inner="$(docker ps --format "{{.Names}}" | grep -m1 "^cluster-")"
      [ -n "$inner" ] || { echo "(no inner cluster container running)"; exit 0; }
      # `errors` reads the CONSENSUS_MODULE and CONTAINER mark-file
      # error buffers. The CONTAINER buffer is where a dying clustered
      # service logs, for example aeron tmpfs exhaustion. The *.log
      # file glob above does not cover it.
      docker exec "$inner" java -cp /opt/kardamom/cluster-node.jar io.aeron.cluster.ClusterTool /opt/kardamom/cluster errors 2>&1 | tail -40
      echo "--- list-members ---"
      docker exec "$inner" java -cp /opt/kardamom/cluster-node.jar io.aeron.cluster.ClusterTool /opt/kardamom/cluster list-members 2>&1 | tail -3' 2>/dev/null || true
  done
}
