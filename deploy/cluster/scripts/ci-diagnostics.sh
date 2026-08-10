# shellcheck shell=bash
# =============================================================================
# ci-diagnostics.sh — failure diagnostics for ci-cluster.sh.
# =============================================================================
# SOURCED into ci-cluster.sh's shell (never executed as a child): the dumpers
# read NODES/NODE_ROLE from lib-topology.sh's topology_load and BRIDGE_NAME
# from the entry script, and dump_diagnostics runs from the entry script's
# EXIT trap on failure. This file must NOT install traps of its own
# (ci-cluster.sh owns the single EXIT trap). Requires lib.sh (log) +
# lib-topology.sh.

# On failure, dump every job's status + each allocation's stdout/stderr BEFORE
# teardown — otherwise the container removal in the entry script's cleanup
# erases the only evidence of why an alloc failed (the workflow's post-step
# runs after this script's EXIT trap, too late). Best-effort; never let
# diagnostics mask the real exit code.
# Raw-UDP multicast reachability check from sealer-0 (192.168.56.51) to
# ingress-0 (192.168.56.31) on a throwaway group, bypassing Aeron entirely.
# Prints how many of 30 sent packets ingress-0 received: 0 means the bridge
# drops cross-node multicast (kernel/bridge issue); >0 means forwarding works and
# any remaining failure is Aeron-specific. python3 ships in the node image.
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
  # Host network state first: the cross-node Aeron data plane is UDP multicast,
  # and the usual failure is the bridge/netfilter dropping it. These reveal
  # whether bridged frames bypass iptables, whether FORWARD is dropping (counts),
  # and the bridge's multicast flags + joined groups per node.
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
  # Direct raw-UDP multicast probe sealer1 -> r1 on a throwaway group, INDEPENDENT of
  # Aeron. If r1 receives 0, the bridge isn't forwarding multicast at all (a
  # kernel/bridge problem); if it receives packets but Aeron still doesn't
  # record, the problem is Aeron-specific. Distinct group/port so it can't
  # collide with the media driver's sockets. Fully best-effort.
  multicast_probe || true
  # Observe the ACTUAL Aeron multicast frames on the bridge: are the sealer's
  # SETUP/DATA frames reaching the tx_ordering DATA group (239.192.56.13), and
  # are subscribers' Status Messages reaching the derived CONTROL group (.12)?
  # Capture all cluster multicast for a few seconds and summarise by src->dst so
  # we can see which streams actually flow and in which direction.
  echo "===== tcpdump ${BRIDGE_NAME:-kardamom-br0}: cluster multicast src->dst (≤8s) ====="
  # Show src -> dst so we can see DATA frames (-> .13/.21/.25 = odd data groups)
  # AND Status Messages (-> .12/.20/.24 = even control groups). If publications
  # never connect, we'll see data going out but NO Status Messages coming back to
  # the control groups.
  sudo timeout 8 tcpdump -i "${BRIDGE_NAME:-kardamom-br0}" -nn -t -c 250 \
    'udp and dst net 239.192.56.0/24' 2>/dev/null \
    | awk '{d=$4; sub(/:$/,"",d); print $2" -> "d}' | sort | uniq -c | sort -rn | head -40 \
    || echo "(tcpdump unavailable or no multicast captured)"
  export NOMAD_ADDR="http://192.168.56.10:4646"
  nomad job status 2>/dev/null || true
  # `validator` included: its job going DEAD (restart budget exhausted after
  # repeated fail-stops) is precisely the failure this dump needs to explain,
  # and `nomad job allocs`/`nomad alloc logs` still serve dead-but-not-GC'd
  # allocations. It was missing from this list once — the retention-shard
  # validator corpse (2026-08-04) left zero evidence.
  for job in aeron anvil cluster sealer sequencer executor validator ingress batcher da-watcher; do
    local allocs
    allocs="$(nomad job allocs -t '{{range .}}{{.ID}}{{"\n"}}{{end}}' "${job}" 2>/dev/null || true)"
    [[ -z "${allocs}" ]] && continue
    while read -r alloc; do
      [[ -z "${alloc}" ]] && continue
      echo "----- ${job} alloc ${alloc}: status -----"
      nomad alloc status "${alloc}" 2>/dev/null | sed -n '1,40p' || true
      # stderr carries the tracing logs. Show the HEAD (startup: channel setup,
      # "recording ready", "aggregating quorum watermark" — the markers that say
      # whether multicast carried data and quorum advanced) AND the tail (most
      # recent state), since the interesting events are at startup but failures
      # surface at the end.
      local err
      err="$(nomad alloc logs -stderr "${alloc}" 2>/dev/null || true)"
      echo "----- ${job} alloc ${alloc}: stderr (head 30) -----"
      printf '%s\n' "${err}" | head -30 || true
      echo "----- ${job} alloc ${alloc}: stderr (tail 40) -----"
      printf '%s\n' "${err}" | tail -40 || true
      # Full role/election history matters for the cluster job; more tail there.
      local stdout_tail=40
      [[ "${job}" == "cluster" ]] && stdout_tail=200
      echo "----- ${job} alloc ${alloc}: stdout (tail ${stdout_tail}) -----"
      nomad alloc logs "${alloc}" 2>/dev/null | tail -"${stdout_tail}" || true
    done <<<"${allocs}"
  done

  # Aeron Cluster members log fatal errors to *-error.log files in the cluster +
  # aeron dirs (NOT to the alloc stderr — the JVM exits 0 via the termination
  # hook after a ConsensusModule/ServiceContainer error). Dump them from each
  # sealer node (the dirs are bind-mounted from the node into the inner task).
  for n in "${NODES[@]}"; do
    [[ "${NODE_ROLE[$n]}" == "sealer" ]] || continue
    echo "===== ${n}: Aeron cluster error logs ====="
    docker exec "kardamom-${n}" sh -c '
      for f in /opt/kardamom/cluster/*error*.log /opt/kardamom/aeron-mount/cluster-dir/*error*.log; do
        [ -f "$f" ] && { echo "--- $f ---"; cat "$f"; }
      done' 2>/dev/null || true
  done

  # Consensus-module state per member (election phase, log/commit/append
  # positions, recovery plan) — the ground truth for "leader elected but
  # nothing commits" stalls. ClusterTool ships in the cluster-node jar; run it
  # inside each sealer node's INNER cluster task container (nomad docker task,
  # named by its alloc; find it via the inner docker ps).
  for n in "${NODES[@]}"; do
    [[ "${NODE_ROLE[$n]}" == "sealer" ]] || continue
    echo "===== ${n}: ClusterTool errors + members ====="
    docker exec "kardamom-${n}" sh -c '
      inner="$(docker ps --format "{{.Names}}" | grep -m1 "^cluster-")"
      [ -n "$inner" ] || { echo "(no inner cluster container running)"; exit 0; }
      # errors reads the CONSENSUS_MODULE and CONTAINER mark-file error buffers;
      # the CONTAINER one is where a dying clustered SERVICE logs (e.g. aeron
      # tmpfs exhaustion) and is NOT covered by the *.log file glob above.
      docker exec "$inner" java -cp /opt/kardamom/cluster-node.jar io.aeron.cluster.ClusterTool /opt/kardamom/cluster errors 2>&1 | tail -40
      echo "--- list-members ---"
      docker exec "$inner" java -cp /opt/kardamom/cluster-node.jar io.aeron.cluster.ClusterTool /opt/kardamom/cluster list-members 2>&1 | tail -3' 2>/dev/null || true
  done
}
