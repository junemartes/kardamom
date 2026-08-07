#!/usr/bin/env bash
# =============================================================================
# ci-cluster.sh — bring up the kardamom cluster with CONTAINERS as nodes, for
# CI (.github/workflows/cluster-e2e.yml). The VM-less analogue of `make up`.
# =============================================================================
#
# ⚠ EXPERIMENTAL / NOT YET RUN GREEN. This drives the full Ansible → Nomad →
# Consul orchestration on a single host using 11 privileged systemd containers
# (Docker-in-Docker for Nomad's docker driver) on a 192.168.56.0/24 bridge with
# the contract's static IPs — so the UNMODIFIED site.yml provisions them. It is
# expected to need iteration on a real runner; the static `cluster-validate`
# workflow is the always-green gate.
#
# Prereqs (the workflow sets these up): docker + buildx on the host; the host
# docker daemon trusts 192.168.56.10:5000 as an insecure registry; ansible with
# the community.docker collection; the kardamom release binaries already built
# under target/release (BIN-per-service); the nomad CLI on PATH.
#
# Usage:  deploy/cluster/scripts/ci-cluster.sh            # up + provision + deploy + smoke
#         KEEP=1 deploy/cluster/scripts/ci-cluster.sh     # leave containers running
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${CLUSTER_DIR}/../.." && pwd)"
cd "${CLUSTER_DIR}"

# Shared control-node helpers (on_control, running_alloc, all_allocs, ...)
# + log/fail — the same ones chaos.sh uses.
# shellcheck source=deploy/cluster/scripts/lib.sh
source "${SCRIPT_DIR}/lib.sh"
# Node-class model (topology_load + the executor/validator node+port mirrors).
# shellcheck source=deploy/cluster/scripts/lib-topology.sh
source "${SCRIPT_DIR}/lib-topology.sh"
# Prometheus scrape/parse helpers (fetch_metrics, prom_value).
# shellcheck source=deploy/cluster/scripts/lib-metrics.sh
source "${SCRIPT_DIR}/lib-metrics.sh"

NET=kardamom-net
SUBNET=192.168.56.0/24
REGISTRY=192.168.56.10:5000
TAG=dev
NODE_IMAGE=kardamom-node:ci
# NOTE: kardamom-recorder was removed (durability is now archive-at-the-sealer);
# the sealer image carries the durability sidecar.
SERVICES=(ingress sequencer executor validator da-watcher batcher)

# Node instances are GENERATED from the class model in group_vars/all.yml
# (node_classes: class -> {count, ip_start}) — the single source of truth. For
# each class we materialise `count` instances named <class>-<i>, each with a
# static IP from the class's ip_start lane on ip_prefix.0/24. Scaling a class is
# a one-line `count` change in group_vars; there is no node list / IP map / Ansible
# inventory to hand-maintain here (the inventory is generated below too). The
# regex parse (no PyYAML) lives in lib-topology.sh's topology_load, shared with
# smoke-load.sh; it populates NODES + NODE_IP/NODE_ROLE/NODE_TIER with
# `declare -g` so the cleanup/diagnostics traps below can read them.
topology_load \
  || { echo "ERROR: no nodes generated from node_classes" >&2; exit 1; }

# Generate the Ansible container inventory from the instances above: one group
# per role (site.yml provisions `all` + `control`), every host carrying its
# node_ip (consul/nomad bind_addr) + role (Nomad node meta for ${meta.role}
# placement). Written to a temp file so the repo carries no hand-maintained
# container inventory.
CONTAINER_INVENTORY="/tmp/kardamom-inventory.containers.ini"
gen_container_inventory() {
  : >"${CONTAINER_INVENTORY}"
  local roles_seen=() r n extra
  for n in "${NODES[@]}"; do
    r="${NODE_ROLE[$n]}"
    [[ " ${roles_seen[*]} " == *" ${r} "* ]] || roles_seen+=("${r}")
  done
  for r in "${roles_seen[@]}"; do
    echo "[${r}]" >>"${CONTAINER_INVENTORY}"
    for n in "${NODES[@]}"; do
      [[ "${NODE_ROLE[$n]}" == "${r}" ]] || continue
      extra=""; [[ "${r}" == "control" ]] && extra=" control_plane=true"
      # node_index = the <i> of <class>-<i>: the node's 0-based index within
      # its class, stamped as Nomad node meta (see nomad.hcl.j2).
      echo "${n} ansible_host=kardamom-${n} kardamom_node=${n} node_ip=${NODE_IP[$n]} role=${r} tier=${NODE_TIER[$n]} node_index=${n##*-}${extra}" >>"${CONTAINER_INVENTORY}"
    done
    echo "" >>"${CONTAINER_INVENTORY}"
  done
  cat >>"${CONTAINER_INVENTORY}" <<EOF
[all:vars]
ansible_connection=community.docker.docker
ansible_python_interpreter=/usr/bin/python3
kardamom_in_container=true
EOF
}

# Push a locally-built image to the in-cluster registry.
#
# On a CI runner the host docker daemon reaches 192.168.56.10:5000 directly, so
# this is a plain `docker push` (REGISTRY_PUSH_NODE unset → unchanged behavior).
#
# On the local Docker-Desktop harness (local-cluster.sh) the VM daemon's registry
# traffic is hijacked by Docker Desktop's transparent HTTP proxy
# (http.docker.internal:3128), whose bypass list does NOT include the VM-internal
# 192.168.56.0/24 bridge — so a direct push routes through that proxy, which can't
# reach the bridge IP, and hangs until "context deadline exceeded" (the registry
# never receives the request). Side-step the proxy entirely by moving the image
# engine-to-engine over the docker socket (`docker save | docker exec … docker
# load`) into REGISTRY_PUSH_NODE's inner docker, then pushing from THERE: that node
# is co-located with the registry (cp1), so its push is node-local and never touches
# the proxy. The other nodes still pull from the registry over the bridge as usual.
push_image() {
  local img="$1"
  if [[ -n "${REGISTRY_PUSH_NODE:-}" ]]; then
    log "push ${img} via kardamom-${REGISTRY_PUSH_NODE} (proxy-safe engine-to-engine load)"
    docker save "${img}" | docker exec -i "kardamom-${REGISTRY_PUSH_NODE}" docker load
    docker exec "kardamom-${REGISTRY_PUSH_NODE}" docker push "${img}"
  else
    docker push "${img}"
  fi
}

# On failure, dump every job's status + each allocation's stdout/stderr BEFORE
# teardown — otherwise the container removal below erases the only evidence of
# why an alloc failed (the workflow's post-step runs after this script's EXIT
# trap, too late). Best-effort; never let diagnostics mask the real exit code.
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

cleanup() {
  [[ "${KEEP:-0}" == "1" ]] && { log "KEEP=1; leaving containers up"; return; }
  log "tearing down containers + network + docker volumes"
  for n in "${NODES[@]}"; do
    docker rm -f "kardamom-${n}" >/dev/null 2>&1 || true
    docker volume rm "kardamom-${n}-docker" "kardamom-${n}-containerd" >/dev/null 2>&1 || true
  done
  docker network rm "${NET}" >/dev/null 2>&1 || true
}

on_exit() {
  local rc="$1"
  [[ "${rc}" != "0" ]] && dump_diagnostics
  cleanup
}
trap 'on_exit "$?"' EXIT

# --- 1. Host sysctls (NOT namespaced; can't be set from inside a container) --
log "applying Aeron socket-buffer sysctls on the host"
sudo sysctl -w net.core.rmem_max=16777216 net.core.wmem_max=16777216 \
                net.core.rmem_default=16777216 net.core.wmem_default=16777216

# Let bridged frames bypass iptables. With br_netfilter loaded and
# bridge-nf-call-iptables=1 (docker's default), L2-bridged IP frames traverse
# the iptables FORWARD chain — whose policy docker sets to DROP, with ACCEPT
# rules only for the traffic it recognises. Flooded UDP multicast (Aeron's
# cross-node tx_ordering + fsync/quorum watermarks) doesn't match those rules,
# so it's dropped in FORWARD even though IGMP snooping is off — unicast TCP
# (Consul/Nomad/ingress RPC) is unaffected because docker DOES allow it, which
# is exactly why the recorders log "recording initiated" but never "recording
# ready". Setting bridge-nf-call-iptables=0 makes bridged frames pure L2, so the
# bridge floods multicast between node containers unimpeded. Safe here: one
# isolated test network, no inter-network isolation to preserve.
sudo modprobe br_netfilter 2>/dev/null || true
if [[ -e /proc/sys/net/bridge/bridge-nf-call-iptables ]]; then
  sudo sysctl -w net.bridge.bridge-nf-call-iptables=0 \
                 net.bridge.bridge-nf-call-ip6tables=0 \
    && log "bridge-nf-call-iptables=0 (bridged multicast bypasses iptables)" \
    || log "WARN: could not clear bridge-nf-call-iptables"
else
  log "br_netfilter not present; bridged frames already bypass iptables"
fi

# --- 2. Bridge network with the contract subnet -----------------------------
docker network rm "${NET}" >/dev/null 2>&1 || true
log "creating bridge ${NET} (${SUBNET})"
# Name the underlying Linux bridge so its /sys path is deterministic (otherwise
# docker derives br-<network-id[:12]>). We disable IGMP snooping on it next.
BRIDGE_NAME=kardamom-br0
# Idempotent: reuse an existing ${NET} (e.g. a KEEP=1 run or local iteration) —
# recreating it would also recreate the underlying bridge and silently reset
# multicast_snooping to the kernel default (1), undoing the fix below on hosts
# where this script cannot sudo.
if ! docker network inspect "${NET}" >/dev/null 2>&1; then
  docker network create --driver bridge \
    -o "com.docker.network.bridge.name=${BRIDGE_NAME}" \
    --subnet "${SUBNET}" "${NET}"
else
  log "network ${NET} already exists; reusing"
fi

# Aeron's cross-node data plane is UDP MULTICAST (tx_ordering: sealer ->
# recorders + executor; the per-recorder fsync watermarks and the aggregated
# quorum watermark). This bridge is an isolated L2 segment with NO IGMP querier,
# so with the kernel default (multicast_snooping=1) the bridge stops flooding
# group traffic once its snooping state lapses — multicast then never crosses
# between node containers. The symptom is precise: each recorder logs "recording
# initiated" for TxOrdering but never "recording ready" (the archive's recording
# subscription gets no image), so no fsync watermark is published, the quorum
# watermark never advances, and ingress (--ack-policy on-quorum) times out with
# "timed out waiting for receipt or watermark". Disabling snooping makes the
# bridge flood multicast to every port — correct and reliable for a 5-node test
# segment. (Best-effort: warn but continue if /sys isn't writable.)
SNOOP_PATH="/sys/class/net/${BRIDGE_NAME}/bridge/multicast_snooping"
if echo 0 | sudo tee "${SNOOP_PATH}" >/dev/null 2>&1; then
  log "disabled IGMP snooping on ${BRIDGE_NAME} (multicast flood mode)"
else
  log "WARN: could not write ${SNOOP_PATH}; cross-node multicast may be filtered"
fi

# --- 3. Node image + 5 systemd containers -----------------------------------
log "building node image"
docker build -f docker/node.Dockerfile -t "${NODE_IMAGE}" docker/
for n in "${NODES[@]}"; do
  log "starting node ${n} (${NODE_IP[$n]})"
  # Dedicated volumes for the inner Docker's storage. Without them the in-node
  # Docker (DinD — Nomad's docker driver + the registry) puts its overlay
  # layers on the node container's OWN overlay rootfs, and the kernel rejects
  # overlay-on-overlay ("mount overlay ... invalid argument"). Both are needed:
  # /var/lib/docker (engine state) AND /var/lib/containerd (the containerd
  # image-store snapshotter, where modern Docker keeps image layers). The
  # volumes live on the host's real fs, where overlay works.
  docker volume create "kardamom-${n}-docker" >/dev/null
  docker volume create "kardamom-${n}-containerd" >/dev/null
  # Idempotent: a node left up by a previous KEEP=1 run is reused as-is.
  if docker inspect "kardamom-${n}" >/dev/null 2>&1; then
    log "node kardamom-${n} already exists; reusing"
    docker start "kardamom-${n}" >/dev/null 2>&1 || true
    continue
  fi
  docker run -d --name "kardamom-${n}" --hostname "${n}" \
    --privileged --cgroupns=host \
    -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
    -v "kardamom-${n}-docker:/var/lib/docker" \
    -v "kardamom-${n}-containerd:/var/lib/containerd" \
    --tmpfs /run --tmpfs /run/lock \
    --network "${NET}" --ip "${NODE_IP[$n]}" \
    --shm-size=512m \
    "${NODE_IMAGE}" >/dev/null
done

# Wait for systemd to be ready in each (systemctl is-system-running returns
# running/degraded once up).
for n in "${NODES[@]}"; do
  log "waiting for systemd in ${n}"
  for _ in $(seq 1 30); do
    state="$(docker exec "kardamom-${n}" systemctl is-system-running 2>/dev/null || true)"
    [[ "${state}" == "running" || "${state}" == "degraded" || "${state}" == "starting" ]] && break
    sleep 1
  done
done

# --- 4. Provision with the UNMODIFIED site.yml over the docker connection ----
gen_container_inventory
log "ansible-playbook site.yml (generated container inventory: ${CONTAINER_INVENTORY})"
ANSIBLE_HOST_KEY_CHECKING=False \
  ansible-playbook -i "${CONTAINER_INVENTORY}" ansible/site.yml

# --- 5. Build thin service images from prebuilt binaries, push to r1 ---------
# The workflow ran `cargo build --release` for each BIN; wrap each into a thin
# image and push to the in-cluster registry (reachable on the bridge IP).
log "building + pushing service images to ${REGISTRY}"
# Aeron image (same canonical Dockerfile as make images).
docker build -f "${ROOT}/crates/log/docker/aeron/Dockerfile" \
  -t "${REGISTRY}/kardamom-aeron:${TAG}" "${ROOT}/crates/log/docker/aeron"
push_image "${REGISTRY}/kardamom-aeron:${TAG}"

# The binaries link Aeron DYNAMICALLY (rusteron's static feature is broken on
# Linux), so the thin runtime image must carry libaeron.so /
# libaeron_archive_c_client.so. rusteron builds them under the cargo build dir;
# stage them into the image build context (target/release) so the Dockerfile
# can COPY them in. ldconfig in the image then makes them resolvable.
staging="${ROOT}/target/release/_aeronlibs"
rm -rf "${staging}"; mkdir -p "${staging}"
found=0
while IFS= read -r so; do
  cp -f "${so}" "${staging}/"; found=$((found + 1))
done < <(find "${ROOT}/target/release/build" -path '*/out/build/lib/libaeron*.so' 2>/dev/null)
log "staged ${found} Aeron shared lib(s): $(ls "${staging}" 2>/dev/null | tr '\n' ' ')"
[[ "${found}" -gt 0 ]] || { echo "ERROR: no libaeron*.so found under target/release/build" >&2; exit 1; }

for svc in "${SERVICES[@]}"; do
  bin="kardamom-${svc}"
  docker build -f docker/ci-service.Dockerfile --build-arg "BIN=${bin}" \
    -t "${REGISTRY}/${bin}:${TAG}" "${ROOT}/target/release"
  push_image "${REGISTRY}/${bin}:${TAG}"
done

# --- 5b. Cluster image (Java Aeron-Cluster node) -----------------------------
# The clustered sealer (cluster.nomad.hcl, Phase 3) runs a PURE-JVM Aeron Cluster
# member, NOT a Rust SERVICE — so it's built separately from the SERVICES loop
# above: the workflow runs `./gradlew :service:shadowJar` (before this script) to
# produce kardamom-cluster-node.jar; here we stage that jar into the Dockerfile's
# build context and build/push the image deploy.sh's cluster.nomad.hcl pulls
# (192.168.56.10:5000/kardamom-cluster:dev). cluster.Dockerfile COPYs the jar.
CLUSTER_JAR="${ROOT}/cluster/sealer-service/service/build/libs/kardamom-cluster-node.jar"
if [[ -f "${CLUSTER_JAR}" ]]; then
  log "building + pushing cluster image (Java Aeron-Cluster node) to ${REGISTRY}"
  cp "${CLUSTER_JAR}" "${ROOT}/deploy/cluster/docker/kardamom-cluster-node.jar"
  docker build -f "${ROOT}/deploy/cluster/docker/cluster.Dockerfile" \
    -t "${REGISTRY}/kardamom-cluster:${TAG}" "${ROOT}/deploy/cluster/docker"
  push_image "${REGISTRY}/kardamom-cluster:${TAG}"
else
  # The deploy uses the CLUSTERED sealer (cluster.nomad.hcl), so a missing jar is
  # fatal: deploy.sh would fail pulling kardamom-cluster:${TAG}. Fail loudly here.
  echo "ERROR: cluster jar not found at ${CLUSTER_JAR}" >&2
  echo "       Build it first: (cd cluster/sealer-service && ./gradlew :service:shadowJar)" >&2
  exit 1
fi

# --- 6. Deploy the Nomad jobs + smoke ---------------------------------------
export NOMAD_ADDR="http://192.168.56.10:4646"
# Runner identity, stamped for EVERY shard (#124: chaos-case load verdicts
# show the same intermittent collapses as the load shard).
log "runner: name=${RUNNER_NAME:-local} host=$(hostname) cpus=$(nproc) loadavg=[$(cut -d' ' -f1-3 /proc/loadavg)] mem_avail_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo)"

log "deploy.sh (Nomad endpoint ${NOMAD_ADDR})"
./scripts/deploy.sh

log "smoke test (gate: single-tx must pass before load smoke runs)"
./scripts/smoke.sh

# --- 7. Sustained-load invariant gate (Rust harness: FIXED-RATE soak;
# must-deliver + drop accounting + keep-pace) then 8. chaos/resilience
# suite — env-gated stages.
# Both stages drive the Rust kardamom-load harness (built alongside the service
# bins by the workflow, staged at target/release). Durations/rates are ENV-tunable
# so PR runs stay short and a full soak can be dialed up via the cluster-e2e.yml
# workflow_dispatch / matrix shard knobs.
#
# Funded-account budget: genesis prefunds Anvil accounts #0..#17, each its own
# contiguous nonce chain on the never-reset chain (a fresh account's first tx
# MUST be nonce 0, with no gaps). Allocation: #0 = gate smoke above; #1..#6 =
# the sustained-load harness; #7..#15 = one fresh account per chaos case (see
# chaos.sh); #16 = the ingress-churn failover re-smoke (step 7b); #17 = the
# fallback executor-churn re-smoke. Every check owns its own account, so every
# smoke tx is nonce 0 — there's no NONCE knob and no nonce-continuation.
# RUN_LOAD / RUN_CHAOS (default 1) let a CI shard run just one stage so the full
# suite can be split across runners (each shard brings up its own cluster). When
# unset both default to 1 — the local / single-runner path runs smoke + the
# default cluster chaos cases. CHAOS_CASES selects which chaos cases to run; in
# cluster mode the default set is the three Raft cases (see chaos.sh).
LOAD_BIN="${ROOT}/target/release/kardamom-load"
if [[ -x "${LOAD_BIN}" ]]; then
  if [[ "${RUN_LOAD:-1}" == "1" ]]; then
    # Runner-identity stamping (#124): the load ceiling swings 800→18 on
    # identical code, and "degraded runner window" has only ever been an
    # inference. Stamp WHO is running this shard and HOW LOADED the host is —
    # at start and sampled through the load stages — so collapses correlate
    # with runner hosts/windows instead of being guessed at. RUNNER_NAME is
    # exported by the workflow (github's runner.name); absent locally.
    log "load runner: name=${RUNNER_NAME:-local} host=$(hostname) cpus=$(nproc) loadavg=[$(cut -d' ' -f1-3 /proc/loadavg)] mem_avail_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo)"
    (
      while true; do
        log "load runner sample: loadavg=[$(cut -d' ' -f1-3 /proc/loadavg)] mem_avail_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo)"
        sleep 30
      done
    ) &
    LOADAVG_SAMPLER_PID=$!
    # No EXIT trap here — it would CLOBBER on_exit's teardown trap. The
    # sampler is killed explicitly after the load stages; on a mid-load
    # failure exit the orphan is reaped by the CI job's process cleanup.

    # FIXED-RATE invariant gate, not edge discovery: this shard runs on
    # 4-core GH-hosted VMs where the whole stack + harness share the host —
    # a ramp-to-edge there measures the hypervisor (ceilings swung 800→18 on
    # identical code) and soaking at 0.8×edge parks the run AT the edge on a
    # bad VM, failing deadlines sized for healthy hosts. Correctness (zero
    # loss, no gaps, keep-pace, no divergence) is rate-independent: gate on
    # it at a rate the weakest runner sustains. Throughput/latency numbers
    # in the report are informational; real perf comes from the perf suite
    # on dedicated hardware.
    log "load test: kardamom-load fixed-rate invariant gate (duration=${LOAD_DURATION_S:-60}s rate=${LOAD_TARGET_TPS:-200}tps)"
    # --chain-id is passed explicitly (412346, from group_vars/all.yml) rather
    # than probed via eth_chainId: ingress.toml sets no chain_id, so its
    # eth_chainId returns a default that does NOT match the executors' chain, and
    # txs signed with it would never execute. smoke.sh hardcodes 412346 likewise.
    # Sender offset 1 reserves account #0 for the single-tx smoke gate above.
    "${LOAD_BIN}" --rpc http://192.168.56.31:8545 --chain-id 412346 --fixed-rate \
      --duration "${LOAD_DURATION_S:-60}s" --target-tps "${LOAD_TARGET_TPS:-200}" \
      --senders "${LOAD_SENDERS:-6}" --sender-offset 1 --assert-all-delivered \
      --completeness accepted --max-gap "${LOAD_MAX_GAP:-5}" \
      --scrape executor,ingress,sequencer --output /tmp/kardamom-load.json

    # DeFi stage: CLOB + swap-pool + vault mix (contracts deployed by the
    # harness from its first sender). Contract-shaped gas + write sets —
    # exercises the BAL attribution + parallel-validation path under
    # realistic storage churn, reported gas-centrically (Mgas/s). Fresh
    # nonces continue from the transfer stage via --nonce-start probing:
    # kardamom-load starts at 0, so this stage uses DIFFERENT senders
    # (offset past the transfer stage's) to keep nonce bookkeeping trivial.
    log "load test: kardamom-load DEFI stage (duration=${DEFI_DURATION_S:-45}s rate=${DEFI_TARGET_TPS:-100}tps)"
    "${LOAD_BIN}" --rpc http://192.168.56.31:8545 --chain-id 412346 --fixed-rate \
      --workload defi \
      --duration "${DEFI_DURATION_S:-45}s" --target-tps "${DEFI_TARGET_TPS:-100}" \
      --senders "${DEFI_SENDERS:-6}" --sender-offset "$((1 + ${LOAD_SENDERS:-6}))" \
      --assert-all-delivered --completeness accepted --max-gap "${LOAD_MAX_GAP:-5}" \
      --scrape executor,ingress,sequencer --output /tmp/kardamom-load-defi.json

    # Load stages done — stop the runner sampler (#124).
    kill "${LOADAVG_SAMPLER_PID}" 2>/dev/null || true
    LOADAVG_SAMPLER_PID=""
  else
    log "RUN_LOAD=0 — skipping sustained-load stage (chaos-only shard)"
  fi

  # --- chain-semantics suite (Target C) --------------------------------
  # The SAME scenario drivers the Target-L tests run
  # (crates/e2e/src/scenarios, docs/agents/chain-semantics-e2e-suite-spec.md),
  # pointed at this cluster instead of a single-host stack: nonce ordering,
  # gap non-processing, RPC liveness and validator/executor consistency, all
  # observed through the ingress JSON-RPC + the services' /metrics.
  #
  # OFF by default (RUN_SEMANTICS=0) so it only runs on its own shard and can
  # never affect the load / chaos shards. Executor, sequencer and validator
  # metrics all bind 0.0.0.0 in their Nomad jobs, so the runner scrapes them
  # directly (the same way chaos.sh's progress probe does); the ingress binds
  # loopback-only, so the one probe that needs ingress metrics self-skips.
  #
  # Accounts: this shard runs no load/chaos, so only the smoke gate (#0) has
  # been used — the semantics cases own #1..#15 (see the ledger above).
  if [[ "${RUN_SEMANTICS:-0}" == "1" ]]; then
    SEMANTICS_BIN="${ROOT}/target/release/kardamom-semantics"
    if [[ -x "${SEMANTICS_BIN}" ]]; then
      # The l1-batch case (#39) asserts the live batcher's L2 → L1 round trip
      # against the in-cluster anvil. The settlement proxy address is
      # deterministic on-chain state — re-resolve it the same way deploy.sh's
      # Phase 2b did rather than plumbing it through a file.
      SETTLEMENT_ADDRESS="${SETTLEMENT_ADDRESS:-}"
      DEPLOY_BIN="${ROOT}/target/release/kardamom-deploy"
      if [[ -z "${SETTLEMENT_ADDRESS}" && -x "${DEPLOY_BIN}" ]]; then
        # Registry ids print as hashes; the settlement is the only contract
        # registered for this chain id (deploy.sh Phase 2b), so first proxy
        # line wins.
        SETTLEMENT_ADDRESS="$("${DEPLOY_BIN}" --rpc-url http://192.168.56.10:8546 \
          --owner 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266 \
          addresses --l2-chain-id 412346 2>/dev/null \
          | awk '/proxy/ && !found {print $2; found=1}' || true)"
      fi
      if [[ -z "${SETTLEMENT_ADDRESS}" ]]; then
        log "ERROR: could not resolve the settlement address for the l1-batch case"
        exit 1
      fi
      log "chain-semantics suite (Target C): ${SEMANTICS_CASES:-nonce-unordered,nonce-gap,rpc-liveness,rpc-vectors,consistency,l1-batch} (settlement ${SETTLEMENT_ADDRESS})"
      "${SEMANTICS_BIN}" \
        --rpc http://192.168.56.31:8545 --chain-id 412346 \
        --executor-metrics 192.168.56.41:9004,192.168.56.42:9004,192.168.56.43:9004 \
        --sequencer-metrics 192.168.56.21:9001,192.168.56.22:9001 \
        --validator-metrics 192.168.56.61:9006 \
        --pending-receipt-timeout-ms "${SEMANTICS_PARK_MS:-30000}" \
        --account-base "${SEMANTICS_ACCOUNT_BASE:-1}" \
        --l1-rpc http://192.168.56.10:8546 \
        --settlement "${SETTLEMENT_ADDRESS}" \
        --cases "${SEMANTICS_CASES:-nonce-unordered,nonce-gap,rpc-liveness,rpc-vectors,consistency,l1-batch}"
    else
      log "ERROR: RUN_SEMANTICS=1 but ${SEMANTICS_BIN} is not staged"
      exit 1
    fi
  fi

  if [[ "${RUN_CHAOS:-1}" == "1" ]]; then
    # The chaos suite kills pipeline components under steady load and asserts they
    # auto-recover AND every accepted tx still receipts. The COMPONENT cases
    # (graceful/hard-executor, etc.) exercise the executor's same-node restart: it
    # reopens its persistent /opt/kardamom/state and runs Phase-2 crash recovery
    # (replays tx_ordering + tx_data + tx_deposits from the archive, skip-counts
    # past its durable cursor) — so a broken recovery surfaces here as a missed
    # receipt or a crash-loop that never restores the executor count. (Needs the
    # executor's archive replay endpoints + ingress/da-watcher --archive-durability,
    # wired in the jobs.) The CLUSTER cases exercise the Raft sealer (leader /
    # follower kill, quorum loss + recovery).
    log "chaos suite (kills components under steady load; asserts auto-recovery)"
    # Default cases are the CLUSTERED-sealer Raft cases (Phase 3): the deploy uses
    # cluster.nomad.hcl, so the always-on cluster gate exercises leader-kill /
    # follower-kill / quorum-loss-recover. A shard can override CHAOS_CASES to run
    # the component (executor/ingress/sequencer/sealer) cases instead.
    CHAOS_TPS="${CHAOS_TPS:-50}" CHAOS_CASE_S="${CHAOS_CASE_S:-45}" \
      CHAOS_CASES="${CHAOS_CASES:-cluster-leader-kill cluster-follower-kill cluster-quorum-loss-recover}" \
      CHAOS_RESTART_SLO_S="${CHAOS_RESTART_SLO_S:-60}" \
      CHAOS_RESCHEDULE_SLO_S="${CHAOS_RESCHEDULE_SLO_S:-150}" \
      CHAOS_LEADER_SLO_S="${CHAOS_LEADER_SLO_S:-30}" \
      LOAD_BIN="${LOAD_BIN}" LOAD_MAX_GAP="${LOAD_MAX_GAP:-5}" \
      ./scripts/chaos.sh
  else
    log "RUN_CHAOS=0 — skipping chaos stage (load-only shard)"
  fi
else
  # Fallback (kardamom-load not staged): the legacy bash load smoke (accounts
  # #1..#N) + single subscriber-churn check. Kept so a cluster bring-up without the
  # harness still exercises a sustained stream + a basic resilience event.
  log "WARN: ${LOAD_BIN} not found — running legacy load smoke + subscriber-churn"
  # The load smoke starts its sender set at account #1 (offset 1) so its nonces
  # never collide with account #0 (reserved for the smoke gate + churn re-smoke).
  SMOKE_SENDER_OFFSET=1 ./scripts/smoke-load.sh
  log "subscriber-churn: stopping one executor alloc and re-running smoke"
  alloc="$(running_alloc executor || true)"
  [ -n "${alloc}" ] && on_control 'nomad alloc stop "$1"' "${alloc}" || true
  sleep 5
  # Re-smoke from a dedicated account (#17), disjoint from the gate (#0) and the
  # ingress-churn re-smoke (#16) — every check owns its own nonce-0 account.
  PK="0x689af8efa8c651a91ad287602527f3af2fe9f6501a7ac4b061667b5a93e037fd" \
    ./scripts/smoke.sh
fi

# --- 7b. Ingress active/active failover + multicast-receipts freeze guard ----
# Active/active ingress (docs/agents/resilient-ingress-spec.md): kill ingress-0
# (@.31) and re-smoke against the surviving ingress-1 (@.32). This validates two
# things at once:
#   (a) FAILOVER — a client that loses one ingress is served by another replica
#       (both accept txs, recover the sender, and route to all sequencer shards).
#   (b) The 2a MULTICAST-RECEIPTS FREEZE GUARD — ingress-0 leaving the shared
#       tx_receipts multicast group must NOT freeze the image for ingress-1 (the
#       exact subscriber-churn pathology MDS was introduced to avoid; see the
#       TxReceipts section of config/channels.toml.tpl). If the group froze,
#       ingress-1 would never receive the receipt and this re-smoke would time
#       out — i.e. this IS the freeze reproduction. If it fails here, tighten the
#       Aeron image-liveness / no_unavailable_image handling until it does not.
log "ingress-churn: stopping ingress-0 and re-smoking against ingress-1 (.32)"
docker exec kardamom-control-0 bash -lc 'export NOMAD_ADDR=http://192.168.56.10:4646; \
  alloc=$(nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}{{.NodeName}} {{.ID}}{{\"\n\"}}{{end}}{{end}}" ingress 2>/dev/null | awk "/ingress-0/{print \$2; exit}"); \
  [ -n "$alloc" ] && nomad alloc stop "$alloc" || true' || true
sleep 5
# Re-smoke against the surviving ingress-1 from a DEDICATED funded account (#16,
# genesis dev.toml) — disjoint from the gate (#0), load (#1..#6), chaos
# (#7..#15), and the fallback executor-churn (#17), so there is no cross-stage
# nonce coordination regardless of which branch above ran.
PK="0xea6c44ac03bff858b476bba40716402b03e41b8e97e276d1baec7c37d42484a0" \
  RPC_URL="http://192.168.56.32:8545" ./scripts/smoke.sh

# --- 7c. Validator sync + keep-up verdict -------------------------------------
# The validator (validator.nomad.hcl, one alloc on the aux node — kept out of
# the executor-chaos blast radius)
# followed everything the shard just did — bring-up, smoke, load and/or chaos —
# re-executing every block through the shared engine and cross-checking against
# the executors' receipts + per-block BAL, advancing an MPT state root. Assert:
#   (a) LIVENESS  — its /metrics endpoint (:9006) answers: the process did NOT
#       fail-stop (a proven divergence exits 2 and the job never restarts).
#   (b) SYNC + KEEP-UP — validator_committed_block advances and closes to within
#       VALIDATOR_LAG_MAX of the executors' kardamom_executor_block_number.
#   (c) VERIFICATION — validator_blocks_verified_total > 0 (the BAL cross-check
#       actually ran) and validator_divergence_total is absent/0.
# This is the cluster-mode successor of the old multiprocess
# `multiprocess_e2e_validator_syncs_and_keeps_up` smoke (removed with the
# single-sealer full-pipeline e2e in the cluster-only migration).
log "validator verdict: sync + keep-up + BAL cross-check (no divergence)"
# EXECUTOR_NODES + VALIDATOR_NODES/VALIDATOR_PORT/EXECUTOR_PORT come from
# lib-topology.sh (the validator lives on the aux node — see
# validator.nomad.hcl, kept out of the executor-chaos blast radius).
VALIDATOR_LAG_MAX="${VALIDATOR_LAG_MAX:-10}"
VALIDATOR_SYNC_TIMEOUT_S="${VALIDATOR_SYNC_TIMEOUT_S:-180}"

# Scrape one metric value from a node:port (docker-exec path; the int value of
# the first matching sample — every metric probed here is single-series).
scrape_metric() { # <node> <port> <metric-name> -> value or ""
  local body
  body="$(fetch_metrics "" "$1" "$2" || true)"
  prom_value "${body}" "$3" first
}

# Retry window: a single-shot probe here raced slow container starts (fresh
# image pull on the aux node) and failed an otherwise-healthy deploy — the
# exporter installs on the validator's FIRST log line, so patience is all
# that's needed to tell "starting" from "fail-stopped".
VALIDATOR_NODE=""
for _ in $(seq 1 24); do
  for n in "${VALIDATOR_NODES[@]}"; do
    if timeout 8 docker exec "$n" curl -fsS --max-time 5 "http://127.0.0.1:${VALIDATOR_PORT}/metrics" >/dev/null 2>&1; then
      VALIDATOR_NODE="$n"; break 2
    fi
  done
  sleep 5
done
[[ -n "${VALIDATOR_NODE}" ]] || { echo "FAIL: no validator /metrics on :${VALIDATOR_PORT} after 120s (fail-stopped — divergence or session death?)" >&2; exit 1; }
log "validator found on ${VALIDATOR_NODE}"

# Executor progress reference: any responding executor (state-machine replicas).
executor_block() {
  local v
  for n in "${EXECUTOR_NODES[@]}"; do
    v="$(scrape_metric "$n" "${EXECUTOR_PORT}" kardamom_executor_block_number)"
    [[ -n "$v" ]] && { printf '%.0f\n' "$v"; return 0; }
  done
  return 1
}

# What the verdict asserts per shard tracks what the DESIGN guarantees:
#
# LOAD shard — full sync + bounded lag. The canonical stream is loss-proof
# WITHIN the retention window (cluster sessions + retention + REPLAY_FROM on
# every establishment; a cursor below the retention floor gets
# REPLAY_UNAVAILABLE and takes the peer-checkpoint resync path instead), and
# under plain load the validator demonstrably keeps up.
#
# CHAOS shards — fail-stop safety + forward progress, NOT bounded lag. The
# canonical stream survives chaos (verified: the subscription cursor advances
# through leader kills and quorum loss via replay + catch-up ordering), but
# the MULTICAST side-streams (tx_bal, tx_receipts, tx_data envelopes) have no
# retention/refetch yet: a deliberately-slower-than-hot-path verifier that
# falls behind a 200tps chaos barrage lapses those images and then pays the
# BAL wait per block — bounded-lag-after-chaos requires the archive-backed
# side-stream refetch (tracked follow-up), not test tuning.
v_start="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_committed_block)"; v_start="${v_start:-0}"
v_start="$(printf '%.0f' "${v_start}")"
if [[ "${RUN_LOAD:-1}" == "1" ]]; then
  deadline=$(( $(date +%s) + VALIDATOR_SYNC_TIMEOUT_S ))
  ok=0
  while (( $(date +%s) < deadline )); do
    e_blk="$(executor_block || echo "")"
    v_blk="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_committed_block)"
    v_blk="$(printf '%.0f' "${v_blk:-0}")"
    if [[ -n "${e_blk}" ]] && (( v_blk > 0 )) && (( e_blk - v_blk <= VALIDATOR_LAG_MAX )); then
      ok=1; break
    fi
    sleep 5
  done
  e_blk="${e_blk:-?}"
  if (( ok != 1 )); then
    echo "FAIL: validator did not sync within ${VALIDATOR_SYNC_TIMEOUT_S}s (validator=${v_blk:-?} executor=${e_blk}, started at ${v_start})" >&2
    # A frozen committed-block gauge is the WEDGE signature (F3 class): the
    # reason lives only in the validator's own log on the ephemeral runner —
    # dump its tail or the failure is undiagnosable (the 2026-08-04 #155
    # load-shard wedge at block 227 left nothing).
    echo "----- validator alloc log tails -----" >&2
    while read -r valloc; do
      [[ -z "${valloc}" ]] && continue
      echo "== alloc ${valloc} (stderr tail)" >&2
      on_control 'nomad alloc logs -stderr "$1" 2>/dev/null | tail -60' "${valloc}" >&2 2>/dev/null || true
    done < <(all_allocs validator)
    exit 1
  fi
  log "validator synced: block ${v_blk} vs executor ${e_blk} (lag $(( e_blk - v_blk )) <= ${VALIDATOR_LAG_MAX})"
else
  # Forward progress: the validator must still be verifying and committing.
  deadline=$(( $(date +%s) + 60 ))
  ok=0
  while (( $(date +%s) < deadline )); do
    v_blk="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_committed_block)"
    v_blk="$(printf '%.0f' "${v_blk:-0}")"
    if (( v_blk > v_start )); then ok=1; break; fi
    sleep 5
  done
  if (( ok != 1 )); then
    echo "FAIL: validator made no progress after chaos (stuck at ${v_blk:-?})" >&2
    exit 1
  fi
  log "chaos shard: validator progressing (${v_start} -> ${v_blk}); bounded lag asserted on the load shard"
fi

verified="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_blocks_verified_total)"
verified="$(printf '%.0f' "${verified:-0}")"
(( verified > 0 )) || { echo "FAIL: validator verified 0 blocks against the BAL (tx_bal not flowing?)" >&2; exit 1; }
diverged="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_divergence_total)"
diverged="$(printf '%.0f' "${diverged:-0}")"
(( diverged == 0 )) || { echo "FAIL: validator counted ${diverged} divergence(s)" >&2; exit 1; }
# The metric resets if the alloc restarted (recovery loop); a PRE-restart
# divergence still shows in the alloc log — catch it there too. Check EVERY
# alloc of the job (not just the first): a rescheduled validator gets a new
# alloc, and the "halted on divergence" line would live in the OLD alloc's log.
# NEVER `producer | grep -q` under pipefail here: grep -q exits at the first
# match, the producer takes SIGPIPE once the logs exceed the pipe buffer, and
# the successful match is DISCARDED — a real divergence reads as absence
# (chaos.sh header documents the observed failure; fixed there in #158).
# Capture first, then substring-match in pure bash.
divergence_logged=0
while read -r valloc; do
  [[ -z "${valloc}" ]] && continue
  vlogs="$(on_control 'nomad alloc logs "$1" 2>/dev/null' "${valloc}" 2>/dev/null || true)"
  if [[ "${vlogs}" == *"halted on divergence"* ]]; then
    divergence_logged=1
    break
  fi
done < <(all_allocs validator)
if (( divergence_logged == 1 )); then
  echo "FAIL: validator halted on divergence (found in alloc ${valloc} log)" >&2
  # Print the reason + context: the divergence line names claimed vs
  # recomputed (or the receipt fields). Without this the CI log proves a
  # divergence happened but not WHICH — the reason string only lived on
  # the ephemeral runner. Also surface any flight-recorder dumps.
  echo "----- divergence context (alloc ${valloc}) -----" >&2
  printf '%s\n' "${vlogs}" \
    | grep -B3 -A10 "halted on divergence" >&2 || true
  echo "----- flight-recorder dumps on validator node (if any) -----" >&2
  docker exec "${VALIDATOR_NODE}" sh -c \
    'for f in /opt/kardamom/state/divergence-*.json; do [ -f "$f" ] && { echo "== $f"; head -c 4096 "$f"; echo; }; done' \
    2>/dev/null >&2 || true
  exit 1
fi
# Incremental-trie shadow-check (--trie-shadow-check 8 in validator.nomad.hcl):
# every 8th committed block recomputes the state root by FULL rebuild and
# compares it to the node-incremental walker's (every-block checking would
# saturate a CI core — see the cadence rationale in validator.nomad.hcl). A
# mismatch fail-stops the validator (caught by the liveness probe above);
# assert the counters directly too.
# Runs on every shard: the validator commits blocks (v_blk > 0 asserted above
# on both the load and chaos paths), so checks_total must be > 0 everywhere.
shadow_checks="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" kardamom_state_trie_shadow_checks_total)"
shadow_checks="$(printf '%.0f' "${shadow_checks:-0}")"
(( shadow_checks > 0 )) || { echo "FAIL: trie shadow-check never ran (checks=${shadow_checks})" >&2; exit 1; }
shadow_mismatch="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" kardamom_state_trie_shadow_mismatch_total)"
shadow_mismatch="$(printf '%.0f' "${shadow_mismatch:-0}")"
(( shadow_mismatch == 0 )) || { echo "FAIL: incremental state trie diverged from full rebuild (${shadow_mismatch} mismatches)" >&2; exit 1; }
log "validator verdict PASSED: ${verified} blocks BAL-verified, 0 divergences, ${shadow_checks} shadow-checks / 0 mismatches"

log "cluster-e2e PASSED"
