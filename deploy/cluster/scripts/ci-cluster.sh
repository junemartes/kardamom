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

# The cluster-e2e client binary (built --release by the workflow) drives the
# pipeline over ingress JSON-RPC + the anvil L1. LOCKBOX_FILE is where deploy.sh
# writes the resolved ETHLockbox address; export it so the deploy.sh invoked
# below writes to the same path this script reads back for the client.
E2E_BIN="${ROOT}/target/release/cluster-e2e"
export LOCKBOX_FILE="${LOCKBOX_FILE:-/tmp/kardamom-lockbox}"

# cluster-e2e is a pure RPC client, but kardamom-log sits in its dependency tree;
# point the dynamic loader at the Aeron .so's that Phase 5 stages under
# target/release, as a safeguard against a libaeron NEEDED entry on Linux. The
# dir is populated before the gate (Phase 6) runs; absent => harmless.
run_e2e() { LD_LIBRARY_PATH="${ROOT}/target/release/_aeronlibs:${LD_LIBRARY_PATH:-}" "${E2E_BIN}" "$@"; }

NET=kardamom-net
SUBNET=192.168.56.0/24
REGISTRY=192.168.56.10:5000
TAG=dev
NODE_IMAGE=kardamom-node:ci
# NOTE: kardamom-recorder was removed (durability is now archive-at-the-sealer);
# the sealer image carries the durability sidecar.
SERVICES=(ingress sequencer executor sealer da-watcher batcher)

# Node instances are GENERATED from the class model in group_vars/all.yml
# (node_classes: class -> {count, ip_start}) — the single source of truth. For
# each class we materialise `count` instances named <class>-<i>, each with a
# static IP from the class's ip_start lane on ip_prefix.0/24. Scaling a class is
# a one-line `count` change in group_vars; there is no node list / IP map / Ansible
# inventory to hand-maintain here (the inventory is generated below too). Parsed
# with a plain regex (no PyYAML), so it runs anywhere python3 does.
NODES=(); declare -A NODE_IP=(); declare -A NODE_ROLE=(); declare -A NODE_TIER=()
while read -r _name _ip _role _tier; do
  [[ -z "${_name}" ]] && continue
  NODES+=("${_name}"); NODE_IP[${_name}]="${_ip}"
  NODE_ROLE[${_name}]="${_role}"; NODE_TIER[${_name}]="${_tier}"
done < <(python3 - <<'PY'
# Parse node_classes with a plain regex (no PyYAML dependency — same approach as
# scripts/check-contract.py, so this runs anywhere python3 does). Each class line:
#   <name>: { count: N, ip_start: M, tier: T }
import re
text = open('ansible/group_vars/all.yml').read()
pref = re.search(r'^ip_prefix:\s*"([\d.]+)"', text, re.M).group(1)
for m in re.finditer(
        r'^\s{2}(\w+):\s*\{\s*count:\s*(\d+),\s*ip_start:\s*(\d+),\s*tier:\s*(\w+)',
        text, re.M):
    cls, count, ip_start, tier = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
    for i in range(count):
        print(f"{cls}-{i} {pref}.{ip_start + i} {cls} {tier}")
PY
)
[[ "${#NODES[@]}" -gt 0 ]] || { echo "ERROR: no nodes generated from node_classes" >&2; exit 1; }

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
      echo "${n} ansible_host=kardamom-${n} kardamom_node=${n} node_ip=${NODE_IP[$n]} role=${r} tier=${NODE_TIER[$n]}${extra}" >>"${CONTAINER_INVENTORY}"
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

log() { echo "==> $*"; }

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
  for job in aeron anvil sealer sequencer executor ingress batcher da-watcher; do
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
      echo "----- ${job} alloc ${alloc}: stdout (tail 40) -----"
      nomad alloc logs "${alloc}" 2>/dev/null | tail -40 || true
    done <<<"${allocs}"
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
docker network create --driver bridge \
  -o "com.docker.network.bridge.name=${BRIDGE_NAME}" \
  --subnet "${SUBNET}" "${NET}"

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

# --- 6. Deploy the Nomad jobs + smoke ---------------------------------------
export NOMAD_ADDR="http://192.168.56.10:4646"
log "deploy.sh (Nomad endpoint ${NOMAD_ADDR})"
./scripts/deploy.sh

# Cluster e2e client (gate): the richer replacement for the single-tx smoke.
# Drives the deployed cluster over ingress JSON-RPC + the anvil L1 and asserts a
# signed transfer round-trip, an L1→L2 deposit (anvil depositETH → da-watcher →
# executor → receipt-by-source_hash), and a contract deploy — all over plain
# JSON-RPC. deploy.sh wrote the deployed Lockbox address to LOCKBOX_FILE; if it
# is absent (deploy skipped), fall back to the non-deposit scenarios so the gate
# still exercises transfers + contract deploy. Account #0 L2 nonces 0-3.
log "cluster-e2e gate (transfer + deposit + contract-deploy)"
[[ -x "${E2E_BIN}" ]] || { echo "ERROR: cluster-e2e binary not found at ${E2E_BIN}" >&2; exit 1; }
LOCKBOX_ADDRESS="$(cat "${LOCKBOX_FILE}" 2>/dev/null || true)"
if [[ -n "${LOCKBOX_ADDRESS}" ]]; then
  log "  lockbox ${LOCKBOX_ADDRESS}: running all scenarios"
  run_e2e all --lockbox "${LOCKBOX_ADDRESS}"
else
  log "  no lockbox address; deposit scenario skipped (transfer + contract-deploy only)"
  run_e2e transfer --count 3 --start-nonce 0
  run_e2e contract-deploy --nonce 3
fi

# --- 7. High sustained-load (Rust harness: ramp -> soak; must-deliver + drop
# accounting + keep-pace) then 8. chaos/resilience suite. Both use kardamom-load
# (built alongside the service bins, staged at target/release). Durations/rates
# are ENV-tunable so PR runs stay short and a full soak can be dialed up via the
# cluster-e2e.yml workflow_dispatch inputs.
#
# Funded-account budget: genesis prefunds Anvil accounts #0..#15, each its own
# contiguous nonce chain on the never-reset chain (a fresh account's first tx
# MUST be nonce 0, with no gaps). Allocation: #0 = cluster-e2e gate above (nonces
# 0-3: transfer x3 + contract deploy); #1..#6 = the sustained-load harness;
# #7..#15 = one fresh account per chaos case (see chaos.sh). This disjoint split
# is why no stage needs nonce-continuation. RUN_LOAD / RUN_CHAOS (default 1) let
# a CI shard run just one stage so the full soak can be split across runners
# (each shard brings up its own cluster). The always-on per-PR run leaves both at 1.
LOAD_BIN="${ROOT}/target/release/kardamom-load"
if [[ -x "${LOAD_BIN}" ]]; then
  if [[ "${RUN_LOAD:-1}" == "1" ]]; then
    log "load test: kardamom-load ramp+soak (duration=${LOAD_DURATION_S:-60}s target=${LOAD_TARGET_TPS:-200}tps)"
    # --chain-id is passed explicitly (412346, from group_vars/all.yml) rather
    # than probed via eth_chainId: ingress.toml sets no chain_id, so its
    # eth_chainId returns a default that does NOT match the executors' chain, and
    # txs signed with it would never execute. smoke.sh hardcodes 412346 likewise.
    "${LOAD_BIN}" --rpc http://192.168.56.31:8545 --chain-id 412346 \
      --duration "${LOAD_DURATION_S:-60}s" --target-tps "${LOAD_TARGET_TPS:-200}" \
      --senders "${LOAD_SENDERS:-6}" --sender-offset 1 --assert-all-delivered \
      --completeness accepted --max-gap "${LOAD_MAX_GAP:-5}" \
      --scrape executor,sealer,ingress,sequencer --output /tmp/kardamom-load.json
  else
    log "RUN_LOAD=0 — skipping sustained-load stage (chaos-only shard)"
  fi

  if [[ "${RUN_CHAOS:-1}" == "1" ]]; then
    log "chaos suite (kills components under steady load; asserts auto-recovery)"
    CHAOS_TPS="${CHAOS_TPS:-50}" CHAOS_CASE_S="${CHAOS_CASE_S:-45}" \
      CHAOS_CASES="${CHAOS_CASES:-graceful-executor hard-executor sealer-graceful}" \
      CHAOS_RESTART_SLO_S="${CHAOS_RESTART_SLO_S:-60}" \
      CHAOS_RESCHEDULE_SLO_S="${CHAOS_RESCHEDULE_SLO_S:-150}" \
      LOAD_BIN="${LOAD_BIN}" LOAD_MAX_GAP="${LOAD_MAX_GAP:-5}" \
      ./scripts/chaos.sh
  else
    log "RUN_CHAOS=0 — skipping chaos stage (load-only shard)"
  fi
else
  # Fallback (kardamom-load not staged): the legacy bash load smoke (accounts
  # #1..#4) + a single subscriber-churn check via the cluster-e2e client.
  log "WARN: ${LOAD_BIN} not found — running legacy load smoke + subscriber-churn"
  SMOKE_SENDER_OFFSET=1 ./scripts/smoke-load.sh
  log "subscriber-churn: stopping one executor alloc and re-running the e2e transfer"
  docker exec kardamom-control-0 bash -lc 'export NOMAD_ADDR=http://192.168.56.10:4646; \
    alloc=$(nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}{{.ID}}{{end}}{{end}}" executor 2>/dev/null | head -c 36); \
    [ -n "$alloc" ] && nomad alloc stop "$alloc" || true' || true
  sleep 5
  # Gate ran account #0 nonces 0-3; load smoke used offset 1 (never #0); so #0
  # nonce 4 is free for this churn re-check (ingress can't fill the nonce).
  run_e2e transfer --count 1 --start-nonce 4
fi

log "cluster-e2e PASSED"
