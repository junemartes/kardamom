#!/usr/bin/env bash
# =============================================================================
# ci-cluster.sh — bring up the kardamom cluster with CONTAINERS as nodes, for
# CI (.github/workflows/cluster-e2e.yml). The VM-less analogue of `make up`.
# =============================================================================
#
# ⚠ EXPERIMENTAL / NOT YET RUN GREEN. This drives the full Ansible → Nomad →
# Consul orchestration on a single host using 5 privileged systemd containers
# (Docker-in-Docker for Nomad's docker driver) on a 192.168.56.0/24 bridge with
# the contract's static IPs — so the UNMODIFIED site.yml provisions them. It is
# expected to need iteration on a real runner; the static `cluster-validate`
# workflow is the always-green gate.
#
# Prereqs (the workflow sets these up): docker + buildx on the host; the host
# docker daemon trusts 192.168.56.11:5000 as an insecure registry; ansible with
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

NET=kardamom-net
SUBNET=192.168.56.0/24
REGISTRY=192.168.56.11:5000
TAG=dev
NODE_IMAGE=kardamom-node:ci
SERVICES=(ingress sequencer executor sealer da-watcher batcher recorder)

# node name -> static IP (mirrors ansible/group_vars/all.yml cluster_nodes).
NODES=(r1 r2 r3 w1 w2)
declare -A NODE_IP=( [r1]=192.168.56.11 [r2]=192.168.56.12 [r3]=192.168.56.13 [w1]=192.168.56.21 [w2]=192.168.56.22 )

log() { echo "==> $*"; }

# On failure, dump every job's status + each allocation's stdout/stderr BEFORE
# teardown — otherwise the container removal below erases the only evidence of
# why an alloc failed (the workflow's post-step runs after this script's EXIT
# trap, too late). Best-effort; never let diagnostics mask the real exit code.
# Raw-UDP multicast reachability check from w2 (192.168.56.22) to r1
# (192.168.56.11) on a throwaway group, bypassing Aeron entirely. Prints how
# many of 30 sent packets r1 received: 0 means the bridge drops cross-node
# multicast (kernel/bridge issue); >0 means forwarding works and any remaining
# failure is Aeron-specific. python3 ships in the node image.
multicast_probe() {
  local grp=239.192.99.99 port=45999 rip=192.168.56.11 sip=192.168.56.22
  echo "===== multicast probe ${sip} -> ${rip} (grp ${grp}:${port}) ====="
  docker exec -d kardamom-r1 python3 -c "
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
  docker exec kardamom-w2 python3 -c "
import socket
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
s.setsockopt(socket.IPPROTO_IP,socket.IP_MULTICAST_IF,socket.inet_aton('${sip}'))
s.setsockopt(socket.IPPROTO_IP,socket.IP_MULTICAST_TTL,1)
for _ in range(30):
    s.sendto(b'probe',('${grp}',${port}))
print('probe: w2 sent 30 packets')
" 2>&1 || true
  sleep 5
  local got
  got="$(docker exec kardamom-r1 cat /tmp/mcast_probe 2>/dev/null || echo '?')"
  echo "probe: r1 received ${got}/30 packets (0 => bridge not forwarding multicast)"
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
  for n in r1 w1; do
    echo "----- ${n}: multicast groups (ip maddr) -----"
    docker exec "kardamom-${n}" ip maddr show dev eth0 2>/dev/null || true
  done
  # Direct raw-UDP multicast probe w2 -> r1 on a throwaway group, INDEPENDENT of
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
  echo "===== tcpdump ${BRIDGE_NAME:-kardamom-br0}: cluster multicast (≤6s) ====="
  sudo timeout 6 tcpdump -i "${BRIDGE_NAME:-kardamom-br0}" -nn -t -c 80 \
    'udp and dst net 239.192.56.0/24' 2>/dev/null \
    | awk '{print $1, $2, $3}' | sort | uniq -c | sort -rn | head -30 \
    || echo "(tcpdump unavailable or no multicast captured)"
  export NOMAD_ADDR="http://192.168.56.11:4646"
  nomad job status 2>/dev/null || true
  for job in aeron recorder quorum anvil sealer sequencer executor ingress batcher; do
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
log "ansible-playbook site.yml (container inventory)"
ANSIBLE_HOST_KEY_CHECKING=False \
  ansible-playbook -i ansible/inventory.containers.ini ansible/site.yml

# --- 5. Build thin service images from prebuilt binaries, push to r1 ---------
# The workflow ran `cargo build --release` for each BIN; wrap each into a thin
# image and push to the in-cluster registry (reachable on the bridge IP).
log "building + pushing service images to ${REGISTRY}"
# Aeron image (same canonical Dockerfile as make images).
docker build -f "${ROOT}/crates/log/docker/aeron/Dockerfile" \
  -t "${REGISTRY}/kardamom-aeron:${TAG}" "${ROOT}/crates/log/docker/aeron"
docker push "${REGISTRY}/kardamom-aeron:${TAG}"

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
  docker push "${REGISTRY}/${bin}:${TAG}"
done

# --- 6. Deploy the Nomad jobs + smoke ---------------------------------------
export NOMAD_ADDR="http://192.168.56.11:4646"
log "deploy.sh (Nomad endpoint ${NOMAD_ADDR})"
./scripts/deploy.sh

log "smoke test"
./scripts/smoke.sh

# --- 7. Redundancy: kill one recorder alloc, re-smoke (2-of-3 quorum) -------
log "redundancy: stopping one recorder alloc and re-running smoke"
# Stop the recorder system job on r3 by deregistering then re-checking quorum.
docker exec kardamom-r3 bash -lc 'export NOMAD_ADDR=http://192.168.56.13:4646; \
  alloc=$(nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}{{.ID}}{{end}}{{end}}" recorder 2>/dev/null | head -c 36); \
  [ -n "$alloc" ] && nomad alloc stop "$alloc" || true' || true
sleep 5
# Second transfer from the same signer: account #0's nonce 0 was consumed by
# the first smoke, so this one MUST use nonce 1 (the ingress can't fill it; see
# smoke.sh).
NONCE=1 ./scripts/smoke.sh

log "cluster-e2e PASSED"
