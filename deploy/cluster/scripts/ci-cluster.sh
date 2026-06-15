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
# NOTE: kardamom-recorder was removed (durability is now archive-at-the-sealer);
# the sealer image carries the durability sidecar.
SERVICES=(ingress sequencer executor sealer da-watcher batcher)

# node name -> static IP (mirrors ansible/group_vars/all.yml cluster_nodes).
NODES=(r1 r2 r3 w1 w2)
declare -A NODE_IP=( [r1]=192.168.56.11 [r2]=192.168.56.12 [r3]=192.168.56.13 [w1]=192.168.56.21 [w2]=192.168.56.22 )

log() { echo "==> $*"; }

cleanup() {
  [[ "${KEEP:-0}" == "1" ]] && { log "KEEP=1; leaving containers up"; return; }
  log "tearing down containers + network"
  for n in "${NODES[@]}"; do docker rm -f "kardamom-${n}" >/dev/null 2>&1 || true; done
  docker network rm "${NET}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- 1. Host sysctls (NOT namespaced; can't be set from inside a container) --
log "applying Aeron socket-buffer sysctls on the host"
sudo sysctl -w net.core.rmem_max=16777216 net.core.wmem_max=16777216 \
                net.core.rmem_default=16777216 net.core.wmem_default=16777216

# --- 2. Bridge network with the contract subnet -----------------------------
docker network rm "${NET}" >/dev/null 2>&1 || true
log "creating bridge ${NET} (${SUBNET})"
docker network create --driver bridge --subnet "${SUBNET}" "${NET}"

# --- 3. Node image + 5 systemd containers -----------------------------------
log "building node image"
docker build -f docker/node.Dockerfile -t "${NODE_IMAGE}" docker/
for n in "${NODES[@]}"; do
  log "starting node ${n} (${NODE_IP[$n]})"
  docker run -d --name "kardamom-${n}" --hostname "${n}" \
    --privileged --cgroupns=host \
    -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
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

# --- 7. Subscriber-churn resilience: kill an executor alloc, re-smoke --------
# The Q-of-N recorder-redundancy test is GONE: durability is now a single
# archive at the sealer (no quorum to tolerate a recorder loss). The property
# this change is actually about is that a tx_ordering *subscriber* dropping no
# longer freezes the other subscribers' images (the MDC fix). Kill one executor
# alloc on w1 and re-smoke: ingress + the sealer durability sidecar must keep
# advancing (under the old shared-multicast group, a dropped subscriber froze
# every tx_ordering image; under MDC it does not).
log "subscriber-churn: stopping one executor alloc and re-running smoke"
docker exec kardamom-w1 bash -lc 'export NOMAD_ADDR=http://192.168.56.21:4646; \
  alloc=$(nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}{{.ID}}{{end}}{{end}}" executor 2>/dev/null | head -c 36); \
  [ -n "$alloc" ] && nomad alloc stop "$alloc" || true' || true
sleep 5
./scripts/smoke.sh

log "cluster-e2e PASSED"
