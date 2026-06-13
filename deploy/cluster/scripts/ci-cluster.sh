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

cleanup() {
  [[ "${KEEP:-0}" == "1" ]] && { log "KEEP=1; leaving containers up"; return; }
  log "tearing down containers + network + docker volumes"
  for n in "${NODES[@]}"; do
    docker rm -f "kardamom-${n}" >/dev/null 2>&1 || true
    docker volume rm "kardamom-${n}-docker" "kardamom-${n}-containerd" >/dev/null 2>&1 || true
  done
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
./scripts/smoke.sh

log "cluster-e2e PASSED"
