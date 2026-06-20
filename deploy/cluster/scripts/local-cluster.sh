#!/usr/bin/env bash
# =============================================================================
# local-cluster.sh — run the container cluster-e2e LOCALLY on Docker Desktop.
# =============================================================================
#
# Dev-machine analogue of .github/workflows/cluster-e2e.yml. The workflow runs
# `cargo build --release` + ci-cluster.sh directly on a GitHub ubuntu-24.04
# runner; this script reproduces that on a laptop where the only Linux "host" is
# the Docker Desktop VM:
#
#   1. Builds the service binaries in a REPRODUCIBLE builder image
#      (docker/local-build.Dockerfile) that captures the build deps the runner
#      image provides implicitly (cmake>=3.30, libclang, rustfmt, a JDK), and
#      stages them under target/release exactly where ci-cluster.sh expects.
#   2. Builds a runner-surrogate "orchestrator" (docker/orchestrator.Dockerfile)
#      with the docker CLI + ansible + nomad.
#   3. Ensures the Docker daemon trusts the in-cluster registry (plain HTTP).
#   4. Runs the UNMODIFIED ci-cluster.sh from the orchestrator (sharing the VM's
#      network namespace + docker socket, so node containers are siblings on the
#      VM daemon — the topology ci-cluster.sh assumes on a runner).
#
# Usage:
#   deploy/cluster/scripts/local-cluster.sh            # build + bring up
#   KEEP=1 deploy/cluster/scripts/local-cluster.sh     # leave the cluster up to inspect
#   deploy/cluster/scripts/local-cluster.sh build      # only build binaries + images
#   deploy/cluster/scripts/local-cluster.sh up         # only run ci-cluster.sh
#
# NOTE: the orchestrator ships foundry `cast`, so smoke.sh uses its preferred
# signed-tx Path A (like CI). Run with KEEP=1 and inspect the cluster (or
# `docker exec kardamom-orch ...`) for the multi-host Aeron work.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${CLUSTER_DIR}/../.." && pwd)"
DOCKER_DIR="${CLUSTER_DIR}/docker"
REGISTRY_CIDR="192.168.56.0/24"

log() { echo "==> $*"; }

build_binaries() {
  log "building reproducible builder image (docker/local-build.Dockerfile)"
  docker build -f "${DOCKER_DIR}/local-build.Dockerfile" -t kardamom-local-build:latest "${DOCKER_DIR}"
  docker volume create kardamom-ltarget >/dev/null
  docker volume create kardamom-cargo-registry >/dev/null
  log "building service binaries in the builder (host arch)"
  # Mount the registry cache at a SUB-path so it doesn't shadow the image's
  # toolchain at /root/.cargo/bin; persist build outputs via CARGO_TARGET_DIR.
  docker run --rm \
    -v "${ROOT}:/work" \
    -v kardamom-cargo-registry:/root/.cargo/registry \
    -v kardamom-ltarget:/ltarget \
    -e CARGO_TARGET_DIR=/ltarget -e CARGO_NET_RETRY=10 -e CARGO_HTTP_MULTIPLEXING=false \
    -w /work kardamom-local-build:latest bash -c '
      set -e
      cargo build --release \
        -p kardamom-ingress -p kardamom-sequencer -p kardamom-executor \
        -p kardamom-sealer -p kardamom-da-watcher -p kardamom-batcher \
        --bins
      # The sustained-load + chaos harness ci-cluster.sh runs from target/release.
      cargo build --release -p kardamom-bench --bin kardamom-load
      # Stage exactly where ci-cluster.sh looks: the binaries in target/release,
      # and libaeron*.so under a build/*/out/build/lib path it greps for.
      # (kardamom-recorder removed — durability is archive-at-the-sealer.)
      mkdir -p /work/target/release/build/aeronstage/out/build/lib
      for b in ingress sequencer executor sealer da-watcher batcher; do
        cp -f "/ltarget/release/kardamom-$b" /work/target/release/
      done
      cp -f /ltarget/release/kardamom-load /work/target/release/
      find /ltarget/release/build -path "*/out/build/lib/libaeron*.so" \
        -exec cp -f {} /work/target/release/build/aeronstage/out/build/lib/ \;
      echo "staged: $(ls /work/target/release/build/aeronstage/out/build/lib/ | tr "\n" " ")"
    '
  log "service binaries staged under target/release"
}

build_orchestrator() {
  log "building orchestrator image (docker/orchestrator.Dockerfile)"
  docker build -f "${DOCKER_DIR}/orchestrator.Dockerfile" -t kardamom-orchestrator:latest "${DOCKER_DIR}"
}

ensure_registry_trusted() {
  # Informational only — never fatal. ci-cluster.sh pushes via REGISTRY_PUSH_NODE
  # (engine-to-engine: `docker save | docker exec control-0 docker load`, then the
  # push runs INSIDE control-0 against its own registry). The host/VM docker daemon
  # never sends registry traffic, so its insecure-registry trust is irrelevant here
  # — only the in-node dockers need it, and Ansible configures those. (The old hard
  # gate was also racy: a transient `docker info` hiccup during a Docker Desktop
  # restart aborted the whole run before it began.)
  if docker info 2>/dev/null | grep -q "${REGISTRY_CIDR}"; then
    log "host daemon trusts insecure registry ${REGISTRY_CIDR} (not required for the in-node push)"
  else
    log "note: host daemon doesn't list ${REGISTRY_CIDR} as insecure — fine; the in-node push doesn't route registry traffic through it"
  fi
}

run_cluster() {
  ensure_registry_trusted
  docker rm -f kardamom-orch >/dev/null 2>&1 || true
  log "starting orchestrator (--privileged --network=host --pid=host + docker.sock)"
  docker run -d --name kardamom-orch \
    --privileged --network=host --pid=host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "${ROOT}:/work" \
    kardamom-orchestrator:latest >/dev/null
  log "running ci-cluster.sh inside orchestrator (KEEP=${KEEP:-0})"
  # REGISTRY_PUSH_NODE makes ci-cluster.sh's push_image() side-step Docker
  # Desktop's HTTP proxy (which can't reach the VM-internal registry IP and would
  # otherwise hang the push): the image is loaded engine-to-engine into cp1's inner
  # docker and pushed from there, co-located with the registry. No-op on CI.
  # Forward the load/chaos tuning knobs (each `-e VAR` passes the current value
  # if set, else is a no-op) so a dev can dial the local run down/up.
  docker exec -e KEEP="${KEEP:-0}" -e REGISTRY_PUSH_NODE=control-0 \
    -e LOAD_DURATION_S -e LOAD_TARGET_TPS -e LOAD_SENDERS -e LOAD_MAX_GAP \
    -e CHAOS_TPS -e CHAOS_CASE_S -e CHAOS_CASES \
    -e CHAOS_RESTART_SLO_S -e CHAOS_RESCHEDULE_SLO_S \
    kardamom-orch \
    bash -lc 'cd /work && deploy/cluster/scripts/ci-cluster.sh'
}

case "${1:-all}" in
  build) build_binaries; build_orchestrator ;;
  up)    run_cluster ;;
  all)   build_binaries; build_orchestrator; run_cluster ;;
  *) echo "usage: $0 [build|up|all]   (KEEP=1 to leave the cluster up)" >&2; exit 2 ;;
esac
