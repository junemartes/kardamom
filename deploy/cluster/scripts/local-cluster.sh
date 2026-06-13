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
# NOTE: smoke.sh prefers foundry `cast`; the orchestrator does not ship it, so
# the smoke step falls back to its curl path. Run with KEEP=1 and inspect the
# cluster (or `docker exec kardamom-orch ...`) for the multi-host Aeron work.
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
        -p kardamom-recorder --bins
      # Stage exactly where ci-cluster.sh looks: the binaries in target/release,
      # and libaeron*.so under a build/*/out/build/lib path it greps for.
      mkdir -p /work/target/release/build/aeronstage/out/build/lib
      for b in ingress sequencer executor sealer da-watcher batcher recorder; do
        cp -f "/ltarget/release/kardamom-$b" /work/target/release/
      done
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
  if docker info 2>/dev/null | grep -q "${REGISTRY_CIDR}"; then
    log "insecure registry ${REGISTRY_CIDR} already trusted by the docker daemon"
    return 0
  fi
  cat >&2 <<EOF
==> The in-cluster registry (192.168.56.11:5000) is plain HTTP and is NOT in the
    docker daemon's insecure-registries. ci-cluster.sh's image push will fail.
    Add it and restart docker, e.g. on Docker Desktop:

      jq '.["insecure-registries"]=((.["insecure-registries"]//[])+["${REGISTRY_CIDR}"]|unique)' \\
          ~/.docker/daemon.json > /tmp/d.json && mv /tmp/d.json ~/.docker/daemon.json
      docker desktop restart && sleep 30

    Then re-run. (Set SKIP_REGISTRY_CHECK=1 to proceed anyway.)
EOF
  [[ "${SKIP_REGISTRY_CHECK:-0}" == "1" ]] && return 0
  exit 1
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
  docker exec -e KEEP="${KEEP:-0}" kardamom-orch \
    bash -lc 'cd /work && deploy/cluster/scripts/ci-cluster.sh'
}

case "${1:-all}" in
  build) build_binaries; build_orchestrator ;;
  up)    run_cluster ;;
  all)   build_binaries; build_orchestrator; run_cluster ;;
  *) echo "usage: $0 [build|up|all]   (KEEP=1 to leave the cluster up)" >&2; exit 2 ;;
esac
