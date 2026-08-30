#!/usr/bin/env bash
# =============================================================================
# ci-cluster.sh — brings up the kardamom cluster with containers as nodes, for
# CI (.github/workflows/cluster-e2e.yml). The VM-less analogue of `make up`.
# =============================================================================
#
# Warning: this workflow is experimental, and has not yet run green. It
# drives the full Ansible, Nomad, and Consul orchestration on a single
# host, using 11 privileged systemd containers (Docker-in-Docker for
# Nomad's docker driver) on a 192.168.56.0/24 bridge, with the
# contract's static IPs. The unmodified site.yml provisions them. This
# workflow may need iteration on a real runner. The static
# `cluster-validate` workflow is the always-green gate.
#
# Prerequisites (the workflow sets these up): docker and buildx on the
# host; the host docker daemon trusts 192.168.56.10:5000 as an insecure
# registry; ansible with the community.docker collection; the kardamom
# release binaries already built under target/release, one binary per
# service; the nomad CLI on PATH.
#
# Usage:  deploy/cluster/scripts/ci-cluster.sh            # up + provision + deploy + smoke
#         KEEP=1 deploy/cluster/scripts/ci-cluster.sh     # leave containers running
#
# Split layout: this file is the entry point. It holds bring-up
# (sysctls, bridge, node containers, provisioning), the stage gating
# (RUN_LOAD, RUN_SEMANTICS, RUN_CHAOS), and the one EXIT trap. Everything
# else lives in files sourced into this shell. They are libraries, never
# run as child processes; only this file installs an EXIT trap, and
# sourced files never do:
#   lib.sh                 control-node helpers, log/fail
#   lib-topology.sh        node-class model (nodes, IPs, ports)
#   lib-metrics.sh         fetch_metrics, prom_value (scrape/parse)
#   ci-diagnostics.sh      failure diagnostics (multicast probe, alloc dumps)
#   ci-images.sh           image build and registry push (push_image, build_*)
#   ci-stages.sh           container inventory and the env-gated stage bodies
#   validator-verdict.sh   §7c validator verdict and the divergence-log scan,
#                          shared with chaos.sh (one implementation)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${CLUSTER_DIR}/../.." && pwd)"
cd "${CLUSTER_DIR}"

# Shared control-node helpers (on_control, running_alloc, all_allocs,
# ...), plus log and fail. The same ones chaos.sh uses.
# shellcheck source=deploy/cluster/scripts/lib.sh
source "${SCRIPT_DIR}/lib.sh"
# Node-class model (topology_load, plus the executor and validator
# node and port mirrors).
# shellcheck source=deploy/cluster/scripts/lib-topology.sh
source "${SCRIPT_DIR}/lib-topology.sh"
# Prometheus scrape/parse helpers (fetch_metrics, prom_value).
# shellcheck source=deploy/cluster/scripts/lib-metrics.sh
source "${SCRIPT_DIR}/lib-metrics.sh"
# cosign signing and verification for the digest manifest and images:
# sign_pushed_image (used by ci-images.sh push_image) and
# sign_digest_manifest.
# shellcheck source=deploy/cluster/scripts/lib-signing.sh
source "${SCRIPT_DIR}/lib-signing.sh"
# ci-cluster's own sourced libraries; see the split layout note above.
# shellcheck source=deploy/cluster/scripts/ci-diagnostics.sh
source "${SCRIPT_DIR}/ci-diagnostics.sh"
# shellcheck source=deploy/cluster/scripts/ci-images.sh
source "${SCRIPT_DIR}/ci-images.sh"
# shellcheck source=deploy/cluster/scripts/ci-stages.sh
source "${SCRIPT_DIR}/ci-stages.sh"
# shellcheck source=deploy/cluster/scripts/validator-verdict.sh
source "${SCRIPT_DIR}/validator-verdict.sh"

NET=kardamom-net
SUBNET=192.168.56.0/24
REGISTRY=192.168.56.10:5000
TAG=dev
# Per-deploy image digest manifest. push_image
# (ci-images.sh) records a "<svc> <repo>@sha256:..." line per pushed
# image. deploy.sh reads the same default path to pin every job's
# image_ref variable. This file is gitignored; it is an audit record of
# one deploy, not source.
DIGEST_MANIFEST="${DIGEST_MANIFEST:-${CLUSTER_DIR}/images.digests}"
export DIGEST_MANIFEST
NODE_IMAGE=kardamom-node:ci
# kardamom-recorder is removed; durability now happens at the sealer
# archive. The sealer image carries the durability sidecar.
SERVICES=(ingress sequencer executor validator da-watcher batcher)

# Node instances are generated from the class model in
# group_vars/all.yml (node_classes: class -> {count, ip_start}), the
# single source of truth. For each class, this creates `count` instances
# named <class>-<i>, each with a static IP from the class's ip_start
# lane on ip_prefix.0/24. Scaling a class is a one-line `count` change in
# group_vars. There is no node list, IP map, or Ansible inventory to
# maintain by hand here; ci-stages.sh's gen_container_inventory
# generates the inventory. The regex parse, which avoids PyYAML, lives
# in lib-topology.sh's topology_load, shared with smoke-load.sh. It
# populates NODES and NODE_IP/NODE_ROLE/NODE_TIER with `declare -g`, so
# the cleanup and diagnostics traps below can read them.
topology_load \
  || { echo "ERROR: no nodes generated from node_classes" >&2; exit 1; }

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
  release_deploy_lock
}
trap 'on_exit "$?"' EXIT

# --- 0. Advisory deploy lock -------------------------------------------------
# Multiple agent sessions share this host's one cluster, and every
# deploy starts with a purge and wipe. Two concurrent deploys can
# destroy each other: a second deploy's purge can hit the first
# deploy's mid-provision containers. This lock guards deploy against
# deploy only. It is held for the duration of this script, and released
# on exit. A cluster left running with KEEP=1 can be redeployed onto
# later; deploying over an idle cluster is how every session starts. A
# lock whose pid is dead is stale, and gets reaped. To override, at the
# cost of destroying the other deploy: KARDAMOM_CLUSTER_FORCE=1.
DEPLOY_LOCK=/tmp/kardamom-cluster-deploy.lock
release_deploy_lock() {
  # Only the holder removes the lock. Re-check the pid, since a forced
  # second deploy may have replaced the file with its own pid.
  if [[ -f "${DEPLOY_LOCK}" ]]; then
    local lpid
    lpid="$(awk '{print $1; exit}' "${DEPLOY_LOCK}" 2>/dev/null)"
    [[ "${lpid}" == "$$" ]] && rm -f "${DEPLOY_LOCK}"
  fi
}
if [[ -f "${DEPLOY_LOCK}" && "${KARDAMOM_CLUSTER_FORCE:-0}" != "1" ]]; then
  read -r LOCK_PID LOCK_WHO <"${DEPLOY_LOCK}" || true
  if [[ -n "${LOCK_PID:-}" ]] && kill -0 "${LOCK_PID}" 2>/dev/null; then
    echo "ERROR: another cluster deploy is IN FLIGHT (pid ${LOCK_PID}, ${LOCK_WHO:-unknown}, ${DEPLOY_LOCK})." >&2
    echo "       Deploying now would purge its half-built cluster. Wait for it, or" >&2
    echo "       KARDAMOM_CLUSTER_FORCE=1 to override (destroys their deploy)." >&2
    exit 1
  fi
  echo "==> stale deploy lock (pid ${LOCK_PID:-?} dead); reaping"
fi
echo "$$ $(whoami)@$(hostname):${KARDAMOM_SESSION:-unlabeled} $(date -u +%FT%TZ)" >"${DEPLOY_LOCK}"
log "deploy lock acquired (${DEPLOY_LOCK}, pid $$)"

# --- 1. Host sysctls (not namespaced; cannot be set from inside a container) --
log "applying Aeron socket-buffer sysctls on the host"
sudo sysctl -w net.core.rmem_max=16777216 net.core.wmem_max=16777216 \
                net.core.rmem_default=16777216 net.core.wmem_default=16777216

# Let bridged frames bypass iptables. With br_netfilter loaded and
# bridge-nf-call-iptables=1 (docker's default), L2-bridged IP frames
# pass through the iptables FORWARD chain. Docker sets that chain's
# policy to DROP, with ACCEPT rules only for traffic it recognizes.
# Flooded UDP multicast (Aeron's cross-node tx_ordering, plus
# fsync/quorum watermarks) does not match those rules, so it gets
# dropped in FORWARD, even with IGMP snooping off. Unicast TCP
# (Consul/Nomad/ingress RPC) is unaffected, since docker does allow it.
# This is exactly why the recorders log "recording initiated" but never
# "recording ready". Setting bridge-nf-call-iptables=0 makes bridged
# frames pure L2, so the bridge floods multicast between node containers
# without being blocked. This is safe here: one isolated test network,
# with no inter-network isolation to preserve.
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
# Name the underlying Linux bridge, so its /sys path is deterministic.
# Otherwise, docker derives br-<network-id[:12]>. The next step disables
# IGMP snooping on it.
BRIDGE_NAME=kardamom-br0
# This step is idempotent: reuse an existing ${NET}, for example from a
# KEEP=1 run or local iteration. Recreating it would also recreate the
# underlying bridge, and silently reset multicast_snooping to the kernel
# default (1). That would undo the fix below on hosts where this script
# cannot sudo.
if ! docker network inspect "${NET}" >/dev/null 2>&1; then
  docker network create --driver bridge \
    -o "com.docker.network.bridge.name=${BRIDGE_NAME}" \
    --subnet "${SUBNET}" "${NET}"
else
  log "network ${NET} already exists; reusing"
fi

# Aeron's cross-node data plane is UDP multicast: tx_ordering (sealer to
# recorders and executor), the per-recorder fsync watermarks, and the
# aggregated quorum watermark. This bridge is an isolated L2 segment
# with no IGMP querier. So with the kernel default
# (multicast_snooping=1), the bridge stops flooding group traffic once
# its snooping state lapses, and multicast never crosses between node
# containers. The symptom is precise: each recorder logs "recording
# initiated" for TxOrdering, but never "recording ready" — the archive's
# recording subscription gets no image. So no fsync watermark
# publishes, the quorum watermark never advances, and ingress
# (--ack-policy on-quorum) times out with "timed out waiting for
# receipt or watermark". Disabling snooping makes the bridge flood
# multicast to every port, which is correct and reliable for a 5-node
# test segment. This step is best-effort: it warns but continues if
# /sys is not writable.
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
  # Use dedicated volumes for the inner Docker's storage. Without them,
  # the in-node Docker (Docker-in-Docker, for Nomad's docker driver and
  # the registry) would put its overlay layers on the node container's
  # own overlay rootfs. The kernel rejects overlay-on-overlay ("mount
  # overlay ... invalid argument"). Both volumes are needed:
  # /var/lib/docker for engine state, and /var/lib/containerd for the
  # containerd image-store snapshotter, where modern Docker keeps image
  # layers. The volumes live on the host's real filesystem, where
  # overlay works.
  docker volume create "kardamom-${n}-docker" >/dev/null
  docker volume create "kardamom-${n}-containerd" >/dev/null
  # This step is idempotent: reuse a node left up by a previous KEEP=1
  # run, as-is.
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

# Wait for systemd to be ready in each node. systemctl is-system-running
# returns running or degraded once systemd is up.
for n in "${NODES[@]}"; do
  log "waiting for systemd in ${n}"
  for _ in $(seq 1 30); do
    state="$(docker exec "kardamom-${n}" systemctl is-system-running 2>/dev/null || true)"
    [[ "${state}" == "running" || "${state}" == "degraded" || "${state}" == "starting" ]] && break
    sleep 1
  done
done

# --- 4. Provision with the unmodified site.yml over the docker connection ----
gen_container_inventory
log "ansible-playbook site.yml (generated container inventory: ${CONTAINER_INVENTORY})"
ANSIBLE_HOST_KEY_CHECKING=False \
  ansible-playbook -i "${CONTAINER_INVENTORY}" ansible/site.yml

# --- 5. Build thin service images from prebuilt binaries, push to r1 ---------
build_service_images
# --- 5b. Cluster image (Java Aeron-Cluster node) -----------------------------
build_cluster_image
# --- 5c. Sign the completed digest manifest -------------------------------
# Every image above was signed by digest at push time (push_image). The
# manifest blob is signed once, here, after the last push. This is the
# document deploy-time verification and the private repo's image-drift
# sweep anchor on. Signing is keyless, to the public Rekor log, in CI
# only. Outside CI, this logs a clear skip line.
sign_digest_manifest "${DIGEST_MANIFEST}"

# --- 6. Deploy the Nomad jobs + smoke ---------------------------------------
export NOMAD_ADDR="http://192.168.56.10:4646"
# Stamp the runner identity for every shard. Chaos-case load verdicts
# can show the same intermittent slowdowns as the load shard, so this
# helps correlate them.
log "runner: name=${RUNNER_NAME:-local} host=$(hostname) cpus=$(nproc) loadavg=[$(cut -d' ' -f1-3 /proc/loadavg)] mem_avail_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo)"

log "deploy.sh (Nomad endpoint ${NOMAD_ADDR})"
./scripts/deploy.sh

log "smoke test (gate: single-tx must pass before load smoke runs)"
./scripts/smoke.sh

# --- 7. Sustained-load invariant gate (Rust harness: fixed-rate soak,
# must-deliver, drop accounting, keep-pace), then 8. chaos and
# resilience suite. Both are env-gated stages, with bodies in
# ci-stages.sh. Both stages drive the Rust kardamom-load harness, built
# alongside the service binaries by the workflow, staged at
# target/release. Durations and rates are tunable through env
# variables, so PR runs stay short, and a full soak can be dialed up
# through the cluster-e2e.yml workflow_dispatch or matrix shard
# settings.
#
# Funded-account budget: genesis prefunds Anvil accounts #0 through
# #17, each with its own contiguous nonce chain on the never-reset
# chain. A fresh account's first tx must be nonce 0, with no gaps.
# Allocation: #0 is the gate smoke above; #1 through #6 are the
# sustained-load harness; #7 through #15 are one fresh account per
# chaos case (see chaos.sh); #16 is the ingress-churn failover re-smoke
# (step 7b); #17 is the fallback executor-churn re-smoke. Every check
# owns its own account, so every smoke tx is nonce 0. There is no NONCE
# setting and no nonce continuation.
# RUN_LOAD and RUN_CHAOS (default 1) let a CI shard run just one stage,
# so the full suite can split across runners; each shard brings up its
# own cluster. When both are unset, they default to 1, and the local or
# single-runner path runs smoke plus the default cluster chaos cases.
# CHAOS_CASES selects which chaos cases to run. In cluster mode, the
# default set is the three Raft cases (see chaos.sh).
LOAD_BIN="${ROOT}/target/release/kardamom-load"
if [[ -x "${LOAD_BIN}" ]]; then
  if [[ "${RUN_LOAD:-1}" == "1" ]]; then
    stage_load
  else
    log "RUN_LOAD=0 — skipping sustained-load stage (chaos-only shard)"
  fi

  # The chain-semantics suite (Target C) is off by default
  # (RUN_SEMANTICS=0), so it runs only on its own shard. See
  # stage_semantics in ci-stages.sh.
  if [[ "${RUN_SEMANTICS:-0}" == "1" ]]; then
    stage_semantics
  fi

  if [[ "${RUN_CHAOS:-1}" == "1" ]]; then
    stage_chaos
  else
    log "RUN_CHAOS=0 — skipping chaos stage (load-only shard)"
  fi
else
  stage_fallback_load
fi

# --- 7b. Ingress active/active failover, and the multicast-receipts freeze guard
# See stage_ingress_churn in ci-stages.sh for the failover and 2a
# freeze-guard reasoning.
stage_ingress_churn

# --- 7c. Validator sync and keep-up verdict -------------------------------------
# The validator followed everything the shard just did: bring-up,
# smoke, and load or chaos. The full verdict (liveness, sync and
# keep-up, BAL verification with zero divergences, trie shadow-checks)
# lives in validator-verdict.sh, which also holds the divergence-log
# scan shared with the chaos suite.
log "validator verdict: sync + keep-up + BAL cross-check (no divergence)"
run_validator_verdict

log "cluster-e2e PASSED"
