# shellcheck shell=bash
# =============================================================================
# chaos-probes.sh — read-only probes for the chaos suite.
# =============================================================================
# SOURCED into chaos.sh's shell (never executed as a child): the probes read
# the topology constants from lib-topology.sh and scrape via lib-metrics.sh.
# This file must NOT install traps (chaos.sh owns the single EXIT trap).
#
# SIGPIPE-safe substring / ERE tests over captured command output. NEVER use
# `echo "${big}" | grep -q` (or `producer | grep -q`) for asserts in this
# suite: `grep -q` exits at the first match, and once the producer's output
# exceeds the pipe buffer (64KB) the producer takes SIGPIPE (141) — under
# `set -o pipefail` that DISCARDS the successful match. Observed live: the
# retention-overrun asserts reported "consumer never hit REPLAY_UNAVAILABLE"
# while the refusal sat in the very alloc logs they grepped (the per-iteration
# `echo: write error: Broken pipe` spam in the CI log was each match being
# thrown away). Negated sites are worse — a SIGPIPE'd match reads as absence.
# Pure-bash matching has no pipe to break. (PR #158.)
has_line()  { [[ "$1" == *"$2"* ]]; }   # fixed substring (no regex chars)
has_match() { [[ "$1" =~ $2 ]]; }       # POSIX ERE

# --- inner-container helpers -------------------------------------------------

# First RUNNING inner (Nomad docker-driver) container on a node whose name
# starts with a prefix. Replaces the `docker exec <node> docker ps | grep -m1`
# idiom previously repeated ~11×. Case-insensitive + ^-anchored (task
# containers are named <task>-<alloc-id>). Prints the name; empty if none.
inner_container() { # <node> <name-prefix>
  timeout 15 docker exec "$1" sh -c \
    'docker ps --format "{{.Names}}" | grep -im1 "^'"$2"'"' 2>/dev/null || true
}

# A container's StartedAt timestamp on a node — THE newborn-vs-survivor
# identity signal (container NAMES survive an in-place task restart
# (task-<alloc-id>), so name equality proves nothing; a Nomad restart always
# mints a new generation with a new StartedAt — the sequencer-lapse /
# validator-lapse lesson). Empty on failure.
container_started_at() { # <node> <inner-container>
  timeout 15 docker exec "$1" docker inspect -f '{{.State.StartedAt}}' "$2" \
    2>/dev/null || true
}

# --- executor-tier probes ----------------------------------------------------

# Fetch one executor node's /metrics body. Bridge-DIRECT first with docker
# exec fallback (see fetch_metrics in lib-metrics.sh — issue #76 rationale).
# $1 = index into EXECUTOR_NODES.
exec_metrics() {
  fetch_metrics "${EXECUTOR_IPS[$1]}" "${EXECUTOR_NODES[$1]}" "${EXECUTOR_PORT}"
}

# Sealer boundary-counter probe: the sealer's boundary stream as re-exported
# by the executors from cluster egress (the Java cluster node itself has no
# Prometheus endpoint). Ticks ~4/s while the sealer is alive — a finer liveness
# signal than the block gauge. Tries each executor node like executor_progress.
sealer_boundaries() {
  # MAX across all responding executors, NOT the first responder — same
  # rationale as executor_progress below: a replica that restarted (or is
  # replaying/catching up) legitimately reports a low/frozen counter while its
  # peers — and the pipeline — are fine; pinning the probe to it reads as a
  # pipeline stall when nothing is wrong.
  local i v best=""
  for i in "${!EXECUTOR_NODES[@]}"; do
    v="$(prom_value "$(exec_metrics "${i}" || true)" \
      kardamom_sealer_boundaries_emitted_total first)"
    [ -n "${v}" ] && { [ -z "${best}" ] || [ "${v}" -gt "${best}" ]; } && best="${v}"
  done
  [ -n "${best}" ] && { printf '%s' "${best}"; return 0; }
  return 1
}

# Cluster-mode progress probe: the most-recently-committed block number as seen
# by the executor (EXECUTOR_BLOCK_METRIC gauge on :9004). The Java cluster node
# exposes no Prometheus endpoint, so pipeline liveness is measured at the
# executor — it applies the blocks the cluster commits out its egress, so its
# block gauge advancing IS the cluster making progress. Prints the integer
# value (or empty / non-zero if every scrape failed).
executor_progress() {
  # MAX across all responding executors, NOT the first responder: a replica
  # restarted by a chaos case legitimately reports gauge 0 (or a low block)
  # while it replays/recovers, even though its peers — and the pipeline — are
  # fine. Pinning the probe to that replica reads as "pipeline NOT progressing
  # (block 0 -> 0)" when nothing is wrong (observed on node-failure-executor
  # right after hard-executor restarted executor-0).
  local i v best=""
  for i in "${!EXECUTOR_NODES[@]}"; do
    v="$(prom_value "$(exec_metrics "${i}" || true)" "${EXECUTOR_BLOCK_METRIC}" first)"
    [ -n "${v}" ] && { [ -z "${best}" ] || [ "${v}" -gt "${best}" ]; } && best="${v}"
  done
  [ -n "${best}" ] && { printf '%s' "${best}"; return 0; }
  return 1
}

# --- ingress probe -----------------------------------------------------------

# Ingress submit counter (kardamom_ingress_tx_received_total) summed across the
# active/active ingress nodes — the "is load actually flowing?" signal for the
# injection gate in run_case. Ingress binds its exporter on loopback, so this
# goes through docker exec. Prints the sum, or fails if no ingress answered.
ingress_received() {
  local n v total=0 got=0
  for n in "${INGRESS_NODES[@]}"; do
    v="$(prom_value "$(fetch_metrics '' "${n}" "${INGRESS_PORT}" || true)" \
      kardamom_ingress_tx_received_total sum)"
    [ -n "${v}" ] && { total=$(( total + v )); got=1; }
  done
  [ "${got}" -eq 1 ] && { printf '%s' "${total}"; return 0; }
  return 1
}

# --- validator probe ---------------------------------------------------------

val_metric() { # <metric-name> -> integer (empty on scrape failure)
  # The `|| true` on the capture is load-bearing: under `set -euo pipefail` a
  # failed curl (the validator's exporter routinely stalls >5s right after
  # `docker unpause` while it chews through the lapse backlog) would otherwise
  # kill the whole script mid-case with NO fail() message — the validator-lapse
  # case died exactly this way on its first post-unpause probe. Empty output is
  # the documented contract; callers default it.
  local body
  body="$(fetch_metrics "" "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" || true)"
  prom_value "${body}" "$1" first
}

# D-2 companion to val_metric: REQUIRED scrape. val_metric's empty-on-failure
# contract is right for progress polling, but "divergence == 0" style asserts
# were reading a failed scrape as 0 — the assert passed exactly when the
# validator was too wedged to answer. Retries, then fails the case loudly.
val_metric_req() { # <metric-name> <why>
  local v i
  for i in 1 2 3 4 5; do
    v="$(val_metric "$1")"
    [ -n "${v}" ] && { printf '%s' "${v}"; return 0; }
    # Absent metric vs dead exporter: metrics-rs counters are not exported
    # until first incremented, so a healthy zero-divergence validator has NO
    # divergence_total line at all. If the always-present canary scrapes,
    # the exporter is alive and the absent counter genuinely IS 0.
    if [ -n "$(val_metric validator_committed_block)" ]; then
      printf '0'
      return 0
    fi
    sleep 3
  done
  fail "validator exporter unscrapeable after 5 tries — refusing to treat a dead exporter as 0 ($2)"
}

# --- sequencer shard-0 replica probes ----------------------------------------

seqa_metric() { # <metric-name> -> integer sum across label lines (empty on scrape failure)
  # seq-a on node-0: sequencer ip lane starts at .21, seq-a metrics :9001
  # (bridge-first + exec-fallback, mirroring assert_replica_healthy's probe).
  local body
  body="$(fetch_metrics 192.168.56.21 kardamom-sequencer-0 9001 || true)"
  prom_value "${body}" "$1" sum
}
seqb_twin_metric() { # <metric-name> — shard 0's replica B: seq-b on node-1 (.22:9011)
  local body
  body="$(fetch_metrics 192.168.56.22 kardamom-sequencer-1 9011 || true)"
  prom_value "${body}" "$1" sum
}
