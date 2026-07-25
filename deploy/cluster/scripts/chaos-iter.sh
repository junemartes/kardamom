#!/usr/bin/env bash
# =============================================================================
# chaos-iter.sh — ONE local iteration of the CI chaos suite (untracked helper).
#
# CI runs 4 chaos shards, each against a FRESH cluster (14 cases total). The
# chaos accounts are #7..#15 (max 9 per fresh chain), so a full local pass
# needs TWO fresh chains: half `a` = the chaos-executor + chaos-ingress shard
# cases, half `b` = chaos-sequencer + chaos-cluster. Each half: purge every
# Nomad job, wipe all durable cluster state (fresh chain, nonce-0 accounts),
# then run the UNMODIFIED ci-cluster.sh with the CI knobs — bring-up, smoke
# gate, chaos cases, ingress-churn re-smoke, validator verdict.
#
# Run INSIDE the orchestrator:  bash /work/deploy/cluster/scripts/chaos-iter.sh a
# =============================================================================
set -euo pipefail

HALF="${1:?usage: chaos-iter.sh <a|b>}"
export NOMAD_ADDR="http://192.168.56.10:4646"

case "${HALF}" in
  a) CASES="graceful-executor hard-executor node-failure-executor graceful-ingress hard-ingress archive-driver-loss archive-tx-data-wipe" ;;
  b) CASES="graceful-sequencer hard-sequencer sequencer-replica-kill validator-lapse cluster-leader-kill cluster-follower-kill cluster-quorum-loss-recover" ;;
  *) echo "unknown half ${HALF}" >&2; exit 1 ;;
esac

echo "==> [iter] reset: purging Nomad jobs (fresh chain for half ${HALF})"
for j in batcher da-watcher validator executor ingress sequencer cluster anvil aeron; do
  nomad job stop -purge "$j" >/dev/null 2>&1 || true
done

# Nodes may have been docker-killed by a previous chaos case — restart them
# so the wipe below reaches every node and Nomad reconverges.
NODES="$(docker ps -a --format '{{.Names}}' | grep '^kardamom-' | grep -v '^kardamom-orch$' || true)"
for n in ${NODES}; do docker start "$n" >/dev/null 2>&1 || true; done

# Wait for the inner Nomad task containers to die (registry on control-0 is
# not a Nomad task — exclude it).
echo "==> [iter] waiting for inner task containers to stop"
for n in ${NODES}; do
  for _ in $(seq 1 40); do
    left="$(docker exec "$n" docker ps --format '{{.Names}}' 2>/dev/null | grep -cv '^registry' || true)"
    [ "${left:-0}" -eq 0 ] && break
    sleep 2
  done
done

echo "==> [iter] wiping durable state on every node (state/cluster/archive/aeron-mount)"
for n in ${NODES}; do
  docker exec "$n" bash -lc \
    'rm -rf /opt/kardamom/state/* /opt/kardamom/cluster/* /opt/kardamom/archive/* /opt/kardamom/aeron-mount/* 2>/dev/null; true' \
    || echo "WARN: wipe failed on $n"
done

echo "==> [iter] running ci-cluster.sh half=${HALF} cases=[${CASES}]"
cd /work
RUN_LOAD=0 RUN_CHAOS=1 \
  CHAOS_CASES="${CASES}" \
  CHAOS_TPS=200 CHAOS_CASE_S=120 CHAOS_RESCHEDULE_SLO_S=200 CHAOS_LEADER_SLO_S=45 \
  KEEP=1 REGISTRY_PUSH_NODE=control-0 \
  deploy/cluster/scripts/ci-cluster.sh
