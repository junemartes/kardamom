#!/usr/bin/env bash
# =============================================================================
# chaos-iter.sh — runs one local iteration of the CI chaos suite (untracked helper).
#
# CI runs 4 chaos shards. Each shard runs against a fresh cluster (14 cases
# total). The chaos accounts are #7 through #15, and a fresh chain holds at
# most 9 accounts. So a full local pass needs two fresh chains. Half `a` runs
# the chaos-executor and chaos-ingress shard cases. Half `b` runs
# chaos-sequencer and chaos-cluster. Each half purges every Nomad job, wipes
# all durable cluster state for a fresh chain with nonce-0 accounts, then
# runs the unmodified ci-cluster.sh with the CI settings: bring-up, smoke
# gate, chaos cases, ingress-churn re-smoke, and validator verdict.
#
# Run inside the orchestrator: bash /work/deploy/cluster/scripts/chaos-iter.sh a
# =============================================================================
set -euo pipefail

HALF="${1:?usage: chaos-iter.sh <a|b>}"
export NOMAD_ADDR="http://192.168.56.10:4646"

case "${HALF}" in
  # The halves track the CI shards' case lists. cluster-e2e.yml is the
  # source of truth. The retention-overrun* cases need
  # KARDAMOM_CLUSTER_RETENTION deployed small. They are opt-in here, not
  # part of a half.
  a) CASES="graceful-executor hard-executor node-failure-executor state-checkpoint-restore replay-window-resync graceful-ingress hard-ingress archive-driver-loss archive-tx-data-wipe archive-corruption" ;;
  b) CASES="graceful-sequencer hard-sequencer sequencer-replica-kill sequencer-lapse validator-lapse validator-join cluster-leader-kill cluster-follower-kill cluster-member-rejoin cluster-quorum-loss-recover" ;;
  *) echo "unknown half ${HALF}" >&2; exit 1 ;;
esac

echo "==> [iter] reset: purging Nomad jobs (fresh chain for half ${HALF})"
for j in batcher da-watcher validator executor ingress sequencer cluster anvil aeron; do
  nomad job stop -purge "$j" >/dev/null 2>&1 || true
done

# A previous chaos case may have used docker kill on some nodes. Restart
# them so the wipe below reaches every node, and Nomad reconverges.
NODES="$(docker ps -a --format '{{.Names}}' | grep '^kardamom-' | grep -v '^kardamom-orch$' || true)"
for n in ${NODES}; do docker start "$n" >/dev/null 2>&1 || true; done

# Wait for the inner Nomad task containers to stop. The registry on
# control-0 is not a Nomad task, so this excludes it.
echo "==> [iter] waiting for inner task containers to stop"
for n in ${NODES}; do
  for _ in $(seq 1 40); do
    left="$(docker exec "$n" docker ps --format '{{.Names}}' 2>/dev/null | grep -cv '^registry' || true)"
    [ "${left:-0}" -eq 0 ] && break
    sleep 2
  done
done

echo "==> [iter] wiping durable state on every node (state/cluster/archive/aeron-mount/checkpoints)"
for n in ${NODES}; do
  docker exec "$n" bash -lc \
    'rm -rf /opt/kardamom/state/* /opt/kardamom/cluster/* /opt/kardamom/archive/* /opt/kardamom/aeron-mount/* /opt/kardamom/checkpoints/* /opt/kardamom/batcher/* 2>/dev/null; true' \
    || echo "WARN: wipe failed on $n"
done

echo "==> [iter] running ci-cluster.sh half=${HALF} cases=[${CASES}]"
cd /work
RUN_LOAD=0 RUN_CHAOS=1 \
  CHAOS_CASES="${CASES}" \
  CHAOS_TPS=200 CHAOS_CASE_S=120 CHAOS_RESCHEDULE_SLO_S=200 CHAOS_LEADER_SLO_S=45 \
  KEEP=1 REGISTRY_PUSH_NODE=control-0 \
  deploy/cluster/scripts/ci-cluster.sh
