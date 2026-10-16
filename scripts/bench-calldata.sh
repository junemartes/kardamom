#!/usr/bin/env bash
# Run the calldata bench matrix on the local container cluster and write a
# Markdown table. `just bench-calldata` builds kardamom-load and calls this
# script.
#
# Each cell runs a warm-up load, then a measured load at a fixed rate. Each
# load starts every sender at its nonce on the chain, so the cells run one
# after the other on one chain.
#
# The defaults are the cells that the container cluster held on a shared
# 12-CPU host. A cell that overloads the cluster can stall the chain until
# a `just container-reset`, so the cells that can overload run last.
#
# The settings come from the environment:
#   FULL       1 runs the full matrix of the spec: the rates 500, 1000,
#              2000, 4000 and 6000, and the 5,000 tx/s target rows. It
#              overloads a shared host. RATES and EXTRA still override it.
#   RATES      the rates in tx/s (default "250 500")
#   SIZES      the calldata sizes in bytes (default "0 1938 8082"). The raw
#              size of a transfer is about the calldata size + 110 bytes, so
#              the defaults give raw sizes of about 110, 2048 and 8192 bytes.
#   EXTRA      more cells, as "rate/calldata" words; empty for none. The
#              calldata is a size or a weighted mix (default "1000/0"; with
#              FULL=1 the 5,000 tx/s rows: 8 KB only, and the mix of 80 %
#              transfers, 15 % 2 KB and 5 % 8 KB).
#   WARMUP     the warm-up of each cell (default 60s)
#   DURATION   the measured part of each cell (default 300s)
#   SENDERS    the sender count (default 15); the first sender is account 1
#   MAX_IN_FLIGHT  the limit on open submits (default 4096)
#   NODE_CONTRACT  the node contract of the container cluster
#   LOAD_BIN   the kardamom-load binary
set -euo pipefail

out=${1:?usage: bench-calldata.sh OUT.md}
if [[ "${FULL:-0}" == 1 ]]; then
    echo ">> FULL=1: the spec matrix overloads a shared host; an overload can stall the chain until a reset" >&2
    RATES=${RATES:-500 1000 2000 4000 6000}
    EXTRA=${EXTRA-5000/8082 5000/0:16,1938:3,8082:1}
fi
RATES=${RATES:-250 500}
SIZES=${SIZES:-0 1938 8082}
EXTRA=${EXTRA-1000/0}
WARMUP=${WARMUP:-60s}
DURATION=${DURATION:-300s}
SENDERS=${SENDERS:-15}
MAX_IN_FLIGHT=${MAX_IN_FLIGHT:-4096}
NODE_CONTRACT=${NODE_CONTRACT:-deploy/cluster/terraform/containers/node-contract.json}
LOAD_BIN=${LOAD_BIN:-target/release/kardamom-load}
METRIC=kardamom_ingress_seal_latency_seconds
EXPIRED=kardamom_ingress_seal_stamps_expired_total
DROPPED=kardamom_ingress_seal_stamps_dropped_total

work=$(mktemp -d)
# The background jobs of the running cell: the CPU sampler and the load.
# The cells run in this shell, not in a pipe, so the exit trap sees both.
sampler=""
loader=""
stop_jobs() {
    [[ -n "$sampler" ]] && kill "$sampler" 2>/dev/null
    [[ -n "$loader" ]] && kill "$loader" 2>/dev/null
    rm -rf "$work"
}
trap stop_jobs EXIT
# A signal ends the shell through `exit`, so the exit trap runs.
trap 'exit 130' INT
trap 'exit 143' TERM

ip_of() { jq -er --arg n "$1" '.nodes[$n].ip' "$NODE_CONTRACT"; }
ingress_ip=$(ip_of ingress-0)
rpc="http://$ingress_ip:8545"
mapfile -t sealers < <(jq -r '.nodes[] | select(.role == "sealer") | .container' "$NODE_CONTRACT" | sort)

# The metrics of ingress-0. A failed read gives no lines, not an exit.
scrape() { curl -sf "http://$ingress_ip:9006/metrics" || true; }

# The cumulative buckets of the seal latency histogram in the metrics file
# $1, as "le count" lines, and the expired and the dropped stamps, as
# "lost count" lines. A missing metric gives no line.
buckets_of() {
    awk -v m="${METRIC}_bucket" -v e="$EXPIRED" -v d="$DROPPED" '
        index($0, m "{") == 1 {
            match($0, /le="[^"]*"/); print substr($0, RSTART + 4, RLENGTH - 5), $NF }
        index($0, e "{") == 1 || index($0, e " ") == 1 || index($0, d "{") == 1 || index($0, d " ") == 1 {
            print "lost", $NF }' "$1"
}

# The lost stamps of the measured part: the counters in $2 minus those in $1.
lost_between() {
    # A missing counter counts as 0: the ingress exports it at its first count.
    local count='$1 == "lost" { v += $2 } END { print v + 0 }'
    echo $(($(awk "$count" "$2") - $(awk "$count" "$1")))
}

# The quantile q of the bucket counts in $2 minus those in $1, in
# milliseconds. A value inside a bucket is interpolated linearly. A value
# above the top bucket prints as ">top". "-" means no sample.
quantile_ms() {
    join <(sort "$1") <(sort "$2") | awk '$1 != "lost"' | sort -g -k1,1 | awk -v q="$3" '
        { le[NR] = $1; n[NR] = $3 - $2 }
        END {
            total = n[NR]
            if (total <= 0) { print "-"; exit }
            want = q * total; lo = 0; prev = 0
            for (i = 1; i <= NR; i++) {
                if (n[i] >= want && le[i] == "+Inf") { printf ">%.0f\n", lo * 1000; exit }
                if (n[i] >= want) {
                    hi = le[i]
                    span = n[i] - prev
                    frac = (span > 0) ? (want - prev) / span : 1
                    printf "%.1f\n", (lo + (hi - lo) * frac) * 1000
                    exit
                }
                lo = le[i]; prev = n[i]
            }
        }'
}

# The busy and total jiffies of the host, from /proc/stat.
host_jiffies() { awk '/^cpu / { print $2 + $3 + $4 + $7 + $8, $2 + $3 + $4 + $5 + $6 + $7 + $8 }' /proc/stat; }

# The member that logged LEADER last, as its container name. A failed
# read gives no name. awk reads all its input, so no writer gets SIGPIPE.
leader() {
    local c
    for c in "${sealers[@]}"; do
        { docker exec "$c" sh -c 'grep -h "role=" /opt/nomad/alloc/*/alloc/logs/cluster.stdout.* 2>/dev/null | tail -1' || true; } \
            | awk -v c="$c" '/role=LEADER/ { print c }'
    done | awk 'NR == 1'
}

# The "sealer snapshot TAKEN" lines that every member logged after $1. No
# line, or a failed read, gives no output and no exit.
snapshots_since() {
    local c
    for c in "${sealers[@]}"; do
        { docker exec "$c" sh -c 'grep -h "sealer snapshot TAKEN" /opt/nomad/alloc/*/alloc/logs/cluster.stdout.* 2>/dev/null' || true; } \
            | awk -v t="$1" 'substr($1, 1, 19) >= t'
    done
}

# Sample the CPU of one container every 5 s until killed.
sample_cpu() {
    while sleep 5; do
        { docker stats --no-stream --format '{{.CPUPerc}}' "$1" || true; } | tr -d '%'
    done
}

# Wait until the nonce of the first sender holds for 15 s, at most 10
# minutes: the backlog of the last cell is then sealed and executed.
settle() {
    local addr=0x70997970C51812dc3A010C7d01b50e0d17dc79C8 last="" now deadline=$((SECONDS + 600))
    while ((SECONDS < deadline)); do
        now=$(curl -sf -X POST -H 'content-type: application/json' \
            -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_getTransactionCount\",\"params\":[\"$addr\",\"latest\"]}" \
            "$rpc" | jq -r .result || true)
        [[ -n "$now" && "$now" == "$last" ]] && return
        last=$now
        sleep 15
    done
    echo ">> the chain did not settle in 10 minutes" >&2
}

# A load with a value of 1 to 10^9 wei from the clock. Two loads then never sign
# the same transaction, so a transaction of a cell that failed never
# makes the sealer drop a transaction of a later cell as a duplicate.
load() {
    local value=$((10#$(date +%N) + 1))
    # The load runs as a background job, and the shell waits for it: a
    # signal then interrupts the wait at once, and the exit trap stops it.
    "$LOAD_BIN" --rpc "$rpc" --fixed-rate --target-tps "$1" --workload calldata --value "$value" \
        --calldata-bytes "$2" --senders "$SENDERS" --sender-offset 1 --nonce-start chain \
        --duration "$3" --scrape executor,ingress,sequencer --subscribe \
        --max-in-flight "$MAX_IN_FLIGHT" "${@:4}" &
    loader=$!
    local rc=0
    wait "$loader" || rc=$?
    loader=""
    return "$rc"
}

cell() {
    local rate=$1 calldata=$2 dir="$work/$1-${2//[:,]/_}"
    mkdir -p "$dir"
    echo ">> cell rate=$rate calldata=$calldata: warm-up $WARMUP" >&2
    load "$rate" "$calldata" "$WARMUP" --drain-timeout 60s >"$dir/warmup.log" 2>&1 || true
    local lead since
    lead=$(leader)
    since=$(date -u +%Y-%m-%dT%H:%M:%S)
    scrape >"$dir/metrics-before"
    buckets_of "$dir/metrics-before" >"$dir/before"
    read -r busy0 total0 < <(host_jiffies)
    sample_cpu "$lead" >"$dir/cpu" &
    sampler=$!
    echo ">> cell rate=$rate calldata=$calldata: measure $DURATION, leader $lead" >&2
    load "$rate" "$calldata" "$DURATION" --assert-all-delivered --output "$dir/report.json" \
        >"$dir/load.log" 2>&1 || true
    kill "$sampler" 2>/dev/null || true
    sampler=""
    scrape >"$dir/metrics-after"
    buckets_of "$dir/metrics-after" >"$dir/after"
    read -r busy1 total1 < <(host_jiffies)
    local verdict delivered offered receipt seal lost cpu snap load1 host_cpu
    verdict=$(jq -r 'if .verdict.pass then "PASS" else "FAIL" end' "$dir/report.json" 2>/dev/null || echo "NO REPORT")
    delivered=$(jq -r '"\(.verdict.receipted)/\(.verdict.offered)"' "$dir/report.json" 2>/dev/null || echo "-")
    offered=$(jq -r '.verdict.offered / .duration_secs | floor' "$dir/report.json" 2>/dev/null || echo "-")
    receipt=$(jq -r '"\(.lat_p50_us / 1000 | floor) / \(.lat_p99_us / 1000 | floor)"' "$dir/report.json" 2>/dev/null || echo "-")
    seal="$(quantile_ms "$dir/before" "$dir/after" 0.5) / $(quantile_ms "$dir/before" "$dir/after" 0.99)"
    lost=$(lost_between "$dir/before" "$dir/after")
    cpu=$(awk '{ s += $1; if ($1 > m) m = $1 } END { if (NR) printf "%.0f / %.0f", s / NR, m; else print "-" }' "$dir/cpu")
    snap=$(snapshots_since "$since" | sed -n 's/.* ms=\([0-9]*\).*/\1/p' \
        | awk '{ if ($1 > m) m = $1 } END { if (NR) printf "%d (%d)", m, NR; else print "-" }')
    load1=$(cut -d' ' -f1 /proc/loadavg)
    host_cpu=$(awk -v b="$((busy1 - busy0))" -v t="$((total1 - total0))" 'BEGIN { printf "%.0f", t ? 100 * b / t : 0 }')
    echo "| $rate | $offered | $calldata | $verdict | $delivered | $receipt | $seal | $lost | $cpu | $snap | $load1 | $host_cpu |"
}

cells=()
for size in $SIZES; do
    for rate in $RATES; do cells+=("$rate/$size"); done
done
read -r -a extra <<<"$EXTRA"
cells+=("${extra[@]}")

{
    echo "# Calldata bench: $(date -u +%Y-%m-%d)"
    echo
    echo "- Revision: \`$(git rev-parse --short HEAD 2>/dev/null || jj log -r @- --no-graph -T 'commit_id.short()' 2>/dev/null || echo unknown)\`"
    echo "- Host: $(nproc) CPUs, $(awk '/MemTotal/ { printf "%.0f GiB", $2 / 1048576 }' /proc/meminfo), load average at start $(cut -d' ' -f1-3 /proc/loadavg)"
    echo "- Each cell: warm-up $WARMUP, then $DURATION measured, $SENDERS senders, \`--fixed-rate --subscribe --max-in-flight $MAX_IN_FLIGHT\`. Offered tx/s is the offered count over the measured time."
    echo "- Calldata: the calldata bytes of each transaction. The raw size is about the calldata + 110 bytes. \`a:w,b:w\` is a weighted mix."
    echo "- Receipt latency: \`kardamom-load\`, p50 / p99 in ms. Seal latency: \`$METRIC\` on ingress-0, p50 / p99 in ms. The value is a linear guess inside its bucket; above 10 ms the bucket bounds are 25, 50, 100, 250 and 500 ms, then 1, 2.5 and 5 s. A seal later than 180 s gives no sample: lost stamps are \`$EXPIRED\` (no seal within 180 s) plus \`$DROPPED\` (the stamp channel was full). A non-zero count means that the seal values miss those transactions, and the tail is too low."
    echo "- Leader CPU: \`docker stats\` of the leader node container every 5 s, mean / max in % of one CPU."
    echo "- Snapshot: the largest \`ms=\` of the \`sealer snapshot TAKEN\` lines of all members in the measured part, and the line count."
    echo "- Host load and host CPU: the 1-minute load average at the end of the cell, and the busy % of all host CPUs in the measured part."
    echo
    echo "| tx/s | offered tx/s | calldata | verdict | receipted/offered | receipt ms | seal ms | lost stamps | leader CPU % | snapshot ms (n) | host load | host CPU % |"
    echo "|---|---|---|---|---|---|---|---|---|---|---|---|"
} >"$out"

for c in "${cells[@]}"; do
    settle
    cell "${c%%/*}" "${c#*/}" >>"$out"
    tail -n 1 "$out" >&2
done
echo ">> wrote $out" >&2
