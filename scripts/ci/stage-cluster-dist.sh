#!/usr/bin/env bash
# Stage everything a cluster-e2e shard runner needs into one directory, in
# the layout the deploy tree reads from a checkout: the service binaries
# and the operator binaries under target/release, the shard test
# executable as target/release/kardamom-chaos-shards, the Aeron shared
# libraries under their rusteron build directories (roles/images selects
# the most recent build of each library there), and the sealer jar at
# its Gradle output path. A shard runner unpacks this over its checkout
# and runs make with KARDAMOM_STAGED=1.
#
# Use: scripts/ci/stage-cluster-dist.sh <dist-dir>
set -euo pipefail
dist=${1:?dist dir}
rel=target/release
mkdir -p "$dist/$rel/build" "$dist/cluster/sealer-service/service/build/libs"
# The services the images wrap (the state mirror included), the settlement
# deployer and the semantics runner the stages spawn, the operator binary,
# and the archive tool the archive-corruption case runs on the host.
for bin in ingress sequencer executor validator da-watcher batcher state-mirror reconstruct deploy semantics cluster archive-rereplicate; do
  cp "$rel/kardamom-$bin" "$dist/$rel/"
done
# The shard test executable carries a build hash; the newest one is this build's.
shards=$(ls -t "$rel"/deps/shards-* | grep -v '\.d$' | head -n 1)
cp "$shards" "$dist/$rel/kardamom-chaos-shards"
for lib in "$rel"/build/rusteron-archive-*/out/build/lib; do
  build=$(basename "$(dirname "$(dirname "$(dirname "$lib")")")")
  mkdir -p "$dist/$rel/build/$build/out/build/lib"
  cp "$lib"/libaeron*.so "$dist/$rel/build/$build/out/build/lib/"
done
cp cluster/sealer-service/service/build/libs/kardamom-cluster-node.jar "$dist/cluster/sealer-service/service/build/libs/"
find "$dist" -type f | sort
