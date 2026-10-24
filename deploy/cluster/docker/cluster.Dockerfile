# kardamom cluster node: one Aeron Cluster (Raft) member. Pure JVM — no native libs.
FROM eclipse-temurin:17-jre-jammy
WORKDIR /opt/kardamom
COPY kardamom-cluster-node.jar /opt/kardamom/cluster-node.jar
# Aeron's media driver reflects into JDK internals (sun.nio.ch.SelectorImpl,
# jdk.internal.misc.Unsafe, java.util.zip CRC32). On Java 9+ that needs --add-opens
# or the driver dies at startup with InaccessibleObjectException ("module java.base
# does not opens sun.nio.ch"). Same opens the in-JVM TestCluster test task uses
# (cluster/sealer-service/service/build.gradle).
#
# With a cap below Aeron's own 1000 µs (KARDAMOM_IDLE_CAP_US, see
# IdleCap.java), the shell sets its own timer slack to 1 µs, then becomes the
# JVM. At 1000 µs or unset, the member keeps Aeron's waits and the default
# slack, because a 1 µs slack makes every short park of the JVM a wake-up of
# its own. Linux ends a park up to the
# timer slack of the thread after its deadline, 50 µs by default, so a 50 µs
# park can take 100 µs. The JVM keeps the slack of the shell, and each JVM
# thread takes the slack of the thread that starts it. A kernel that refuses
# the write keeps the default slack. The shell prints the slack that the JVM
# gets.
#
# KARDAMOM_CLUSTER_HEAP_MB is the heap of the member, 384 MB when it is
# unset. The retained frames of the sealer grow with the DA lag, and a
# member whose service thread runs out of heap must not stay up: the
# consensus module of a live JVM keeps its role, and a leader with a dead
# service orders nothing. So the JVM exits on the first OutOfMemoryError,
# and the member restarts from its snapshot.
ENTRYPOINT ["/bin/sh", "-c", "if [ \"${KARDAMOM_IDLE_CAP_US:-1000}\" -lt 1000 ] 2>/dev/null; then echo 1000 > /proc/self/timerslack_ns || echo 'could not set the timer slack' >&2; fi; echo \"idle cap: KARDAMOM_IDLE_CAP_US=${KARDAMOM_IDLE_CAP_US:-<unset>} timerslack_ns=$(cat /proc/self/timerslack_ns 2>&1)\"; exec java -Xmx${KARDAMOM_CLUSTER_HEAP_MB:-384}m -XX:+ExitOnOutOfMemoryError --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp /opt/kardamom/cluster-node.jar io.kardamom.sealer.cluster.ClusterNode \"$@\"", "cluster-node"]
