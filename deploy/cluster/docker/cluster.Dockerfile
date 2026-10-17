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
# With a cap set (KARDAMOM_IDLE_CAP_US, see IdleCap.java), the shell sets its
# own timer slack to 1 µs, then becomes the JVM. Linux ends a park up to the
# timer slack of the thread after its deadline, 50 µs by default, so a 50 µs
# park can take 100 µs. The JVM keeps the slack of the shell, and each JVM
# thread takes the slack of the thread that starts it. A kernel that refuses
# the write keeps the default slack. The shell prints the slack that the JVM
# gets.
ENTRYPOINT ["/bin/sh", "-c", "if [ -n \"${KARDAMOM_IDLE_CAP_US:-}\" ]; then echo 1000 > /proc/self/timerslack_ns || echo 'could not set the timer slack' >&2; fi; echo \"idle cap: KARDAMOM_IDLE_CAP_US=${KARDAMOM_IDLE_CAP_US:-<unset>} timerslack_ns=$(cat /proc/self/timerslack_ns 2>&1)\"; exec java -Xmx384m --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp /opt/kardamom/cluster-node.jar io.kardamom.sealer.cluster.ClusterNode \"$@\"", "cluster-node"]
