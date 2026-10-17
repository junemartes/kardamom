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
# The shell sets its own timer slack to 1 µs, then becomes the JVM. Linux
# ends a park up to the timer slack of the thread after its deadline, 50 µs
# by default, so the 50 µs idle park of the member (IdleCap.java) can take
# 100 µs. The JVM keeps the slack of the shell, and each JVM thread takes the
# slack of the thread that starts it. A kernel that refuses the write keeps
# the default slack.
ENTRYPOINT ["/bin/sh", "-c", "{ echo 1000 > /proc/self/timerslack_ns; } 2>/dev/null || true; exec java -Xmx384m --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED -cp /opt/kardamom/cluster-node.jar io.kardamom.sealer.cluster.ClusterNode \"$@\"", "cluster-node"]
