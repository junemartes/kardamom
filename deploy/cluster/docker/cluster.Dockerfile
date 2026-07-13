# kardamom cluster node: one Aeron Cluster (Raft) member. Pure JVM — no native libs.
FROM eclipse-temurin:17-jre-jammy
WORKDIR /opt/kardamom
COPY kardamom-cluster-node.jar /opt/kardamom/cluster-node.jar
# Aeron's media driver reflects into JDK internals (sun.nio.ch.SelectorImpl,
# jdk.internal.misc.Unsafe, java.util.zip CRC32). On Java 9+ that needs --add-opens
# or the driver dies at startup with InaccessibleObjectException ("module java.base
# does not opens sun.nio.ch"). Same opens the in-JVM TestCluster test task uses
# (cluster/sealer-service/service/build.gradle).
ENTRYPOINT ["java", "-Xmx384m", \
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", \
    "--add-opens", "java.base/java.util.zip=ALL-UNNAMED", \
    "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", \
    "-cp", "/opt/kardamom/cluster-node.jar", "io.kardamom.sealer.cluster.ClusterNode"]
