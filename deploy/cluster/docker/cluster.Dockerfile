# kardamom cluster node: one Aeron Cluster (Raft) member. Pure JVM — no native libs.
FROM eclipse-temurin:17-jre-jammy
WORKDIR /opt/kardamom
COPY kardamom-cluster-node.jar /opt/kardamom/cluster-node.jar
ENTRYPOINT ["java", "-Xmx384m", "-cp", "/opt/kardamom/cluster-node.jar", "io.kardamom.sealer.cluster.ClusterNode"]
