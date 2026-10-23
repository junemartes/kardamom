package io.kardamom.sealer.cluster;

import java.net.URI;
import java.util.List;

/**
 * The member list of the cluster: {@code id,ingress,consensus,log,catchup,archive}
 * for each member, separated by {@code |}.
 *
 * @param spec the member list
 */
record ClusterMembers(String spec) {

    /**
     * The admin endpoints of every member except {@code memberId}: the host
     * of each ingress endpoint, with the admin port that every member uses.
     */
    List<URI> peerAdminEndpoints(final int memberId, final int adminPort) {
        return ClusterNode.peerEntries(spec, memberId)
            .map(fields -> URI.create("http://" + fields[1].trim().split(":")[0] + ":" + adminPort))
            .toList();
    }

    /** The consensus endpoints of every member, comma separated: the members that a backup node asks. */
    String consensusEndpoints() {
        return ClusterNode.consensusEndpoints(ClusterNode.entries(spec));
    }
}
