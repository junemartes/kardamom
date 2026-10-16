package io.kardamom.sealer.cluster;

import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * The settings of a member that keeps its retained frames in a file: the
 * file, and the shared token that guards its segments on the admin
 * endpoint.
 *
 * @param file  the retained-file settings
 * @param token the shared token of the job
 */
record CarryBytes(RetainedFile.Config file, RetainedToken token) {

    /**
     * The raw values of the settings, as the node reads them at the start.
     *
     * @param carryBytes        {@code kardamom.cluster.carryBytes}
     * @param segmentBytes      {@code kardamom.cluster.retainedSegmentBytes}
     * @param token             the env var {@code KARDAMOM_RETAINED_TOKEN}
     * @param clusterDir        the consensus module directory
     * @param adminPort         the admin port; 0 is off
     * @param snapshotIntervalS the snapshot interval; 0 is off
     */
    record Inputs(String carryBytes, String segmentBytes, String token, Path clusterDir, int adminPort,
            long snapshotIntervalS) {

        /**
         * The settings, or empty when the switch is off.
         *
         * <ul>
         *   <li>A member that a peer seeds copies the file from the admin
         *       endpoint of a peer, so the file needs the admin port and the
         *       token.</li>
         *   <li>The purge runs at each snapshot, so the file needs the
         *       snapshot scheduler: without it, the file never shrinks.</li>
         * </ul>
         *
         * @throws IllegalArgumentException when a value is missing, or does not parse
         */
        Optional<CarryBytes> parse() {
            return RetainedFile.Config.parse(carryBytes, segmentBytes, clusterDir).map(this::require);
        }

        private CarryBytes require(final RetainedFile.Config file) {
            if (adminPort == 0) {
                throw new IllegalArgumentException(RetainedFile.Config.SETTING + "=true needs"
                    + " kardamom.cluster.adminPort: a member that a peer seeds copies the retained file from it");
            }
            final RetainedToken parsed = RetainedToken.of(token).orElseThrow(() -> new IllegalArgumentException(
                RetainedFile.Config.SETTING + "=true needs the env var " + RetainedToken.ENV
                    + ": it guards the retained file"));
            if (snapshotIntervalS <= 0) {
                throw new IllegalArgumentException(RetainedFile.Config.SETTING + "=true needs"
                    + " kardamom.cluster.snapshotIntervalS above 0: the purge of the retained file runs at each snapshot");
            }
            return new CarryBytes(file, parsed);
        }
    }

    /**
     * The members of the cluster, {@code id,ingress,consensus,log,catchup,archive}
     * each, separated by {@code |}.
     *
     * @param spec the member list
     */
    record Members(String spec) {
        /**
         * The admin endpoints of every member except {@code memberId}: the
         * host of each ingress endpoint, with the admin port that every
         * member uses.
         */
        List<URI> peerAdminEndpoints(final int memberId, final int adminPort) {
            return Arrays.stream(spec.split("\\|"))
                .map(member -> member.split(","))
                .filter(f -> Integer.parseInt(f[0].trim()) != memberId)
                .map(f -> URI.create("http://" + f[1].trim().split(":")[0] + ":" + adminPort))
                .toList();
        }
    }

    /** The copy that a member that a peer seeds runs. */
    RetainedCopy copy(final List<URI> peers, final int memberId) {
        return new RetainedCopy(peers, new SegmentDir(file.dir()), token, memberId);
    }

    /** The segments of the file on the admin endpoint, below {@code head}. */
    RetainedEndpoint endpoint(final LongSupplier head) {
        return RetainedEndpoint.start(new SegmentDir(file.dir()), head, token);
    }
}
