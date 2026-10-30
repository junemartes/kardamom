package io.kardamom.sealer.cluster;

/** Prints the time of each stage of a member-process test since its group started, for the runtime report. */
final class Stage {

    private static long startNs = System.nanoTime();

    private Stage() {
    }

    /** Start the stage clock of a new test. */
    static void begin() {
        startNs = System.nanoTime();
    }

    static void log(final String stage) {
        System.out.printf("%s at %d ms%n", stage, (System.nanoTime() - startNs) / 1_000_000);
    }
}
