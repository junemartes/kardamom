package io.kardamom.sealer;

/**
 * The chain's data-availability status, as the sealer fans it out to every
 * session: the last L2 block posted to L1 (the batcher's published cursor),
 * the last sealed block, the DA-lag budget, and whether the guard refuses
 * new transactions. The egress retention floors ride along, so an observer
 * can show how far the retention stretched above the posted head. The
 * record-lag guard follows: the best recorded cursor of the executors, its
 * budget, and whether it refuses new transactions.
 *
 * @param postedHead     the last L2 block the batcher confirmed on L1; 0
 *                       until the batcher publishes its cursor
 * @param sealedHead     the last sealed block
 * @param budgetBlocks   the DA-lag budget; 0 turns the guard off
 * @param halted         whether the guard refuses new transactions
 * @param retainedFrames the egress frames the member retains for replay
 * @param floorIndex     the oldest record index still retained
 * @param floorBlock     the oldest boundary block still retained
 * @param bestRecorded   the best recorded cursor of the executors, or
 *                       {@link RecordedCursors#NONE} before the first one
 * @param recordLagBudget the record-lag budget; 0 turns the guard off
 * @param recordLagHalted whether the record-lag guard refuses new
 *                       transactions
 */
public record ClusterStatus(
        long postedHead,
        long sealedHead,
        long budgetBlocks,
        boolean halted,
        long retainedFrames,
        long floorIndex,
        long floorBlock,
        long bestRecorded,
        long recordLagBudget,
        boolean recordLagHalted) {
}
