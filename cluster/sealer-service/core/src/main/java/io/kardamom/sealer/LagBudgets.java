package io.kardamom.sealer;

/**
 * The budgets of the two lag guards. Both are replicated configuration:
 * every member must use the same values, because they decide accept or
 * refuse inside the replicated state machine. The snapshot does not carry
 * them. Zero turns a guard off.
 *
 * @param daLagBlocks      how far the sealed head may run past the posted
 *                         head, in blocks
 * @param recordLagRecords how far the last ordered index may run past the
 *                         best recorded cursor, in canonical records
 */
public record LagBudgets(long daLagBlocks, long recordLagRecords) {

    /** The code defaults: the DA-lag guard on, the record-lag guard off. */
    public static final LagBudgets DEFAULT = new LagBudgets(
        CanonicalSealerState.DEFAULT_DA_LAG_BUDGET_BLOCKS,
        CanonicalSealerState.DEFAULT_RECORD_LAG_BUDGET);

    /**
     * @throws IllegalArgumentException if a budget is negative
     */
    public LagBudgets {
        if (daLagBlocks < 0) {
            throw new IllegalArgumentException("daLagBudgetBlocks must be >= 0, got " + daLagBlocks);
        }
        if (recordLagRecords < 0) {
            throw new IllegalArgumentException("recordLagBudget must be >= 0, got " + recordLagRecords);
        }
    }
}
