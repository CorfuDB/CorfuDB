package org.corfudb.infrastructure;

public interface CompactionTriggerPolicy {
    /**
     * Computes if the compaction jvm should be triggered or not.
     *
     * @return
     */
    boolean shouldTrigger(long interval);

    void markCompactionCycleStart();
}
