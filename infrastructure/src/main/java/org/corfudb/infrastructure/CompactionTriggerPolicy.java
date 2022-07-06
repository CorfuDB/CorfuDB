package org.corfudb.infrastructure;

import org.corfudb.runtime.collections.CorfuStore;

public interface CompactionTriggerPolicy {
    /**
     * Computes if the compaction jvm should be triggered or not.
     *
     * @return
     */
    boolean shouldTrigger(long interval, CorfuStore corfuStore);

    void markCompactionCycleStart();
}
