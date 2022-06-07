package org.corfudb.infrastructure;

import org.corfudb.runtime.CorfuRuntime;

public interface CompactionTriggerPolicy {
    /**
     * Computes if the compaction jvm should be triggered or not.
     *
     * @return
     */
    boolean shouldTrigger(long interval);

    void markCompactionCycleStart();

    void setCorfuRuntime(CorfuRuntime corfuRuntime);
}
