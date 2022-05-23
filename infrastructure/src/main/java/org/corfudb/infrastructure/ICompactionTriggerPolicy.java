package org.corfudb.infrastructure;

import org.corfudb.runtime.CorfuRuntime;

public interface ICompactionTriggerPolicy {
    /**
     * Computes if the compaction jvm should be triggered or not.
     *
     * @return
     */
    boolean shouldTrigger(long interval);

    long getLastCompactionCycleStartTime();

    void markCompactionCycleStart();

    void markTrimComplete();

    void setCorfuRuntime(CorfuRuntime corfuRuntime);
}
