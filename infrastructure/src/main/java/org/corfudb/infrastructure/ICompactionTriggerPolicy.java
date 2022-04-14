package org.corfudb.infrastructure;

import org.corfudb.runtime.CorfuRuntime;

public interface ICompactionTriggerPolicy {
    /**
     * Computes if the compaction jvm should be triggered or not.
     *
     * @return
     */
    boolean shouldTrigger(long interval);

    long getLastCheckpointStartTime();
    void markCompactionCycleStart();

    void markTrimComplete();

    void setCorfuRuntime(CorfuRuntime corfuRuntime);
}
