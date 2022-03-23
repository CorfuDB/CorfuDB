package org.corfudb.infrastructure;

import org.corfudb.runtime.CorfuRuntime;

public interface ICompactionTriggerPolicy {
    /**
     * Computes if the compaction jvm should be triggered or not.
     *
     * @return
     */
    public boolean shouldTrigger(long interval);
}
