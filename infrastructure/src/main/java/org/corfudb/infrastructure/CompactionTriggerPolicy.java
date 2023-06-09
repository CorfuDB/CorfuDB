package org.corfudb.infrastructure;

import org.corfudb.runtime.collections.CorfuStore;

public interface CompactionTriggerPolicy {
    boolean shouldTrigger(long interval, CorfuStore corfuStore) throws Exception;

    void markCompactionCycleStart();
}
