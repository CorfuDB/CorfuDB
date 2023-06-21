package org.corfudb.infrastructure;

import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.collections.CorfuStore;

public interface CompactionTriggerPolicy {
    boolean shouldTrigger(long interval, CorfuStore corfuStore, DistributedCheckpointerHelper distributedCheckpointerHelper) throws Exception;

    void markCompactionCycleStart();
}
