package org.corfudb.infrastructure.compaction;

import java.util.concurrent.TimeUnit;

public interface TriggerPolicy {

    // Check if should trigger the next compaction cycle
    boolean shouldTrigger();

    // Reset states
    void reset();

    // Interval between each check and record
    TimeUnit getInterval();

    // Return an appropriate safe snapshot
    SafeSnapshot getSafeSnapshot();
}
