package org.corfudb.runtime.collections;

import lombok.Getter;

/**
 * This class wraps CorfuStreamEntries to carry the enqueue time.
 * This time will allow us to collect metrics around the time spent
 * by an entry in the queue before being sent to the stream listener
 * and therefore assess performance.
 *
 * created by @annym on 2021-04-05
 */
public class CorfuStreamQueueEntry {
    @Getter
    private CorfuStreamEntries entry;

    /*
     Represents the time at which the entry was placed into the queue. It will allow to collect metrics
     around the time an entry spends sitting on the queue before being processed.
     */
    @Getter
    private final long enqueueTime;

    public CorfuStreamQueueEntry(CorfuStreamEntries entry, long enqueueTime) {
        this.entry = entry;
        this.enqueueTime = enqueueTime;
    }
}
