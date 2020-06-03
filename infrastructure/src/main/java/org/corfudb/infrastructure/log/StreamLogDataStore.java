package org.corfudb.infrastructure.log;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.datastore.KvDataStore;
import org.corfudb.infrastructure.datastore.KvDataStore.KvRecord;
import org.corfudb.runtime.view.Address;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Data access layer for StreamLog.
 * <p>
 * Keeps stream log related meta information: startingAddress and tailSegment.
 * Provides access to the stream log related meta information.
 */
@Slf4j
public class StreamLogDataStore {
    private static final String TAIL_SEGMENT_PREFIX = "TAIL_SEGMENT";
    private static final String TAIL_SEGMENT_KEY = "CURRENT";

    private static final String STARTING_ADDRESS_PREFIX = "STARTING_ADDRESS";
    private static final String STARTING_ADDRESS_KEY = "CURRENT";

    private static final String COMMITTED_TAIL_PREFIX = "COMMITTED_TAIL";
    private static final String COMMITTED_TAIL_KEY = "CURRENT";

    private static final KvRecord<Long> TAIL_SEGMENT_RECORD = new KvRecord<>(
            TAIL_SEGMENT_PREFIX, TAIL_SEGMENT_KEY, Long.class
    );

    private static final KvRecord<Long> STARTING_ADDRESS_RECORD = new KvRecord<>(
            STARTING_ADDRESS_PREFIX, STARTING_ADDRESS_KEY, Long.class
    );

    private static final KvRecord<Long> COMMITTED_TAIL_RECORD = new KvRecord<>(
            COMMITTED_TAIL_PREFIX, COMMITTED_TAIL_KEY, Long.class
    );

    private static final long ZERO_ADDRESS = 0L;

    @NonNull
    private final KvDataStore dataStore;

    /**
     * Cached starting address.
     */
    private final AtomicLong startingAddress;

    /**
     * Cached tail segment.
     */
    private final AtomicLong tailSegment;

    /**
     * Cached committed log tail, up to which the log is consolidated.
     */
    private final AtomicLong committedTail;

    public StreamLogDataStore(KvDataStore dataStore) {
        this.dataStore = dataStore;
        this.startingAddress = new AtomicLong(dataStore.get(STARTING_ADDRESS_RECORD, ZERO_ADDRESS));
        this.tailSegment = new AtomicLong(dataStore.get(TAIL_SEGMENT_RECORD, ZERO_ADDRESS));
        this.committedTail = new AtomicLong(dataStore.get(COMMITTED_TAIL_RECORD, Address.NON_ADDRESS));
    }

    /**
     * Return current cached tail segment or get the segment from the data store if not initialized
     *
     * @return tail segment
     */
    public long getTailSegment() {
        return tailSegment.get();
    }

    /**
     * Update current tail segment in the data store.
     * <p>
     * WARNING: this method is not thread safe, it is supposed to be called
     * by operations executed in batch processor to achieve thread safety.
     *
     * @param newTailSegment updated tail segment
     */
    public void updateTailSegment(long newTailSegment) {
        if (tailSegment.get() >= newTailSegment) {
            log.trace("New tail segment {} less than or equals to the old one. Ignore", newTailSegment);
            return;
        }

        log.debug("Update tail segment to: {}", newTailSegment);
        dataStore.put(TAIL_SEGMENT_RECORD, newTailSegment);
        tailSegment.set(newTailSegment);
    }

    /**
     * Returns the dataStore starting address.
     *
     * @return the starting address
     */
    public long getStartingAddress() {
        return startingAddress.get();
    }

    /**
     * Update current starting address in the data store.
     * <p>
     * WARNING: this method is not thread safe, it is supposed to be called
     * by operations executed in batch processor to achieve thread safety.
     *
     * @param newStartingAddress updated starting address
     */
    public void updateStartingAddress(long newStartingAddress) {
        if (startingAddress.get() >= newStartingAddress) {
            log.trace("New starting address {} less than or equals to the old one. Ignore", newStartingAddress);
            return;
        }

        log.info("Update starting address to: {}", newStartingAddress);
        dataStore.put(STARTING_ADDRESS_RECORD, newStartingAddress);
        startingAddress.set(newStartingAddress);
    }

    /**
     * Return the current committed log tail.
     */
    public long getCommittedTail() {
        return committedTail.get();
    }

    /**
     * Update the current committed log tail if the provided tail is greater.
     * <p>
     * This method is synchronized to prevent concurrent updates that could
     * cause the cached committed tail and the disk value inconsistent. Note
     * that in this case atomic operation could not help. The alternative is
     * to do the update through batch processor which is single threaded, but
     * doing that could be less efficient.
     *
     * @param newCommittedTail a new address to update current global committed tail
     */
    public synchronized void updateCommittedTail(long newCommittedTail) {
        if (committedTail.get() >= newCommittedTail) {
            log.trace("New committed tail {} less than or equals to the old one. Ignore", newCommittedTail);
            return;
        }

        log.debug("Update committed tail to: {}", newCommittedTail);
        dataStore.put(COMMITTED_TAIL_RECORD, newCommittedTail);
        committedTail.set(newCommittedTail);
    }

    /**
     * Reset tail segment.
     */
    public void resetTailSegment() {
        log.info("Reset tail segment. Current segment: {}", tailSegment.get());
        dataStore.put(TAIL_SEGMENT_RECORD, ZERO_ADDRESS);
        tailSegment.set(ZERO_ADDRESS);
    }

    /**
     * Reset starting address.
     */
    public void resetStartingAddress() {
        log.info("Reset starting address. Current address: {}", startingAddress.get());
        dataStore.put(STARTING_ADDRESS_RECORD, ZERO_ADDRESS);
        startingAddress.set(ZERO_ADDRESS);
    }

    /**
     * Reset committed log tail.
     */
    public synchronized void resetCommittedTail() {
        log.info("Reset committed tail. Current committed tail: {}", committedTail.get());
        dataStore.put(COMMITTED_TAIL_RECORD, Address.NON_ADDRESS);
        committedTail.set(Address.NON_ADDRESS);
    }
}
