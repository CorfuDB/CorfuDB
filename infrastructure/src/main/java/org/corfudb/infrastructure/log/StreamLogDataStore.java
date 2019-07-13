package org.corfudb.infrastructure.log;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IDataStore;
import org.corfudb.infrastructure.IDataStore.KvRecord;
import org.corfudb.runtime.view.Address;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Data access layer for StreamLog.
 * Keeps stream log related meta information: startingAddress and tailSegment.
 * Provides access to the stream log related meta information.
 */
@Builder
@Slf4j
public class StreamLogDataStore {
    private static final String TAIL_SEGMENT_PREFIX = "TAIL_SEGMENT";
    public static final String TAIL_SEGMENT_KEY = "CURRENT";

    public static final String STARTING_ADDRESS_PREFIX = "STARTING_ADDRESS";
    public static final String STARTING_ADDRESS_KEY = "CURRENT";

    public static final KvRecord<Long> TAIL_SEGMENT_RECORD = new KvRecord<>(
            TAIL_SEGMENT_PREFIX, TAIL_SEGMENT_KEY, Long.class
    );

    public static final KvRecord<Long> STARTING_ADDRESS_RECORD = new KvRecord<>(
            STARTING_ADDRESS_PREFIX, STARTING_ADDRESS_KEY, Long.class
    );

    private static final long ZERO_ADDRESS = 0L;

    @NonNull
    private final IDataStore dataStore;

    /**
     * Cached starting address
     */
    private final AtomicLong startingAddress = new AtomicLong(Address.NON_ADDRESS);
    /**
     * Cached tail segmentNumber
     */
    private final AtomicLong tailSegment = new AtomicLong(Address.NON_ADDRESS);

    /**
     * Return current cached tail segmentNumber or get the segmentNumber from the data store if not initialized
     *
     * @return tail segmentNumber
     */
    public long getTailSegment() {
        if (tailSegment.get() == Address.NON_ADDRESS) {
            tailSegment.set(dataStore.get(TAIL_SEGMENT_RECORD, ZERO_ADDRESS));
        }

        return tailSegment.get();
    }

    /**
     * Update current tail segmentNumber in the data store
     *
     * @param newTailSegment updated tail segmentNumber
     */
    public void updateTailSegment(long newTailSegment) {
        if (tailSegment.get() >= newTailSegment) {
            log.trace("New tail segmentNumber less than or equals to the old one: {}. Ignore", newTailSegment);
            return;
        }

        log.debug("Update tail segmentNumber to: {}", newTailSegment);
        dataStore.put(TAIL_SEGMENT_RECORD, newTailSegment);
        tailSegment.set(newTailSegment);
    }

    /**
     * Returns the dataStore starting address.
     *
     * @return the starting address
     */
    public long getStartingAddress() {
        if (startingAddress.get() == Address.NON_ADDRESS) {
            startingAddress.set(dataStore.get(STARTING_ADDRESS_RECORD, ZERO_ADDRESS));
        }

        return startingAddress.get();
    }

    /**
     * Update current starting address in the data store
     *
     * @param newStartingAddress updated starting address
     */
    public void updateStartingAddress(long newStartingAddress) {
        log.info("Update starting address to: {}", newStartingAddress);

        dataStore.put(STARTING_ADDRESS_RECORD, newStartingAddress);
        startingAddress.set(newStartingAddress);
    }

    /**
     * Reset tail segmentNumber
     */
    public void resetTailSegment() {
        log.info("Reset tail segmentNumber. Current segmentNumber: {}", tailSegment.get());
        dataStore.put(TAIL_SEGMENT_RECORD, ZERO_ADDRESS);
        tailSegment.set(ZERO_ADDRESS);
    }

    /**
     * Reset starting address
     */
    public void resetStartingAddress() {
        log.info("Reset starting address. Current address: {}", startingAddress.get());
        dataStore.put(STARTING_ADDRESS_RECORD, ZERO_ADDRESS);
        startingAddress.set(ZERO_ADDRESS);
    }
}
