package org.corfudb.infrastructure.log;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IDataStore;
import org.corfudb.infrastructure.IDataStore.KvRecord;
import org.corfudb.runtime.view.Address;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Data access layer for StreamLog.
 * Keeps stream log related meta information: startingAddress, tailSegment
 * and per-segment per-stream compaction marks.
 * Provides access to the stream log related meta information.
 */
@Slf4j
@RequiredArgsConstructor
public class StreamLogDataStore {
    private static final String TAIL_SEGMENT_PREFIX = "TAIL_SEGMENT";
    private static final String TAIL_SEGMENT_KEY = "CURRENT";

    private static final String STARTING_ADDRESS_PREFIX = "STARTING_ADDRESS";
    private static final String STARTING_ADDRESS_KEY = "CURRENT";

    private static final String COMPACTION_MARK_PREFIX = "COMPACTION_MARK";
    private static final String COMPACTION_MARK_KEY = "CURRENT";

    private static final KvRecord<Long> TAIL_SEGMENT_RECORD = new KvRecord<>(
            TAIL_SEGMENT_PREFIX, TAIL_SEGMENT_KEY, Long.class
    );

    private static final KvRecord<Long> STARTING_ADDRESS_RECORD = new KvRecord<>(
            STARTING_ADDRESS_PREFIX, STARTING_ADDRESS_KEY, Long.class
    );

    private static final KvRecord<Map> COMPACTION_MARK_RECORD = new KvRecord<>(
            COMPACTION_MARK_PREFIX, COMPACTION_MARK_KEY, Map.class
    );

    private static final long ZERO_ADDRESS = 0L;

    @NonNull
    private final IDataStore dataStore;

    /**
     * Cached starting address
     */
    private final AtomicLong startingAddress = new AtomicLong(Address.NON_ADDRESS);
    /**
     * Cached tail segment
     */
    private final AtomicLong tailSegment = new AtomicLong(Address.NON_ADDRESS);
    /**
     * Cached stream compaction marks. Any snapshot read on the stream before
     * compaction mark may result in in-complete history.
     */
    private final AtomicReference<Map<UUID, Long>> streamCompactionMarks = new AtomicReference<>(null);

    /**
     * Return current cached tail segment or get the segment from the data store if not initialized
     *
     * @return tail segment
     */
    long getTailSegment() {
        if (tailSegment.get() == Address.NON_ADDRESS) {
            tailSegment.set(dataStore.get(TAIL_SEGMENT_RECORD, ZERO_ADDRESS));
        }

        return tailSegment.get();
    }

    /**
     * Update current tail segment in the data store
     *
     * @param newTailSegment updated tail segment
     */
    void updateTailSegment(long newTailSegment) {
        if (tailSegment.get() >= newTailSegment) {
            log.trace("New tail segment less than or equals to the old one: {}. Ignore", newTailSegment);
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
    long getStartingAddress() {
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
    void updateStartingAddress(long newStartingAddress) {
        log.info("Update starting address to: {}", newStartingAddress);

        dataStore.put(STARTING_ADDRESS_RECORD, newStartingAddress);
        startingAddress.set(newStartingAddress);
    }

    /**
     * Reset tail segment
     */
    void resetTailSegment() {
        log.info("Reset tail segment. Current segment: {}", tailSegment.get());
        dataStore.put(TAIL_SEGMENT_RECORD, ZERO_ADDRESS);
        tailSegment.set(ZERO_ADDRESS);
    }

    /**
     * Reset starting address
     */
    void resetStartingAddress() {
        log.info("Reset starting address. Current address: {}", startingAddress.get());
        dataStore.put(STARTING_ADDRESS_RECORD, ZERO_ADDRESS);
        startingAddress.set(ZERO_ADDRESS);
    }

    /**
     * Update current stream compaction marks with a subset of compaction marks.
     *
     * @param compactionMarks a subset of compaction marks to update
     */
    void updateCompactionMarks(Map<UUID, Long> compactionMarks) {
        log.debug("Updating stream compaction mark, current: {}", streamCompactionMarks.get());
        streamCompactionMarks.updateAndGet(cm -> {
            Map<UUID, Long> newCompactionMarks = new HashMap<>(cm);
            compactionMarks.forEach((id, addr) -> newCompactionMarks.merge(id, addr, Math::max));
            dataStore.put(COMPACTION_MARK_RECORD, newCompactionMarks);
            return newCompactionMarks;
        });
        log.debug("Updated stream compaction mark to: {}", streamCompactionMarks.get());
    }

    /**
     * Return current stream compaction marks.
     */
    @SuppressWarnings("unchecked")
    Map<UUID, Long> getCompactionMarks() {
        if (streamCompactionMarks.get() == null) {
            streamCompactionMarks.getAndUpdate(
                    cm -> dataStore.get(COMPACTION_MARK_RECORD, Collections.emptyMap()));
        }

        return streamCompactionMarks.get();
    }

    /**
     * Reset stream compaction marks.
     */
    void resetCompactionMarks() {
        log.info("Reset stream compaction marks.");
        dataStore.put(COMPACTION_MARK_RECORD, Collections.emptyMap());
        streamCompactionMarks.set(Collections.emptyMap());
    }
}
