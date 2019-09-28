package org.corfudb.infrastructure.log;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.datastore.KvDataStore;
import org.corfudb.infrastructure.datastore.KvDataStore.KvRecord;
import org.corfudb.runtime.view.Address;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;

/**
 * Data access layer for StreamLog.
 * Keeps stream log related meta information: head segment,
 * tail segment and global compaction mark.
 * Provides access to the stream log related meta information.
 */
@Slf4j
public class StreamLogDataStore {
    private static final String TAIL_SEGMENT_PREFIX = "TAIL_SEGMENT";
    private static final String TAIL_SEGMENT_KEY = "CURRENT";

    private static final String HEAD_SEGMENT_PREFIX = "HEAD_SEGMENT";
    private static final String HEAD_SEGMENT_KEY = "CURRENT";

    private static final String COMPACTION_MARK_PREFIX = "COMPACTION_MARK";
    private static final String COMPACTION_MARK_KEY = "CURRENT";

    private static final String COMMITTED_TAIL_PREFIX = "COMMITTED_TAIL";
    private static final String COMMITTED_TAIL_KEY = "CURRENT";

    private static final KvRecord<Long> TAIL_SEGMENT_RECORD = new KvRecord<>(
            TAIL_SEGMENT_PREFIX, TAIL_SEGMENT_KEY, Long.class
    );

    private static final KvRecord<Long> HEAD_SEGMENT_RECORD = new KvRecord<>(
            HEAD_SEGMENT_PREFIX, HEAD_SEGMENT_KEY, Long.class
    );

    private static final KvRecord<Long> COMPACTION_MARK_RECORD = new KvRecord<>(
            COMPACTION_MARK_PREFIX, COMPACTION_MARK_KEY, Long.class
    );

    private static final KvRecord<Long> COMMITTED_TAIL_RECORD = new KvRecord<>(
            COMMITTED_TAIL_PREFIX, COMMITTED_TAIL_KEY, Long.class);

    private static final long ZERO_ADDRESS = 0L;

    @NonNull
    private final KvDataStore dataStore;

    /**
     * Cached starting address.
     */
    private final AtomicLong headSegment;

    /**
     * Cached tail segment.
     */
    private final AtomicLong tailSegment;

    /**
     * Cached global compaction mark.
     * Any snapshot read before this may result in in-complete history.
     */
    private final AtomicLong globalCompactionMark;

    /**
     * Cached committed log tail, up to which the log is consolidated.
     */
    private final AtomicLong committedTail;

    public StreamLogDataStore(KvDataStore dataStore) {
        this.dataStore = dataStore;
        this.headSegment =
                new AtomicLong(dataStore.get(HEAD_SEGMENT_RECORD, Address.MAX));
        this.tailSegment =
                new AtomicLong(dataStore.get(TAIL_SEGMENT_RECORD, ZERO_ADDRESS));
        this.globalCompactionMark =
                new AtomicLong(dataStore.get(COMPACTION_MARK_RECORD, Address.NON_ADDRESS));
        this.committedTail =
                new AtomicLong(dataStore.get(COMMITTED_TAIL_RECORD, Address.NON_ADDRESS));
    }

    /**
     * Return the current tail segment.
     *
     * @return tail segment
     */
    long getTailSegment() {
        return tailSegment.get();
    }

    /**
     * Update current tail segment in the data store.
     *
     * @param newTailSegment new tail segment to update
     */
    void updateTailSegment(long newTailSegment) {
        if (update(TAIL_SEGMENT_RECORD, tailSegment, newTailSegment, (curr, next) -> next > curr)) {
            log.debug("Updated tail segment to: {}", newTailSegment);
        } else {
            log.trace("New tail segment {} not greater than current, ignore.", newTailSegment);
        }
    }

    /**
     * Reset the current tail segment.
     */
    void resetTailSegment() {
        log.info("Resetting tail segment. Current segment: {}", tailSegment.get());
        tailSegment.updateAndGet(tail -> {
            dataStore.put(TAIL_SEGMENT_RECORD, ZERO_ADDRESS);
            return ZERO_ADDRESS;
        });
    }

    /**
     * Return the current head segment.
     *
     * @return the starting address
     */
    long getHeadSegment() {
        return headSegment.get();
    }

    /**
     * Update current head segment in the data store.
     *
     * @param newHeadSegment new head segment to update
     */
    void updateHeadSegment(long newHeadSegment) {
        if (update(HEAD_SEGMENT_RECORD, headSegment, newHeadSegment, (curr, next) -> next < curr)) {
            log.debug("Updated head segment to: {}", newHeadSegment);
        } else {
            log.trace("New head segment {} not smaller than current, ignore.", newHeadSegment);
        }
    }

    /**
     * Reset head segment.
     */
    void resetHeadSegment() {
        log.info("Resetting head segment. Current head segment: {}", headSegment.get());
        headSegment.updateAndGet(addr -> {
            dataStore.put(HEAD_SEGMENT_RECORD, Address.MAX);
            return Address.MAX;
        });
    }

    /**
     * Return the current global compaction mark.
     */
    long getGlobalCompactionMark() {
        return globalCompactionMark.get();
    }

    /**
     * Update the current global compaction mark if the provided address is greater.
     *
     * @param newCompactionMark a new address to update current global compaction mark
     */
    void updateGlobalCompactionMark(long newCompactionMark) {
        if (update(COMPACTION_MARK_RECORD, globalCompactionMark, newCompactionMark, (curr, next) -> next > curr)) {
            log.debug("Updated global compaction mark to: {}", newCompactionMark);
        } else {
            log.trace("New global compaction mark {} not greater than current, ignore.", newCompactionMark);
        }
    }

    /**
     * Reset global compaction mark.
     */
    void resetGlobalCompactionMark() {
        log.info("Resetting global compaction mark. Current: {}", globalCompactionMark.get());
        globalCompactionMark.updateAndGet(addr -> {
            dataStore.put(COMPACTION_MARK_RECORD, Address.NON_ADDRESS);
            return Address.NON_ADDRESS;
        });
    }

    /**
     * Return the current committed log tail.
     */
    long getCommittedTail() {
        return committedTail.get();
    }

    /**
     * Update the current committed log tail if the provided tail is greater.
     *
     * @param newCommittedTail a new address to update current global committed tail
     */
    void updateCommittedTail(long newCommittedTail) {
        if (update(COMMITTED_TAIL_RECORD, committedTail, newCommittedTail, (curr, next) -> next > curr)) {
            log.debug("Updated committed log tail to: {}", newCommittedTail);
        } else {
            log.trace("New committed log tail {} not greater than current, ignore.", newCommittedTail);
        }
    }

    /**
     * Reset committed log tail.
     */
    void resetCommittedTail() {
        log.info("Resetting committed log tail. Current: {}", committedTail.get());
        committedTail.updateAndGet(addr -> {
            dataStore.put(COMMITTED_TAIL_RECORD, Address.NON_ADDRESS);
            return Address.NON_ADDRESS;
        });
    }

    private boolean update(KvRecord<Long> key,
                           AtomicLong current, long newValue,
                           BiPredicate<Long, Long> predicate) {
        AtomicBoolean updated = new AtomicBoolean(false);

        current.updateAndGet(curr -> {
            if (!predicate.test(curr, newValue)) {
                return curr;
            }
            updated.set(true);
            dataStore.put(key, newValue);
            return newValue;
        });

        return updated.get();
    }
}
