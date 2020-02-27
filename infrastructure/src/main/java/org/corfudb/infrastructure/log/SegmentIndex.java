package org.corfudb.infrastructure.log;

import lombok.NonNull;

import javax.swing.text.Segment;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by WenbinZhu on 2/10/20.
 */
public class SegmentIndex<T extends AbstractLogSegment> {

    private final StampedLock lock = new StampedLock();

    // Map of the segment ID to segment instance. Value is set to be optional
    // because in case of segment eviction/close we do not remove the mapping
    // but rather set value to be empty so as not to lose track of the segment
    // IDs, otherwise we have to look up in the file system.
    private final NavigableMap<SegmentId, Optional<T>> segmentMap = new TreeMap<>();

    /**
     * Retrieve an existing segment that contains the provided segment ID.
     * If no segment found, create a new one using the supplier and insert
     * into the segment map. This method is thread-safe.
     *
     * @param segmentId       the artificial unmerged ID to locate segment
     * @param segmentSupplier supplier to create new segment if not found
     * @return an existing segment or a new segment created by supplier
     */
    @NonNull
    T getSegmentById(SegmentId segmentId, Function<SegmentId, T> segmentSupplier) {
        Map.Entry<SegmentId, Optional<T>> entry = null;
        long ts = lock.tryOptimisticRead();
        long optimisticTs = ts;

        try {
            // Try optimistic read on the segment map.
            if (ts != 0) {
                entry = segmentMap.floorEntry(segmentId);
            }
            // Acquire read lock if optimistic read failed.
            if (!lock.validate(ts)) {
                ts = lock.readLock();
                entry = segmentMap.floorEntry(segmentId);
            }

            // If there is a segment containing the segment ID, return it.
            if (entry != null && entry.getValue().isPresent()) {
                T segment = entry.getValue().get();
                if (segment.getSegmentId().encloses(segmentId)) {
                    return segment;
                }
            }

            // No segment found, acquire write lock and may insert a new one.
            ts = lock.tryConvertToWriteLock(ts);
            if (ts == 0) {
                ts = lock.writeLock();
            }
            // Entry could exist before the write lock is acquired.
            entry = segmentMap.floorEntry(segmentId);
            if (entry != null && entry.getValue().isPresent()) {
                T segment = entry.getValue().get();
                if (segment.getSegmentId().encloses(segmentId)) {
                    return segment;
                }
            } else if (entry != null) {
                // Segment was set to empty, reuse this segment ID as this
                // represents an existing segment which was evicted/closed.
                segmentId = entry.getKey();
            }

            // Create new segment based on the new unmerged segment ID or
            // and existing segment ID.
            // TODO: evict segment if number of entries overflow
            T newSegment = segmentSupplier.apply(segmentId);
            segmentMap.put(segmentId, Optional.of(newSegment));
            return newSegment;

        } finally {
            // Unlock would throw an exception if ts is
            // not returned by a lock operation.
            if (ts != optimisticTs) {
                lock.unlock(ts);
            }
        }
    }

    /**
     * Close and evict the segment represented by the provided
     * segment ID if it is present in the segment map.
     *
     * @param segmentId the ID of the segment to close
     */
    void invalidate(SegmentId segmentId) {
        long ts = lock.writeLock();
        try {
            Optional<T> segment = segmentMap.get(segmentId);
            if (segment != null && segment.isPresent()) {
                segmentMap.put(segmentId, Optional.empty());
                segment.get().close(true);
            }
        } finally {
            lock.unlockWrite(ts);
        }
    }

    /**
     * Close and evict all the segments. If this method is called,
     * a scan of the file system is required to rebuild the index.
     */
    void invalidateAll() {
        long ts = lock.writeLock();
        try {
            segmentMap.forEach((sid, segment) -> {
                segment.ifPresent(seg -> seg.close(true));
            });
            segmentMap.clear();
        } finally {
            lock.unlockWrite(ts);
        }
    }
}
