package org.corfudb.infrastructure.log;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.datastore.KvDataStore;
import org.corfudb.infrastructure.datastore.KvDataStore.KvRecord;
import org.apache.commons.io.IOUtils;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.view.Address;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data access layer for StreamLog.
 * Keeps stream log related meta information: starting address,
 * tail segment and global compaction mark.
 * Provides access to the stream log related meta information.
 */
@Slf4j
public class StreamLogDataStore {
    private static final String TAIL_SEGMENT_PREFIX = "TAIL_SEGMENT";
    private static final String TAIL_SEGMENT_KEY = "CURRENT";

    private static final String STARTING_ADDRESS_PREFIX = "STARTING_ADDRESS";
    private static final String STARTING_ADDRESS_KEY = "CURRENT";

    private static final String COMPACTION_MARK_PREFIX = "COMPACTION_MARK";
    private static final String COMPACTION_MARK_KEY = "CURRENT";

    private static final String COMPACTED_ADDRESSES_PREFIX = "COMPACTED_ADDRESSES";

    private static final KvRecord<Long> TAIL_SEGMENT_RECORD = new KvRecord<>(
            TAIL_SEGMENT_PREFIX, TAIL_SEGMENT_KEY, Long.class
    );

    private static final KvRecord<Long> STARTING_ADDRESS_RECORD = new KvRecord<>(
            STARTING_ADDRESS_PREFIX, STARTING_ADDRESS_KEY, Long.class
    );

    private static final KvRecord<Long> COMPACTION_MARK_RECORD = new KvRecord<>(
            COMPACTION_MARK_PREFIX, COMPACTION_MARK_KEY, Long.class
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
     * Cached global compaction mark.
     * Any snapshot read before this may result in in-complete history.
     */
    private final AtomicLong globalCompactionMark;


    public StreamLogDataStore(KvDataStore dataStore) {
        this.dataStore = dataStore;
        this.startingAddress =
                new AtomicLong(dataStore.get(STARTING_ADDRESS_RECORD, ZERO_ADDRESS));
        this.tailSegment =
                new AtomicLong(dataStore.get(TAIL_SEGMENT_RECORD, ZERO_ADDRESS));
        this.globalCompactionMark =
                new AtomicLong(dataStore.get(COMPACTION_MARK_RECORD, Address.NON_ADDRESS));
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
        if (updateIfGreater(tailSegment, newTailSegment, TAIL_SEGMENT_RECORD)) {
            log.debug("Updated tail segment to: {}", newTailSegment);
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
     * Return the current starting address.
     *
     * @return the starting address
     */
    long getStartingAddress() {
        return startingAddress.get();
    }

    /**
     * Update current starting address in the data store.
     *
     * @param newStartingAddress updated starting address
     */
    void updateStartingAddress(long newStartingAddress) {
        if (updateIfGreater(startingAddress, newStartingAddress, STARTING_ADDRESS_RECORD)) {
            log.debug("Updated starting address to: {}", newStartingAddress);
        }
    }

    /**
     * Reset starting address.
     */
    void resetStartingAddress() {
        log.info("Resetting starting address. Current address: {}", startingAddress.get());
        startingAddress.updateAndGet(addr -> {
            dataStore.put(STARTING_ADDRESS_RECORD, ZERO_ADDRESS);
            return ZERO_ADDRESS;
        });
    }

    /**
     * Return the current global compaction marks.
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
        if (updateIfGreater(globalCompactionMark, newCompactionMark, COMPACTION_MARK_RECORD)) {
            log.debug("Updated global compaction mark to: {}", newCompactionMark);
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
     * Set the compacted addresses bitmap.
     *
     * @param newBitmap new compacted addresses bitmap.
     */
    void updateCompactedAddresses(long segment, Roaring64NavigableMap newBitmap) {
        Roaring64NavigableMap currBitmap = getCompactedAddresses(segment);
        newBitmap.or(currBitmap);

        newBitmap.runOptimize();
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(byteStream);

        try {
            newBitmap.serialize(output);
            ByteBuffer buffer = ByteBuffer.wrap(byteStream.toByteArray());
            dataStore.put(getCompactedAddressesRecord(segment), buffer);
        } catch (IOException ioe) {
            throw new RuntimeException("Exception when attempting to " +
                    "serialize compacted addresses bitmap", ioe);
        } finally {
            IOUtils.closeQuietly(byteStream);
        }
    }

    /**
     * Get the current compacted addresses bitmap.
     * Caching is done on upper layer.
     */
    Roaring64NavigableMap getCompactedAddresses(long segment) {
        ByteBuffer buf = dataStore.get(getCompactedAddressesRecord(segment));
        if (buf == null) {
            return new Roaring64NavigableMap();
        }

        // Cannot use flip() in case the buf is cached since flip() would
        // change limit to position, but position is 0 if cached.
        buf.rewind();
        ByteArrayInputStream byteStream = new ByteArrayInputStream(
                buf.array(), buf.position(), buf.remaining());
        DataInput input = new DataInputStream(byteStream);

        try {
            Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
            bitmap.deserialize(input);
            return bitmap;
        } catch (IOException ioe) {
            throw new SerializerException("Exception when attempting to " +
                    "deserialize compacted addresses bitmap.", ioe);
        } finally {
            IOUtils.closeQuietly(byteStream);
        }
    }

    void resetAllCompactedAddresses() {
        dataStore.deleteFiles(file -> file.getName().startsWith(COMPACTED_ADDRESSES_PREFIX));
    }

    private KvRecord<ByteBuffer> getCompactedAddressesRecord(long segment) {
        return new KvRecord<>(COMPACTED_ADDRESSES_PREFIX, String.valueOf(segment), ByteBuffer.class);
    }

    private boolean updateIfGreater(AtomicLong target, long newAddress, KvRecord<Long> key) {
        long updatedAddress = target.updateAndGet(curr -> {
            if (newAddress <= curr) {
                return curr;
            }
            dataStore.put(key, newAddress);
            return newAddress;
        });

        return updatedAddress == newAddress;
    }
}
