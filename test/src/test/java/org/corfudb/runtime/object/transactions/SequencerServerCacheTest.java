package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.SequencerServerCache;
import org.corfudb.infrastructure.SequencerServerCache.ConflictTxStream;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.view.Address;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Created by maithem on 7/24/17.
 */
@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class SequencerServerCacheTest extends AbstractObjectTest {

    public static final int entryPerAddress = 20;
    public static final int iterations = 100;
    public static final int cacheSize = iterations * entryPerAddress;
    public static final int numRemains = 10;

    private static final String TEST_STREAM = "test";

    private void generateData(@NonNull Map<ConflictTxStream, Long> recordMap, @NonNull SequencerServerCache cache,
                              long numEntries, long address, boolean verify) {
        final UUID streamId = UUID.randomUUID();
        generateData(recordMap, cache, streamId, 0, numEntries, address, verify);
    }

    private void generateData(@NonNull Map<ConflictTxStream, Long> recordMap, @NonNull SequencerServerCache cache,
                              @NonNull UUID streamId, long numEntries, long address, boolean verify) {
        generateData(recordMap, cache, streamId, 0, numEntries, address, verify);
    }

    /**
     * Generate data with given address and verify that the entries with firstAddress are correctly evicted.
     */
    private void generateData(@NonNull Map<ConflictTxStream, Long> recordMap, @NonNull SequencerServerCache cache,
                              @NonNull UUID streamId, int offSet, long numEntries, long address, boolean verify) {
        final List<ConflictTxStream> entriesToAdd = new ArrayList<>();
        final long initialSmallestAddress = cache.firstAddress();
        final long initialSize = cache.size();

        for (int i = offSet; i < offSet + numEntries; i++) {
            entriesToAdd.add(new ConflictTxStream(streamId, intToByteArray(i)));
        }

        cache.put(entriesToAdd, address);

        if (verify && cache.size() <= initialSize) {
            // Verify that the smallest cache entry has been evicted.
            log.debug("cache.firstAddress: " + cache.firstAddress() + " cacheSize: " + cache.size() + " address:" + address);
            assertThat(initialSmallestAddress).isLessThan(cache.firstAddress());
        }

        // Verify that the entry is now present in the cache.
        entriesToAdd.forEach(conflictKey -> {
            assertThat(cache.get(conflictKey)).isNotEqualTo(Address.NON_ADDRESS);
            recordMap.put(conflictKey, address);
        });

        // Verify that the cache size is not larger than the maximum capacity.
        assertThat(cache.size()).isLessThanOrEqualTo(cache.getCapacity());
    }

    /**
     * Verify cache contains all the data in recordMap and that each address >= firstAddress.
     */
    private void verifyData(@NonNull Map<ConflictTxStream, Long> recordMap, @NonNull SequencerServerCache cache) {
        for (Map.Entry<ConflictTxStream, Long> entry : recordMap.entrySet()) {
            final long expectedAddress = entry.getValue();
            assertThat(expectedAddress).isGreaterThanOrEqualTo(cache.firstAddress());
            log.debug("address " + cache.get(entry.getKey()) + " expected " + expectedAddress);
            assertThat(cache.get(entry.getKey())).isEqualTo(expectedAddress);
        }
    }

    /**
     * Test that adding the same conflict key into the cache does not pollute
     * it with stale entries.
     */
    @Test
    public void testSequencerCacheSameKey() {
        getDefaultRuntime();

        Map<Integer, Integer> map = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {
                })
                .setStreamName(TEST_STREAM)
                .open();

        final int key = 0xBEEF;

        for (int x = 0; x < cacheSize; x++) {
            getRuntime().getObjectsView().TXBegin();
            map.put(key, x);
            getRuntime().getObjectsView().TXEnd();
        }

        SequencerServerCache cache = getSequencer(0).getCache();
        assertThat(cache.size()).isEqualTo(1);
    }

    /**
     * Test that the cache behaves correctly under trim operations.
     */
    @Test
    public void testSequencerCacheTrim() {
        getDefaultRuntime();

        Map<Integer, Integer> map = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {
                })
                .setStreamName(TEST_STREAM)
                .open();

        final int numTxn = 500;
        final Token trimAddress = new Token(getDefaultRuntime().getLayoutView().getLayout().getEpoch(), 250);

        for (int x = 0; x < numTxn; x++) {
            getRuntime().getObjectsView().TXBegin();
            map.put(x, x);
            getRuntime().getObjectsView().TXEnd();
        }

        SequencerServerCache cache = getSequencer(0).getCache();
        assertThat(cache.size()).isEqualTo(numTxn);

        getDefaultRuntime().getAddressSpaceView().prefixTrim(trimAddress);
        // Since the addressSpace only sends a hint to the sequencer, its possible
        // that the method returns before the sequencer receives the trim request,
        // therefore it must be directly invoked to wait for the future.
        getDefaultRuntime().getLayoutView().getRuntimeLayout()
                .getPrimarySequencerClient()
                .trimCache(trimAddress.getSequence()).join();
        assertThat(cache.size()).isEqualTo((int) trimAddress.getSequence());
    }

    /**
     * Check cache eviction algorithm (it must be atomic operation).
     */
    @Test
    public void testCacheBasicEvict() {
        SequencerServerCache cache = new SequencerServerCache(1, Address.NOT_FOUND);
        final int iterations = 10;
        final ConflictTxStream firstKey = new ConflictTxStream(UUID.randomUUID(), new byte[]{});
        final ConflictTxStream secondKey = new ConflictTxStream(UUID.randomUUID(), new byte[]{});

        for (int i = 0; i < iterations; i++) {
            cache.put(Collections.singletonList(firstKey), 2*i);
            cache.put(Collections.singletonList(secondKey), 2*i + 1);

            assertThat(cache.size()).isOne();
            assertThat(cache.get(firstKey)).isEqualTo(Address.NON_ADDRESS);
            assertThat(cache.get(secondKey)).isEqualTo(2*i + 1);
        }
    }

    /**
     * Check that an exception is thrown when trying to put a conflict key with version
     * that is not currently in the cache, but is smaller than the smallest version present.
     */
    @Test
    public void testCachePutVersionSmallerThanSmallest() {
        SequencerServerCache cache = new SequencerServerCache(1, Address.NOT_FOUND);
        final ConflictTxStream firstKey = new ConflictTxStream(UUID.randomUUID(), new byte[]{});
        final ConflictTxStream secondKey = new ConflictTxStream(UUID.randomUUID(), new byte[]{});

        cache.put(Collections.singletonList(firstKey), 0);
        cache.put(Collections.singletonList(secondKey), 1);

        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get(secondKey)).isEqualTo(1);

        assertThrows(IllegalStateException.class, () ->
                cache.put(Collections.singletonList(firstKey), 0));

        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get(secondKey)).isEqualTo(1);
    }

    /**
     * Test that performing a put with a conflict key that is already present
     * but having a smaller version throws an exception. The cache should also
     * remain unmodified.
     */
    @Test
    public void testSequencerPutWithRegression() {
        final UUID streamId = UUID.randomUUID();
        final int capacity = 10;

        SequencerServerCache cache = new SequencerServerCache(capacity, Address.NOT_FOUND);
        Map<ConflictTxStream, Long> recordMap = new HashMap<>();
        for (int i = 0; i < capacity; i++) {
            generateData(recordMap, cache, 1, i, false);
        }

        verifyData(recordMap, cache);

        List<ConflictTxStream> entry = Collections.singletonList(
                new ConflictTxStream(streamId, intToByteArray(capacity - 1)));

        assertThrows(IllegalStateException.class, () -> cache.put(entry, capacity - 2));
        verifyData(recordMap, cache);
    }

    /**
     * Test that performing a put on with conflict key for a version that was already cached
     * throws an exception. The cache should also remain unmodified.
     */
    @Test
    public void testSequencerPutSameVersion() {
        final UUID streamId = UUID.randomUUID();
        final int capacity = 10;

        SequencerServerCache cache = new SequencerServerCache(capacity, Address.NOT_FOUND);
        Map<ConflictTxStream, Long> recordMap = new HashMap<>();
        for (int i = 0; i < capacity; i++) {
            generateData(recordMap, cache, 1, i, false);
        }

        verifyData(recordMap, cache);

        List<ConflictTxStream> entry = Collections.singletonList(
                new ConflictTxStream(streamId, intToByteArray(capacity)));

        assertThrows(IllegalStateException.class, () -> cache.put(entry, capacity - 1));
        verifyData(recordMap, cache);
    }

    /**
     * Test that performing a put on with more conflict keys than the capacity of
     * the cache throws an exception.
     */
    @Test
    public void testSequencerPutLargerThanCapacity() {
        SequencerServerCache cache = new SequencerServerCache(1, Address.NOT_FOUND);
        final ConflictTxStream firstKey = new ConflictTxStream(UUID.randomUUID(), new byte[]{});
        final ConflictTxStream secondKey = new ConflictTxStream(UUID.randomUUID(), new byte[]{});

        assertThrows(IllegalStateException.class,
                () -> cache.put(Arrays.asList(firstKey, secondKey), 0));
    }

    /**
     * Test that cache eviction does not evict incorrect conflict keys.
     */
    @Test
    public void testSequencerEvictLoss() {
        final UUID streamId = UUID.randomUUID();
        final int capacity = 3;

        SequencerServerCache cache = new SequencerServerCache(capacity, Address.NOT_FOUND);
        Map<ConflictTxStream, Long> recordMap = new HashMap<>();

        // TX1: ck1 and ck2 for v1
        generateData(recordMap, cache, streamId, 2, 0, false);

        // TX2: ck1 for v2
        generateData(recordMap, cache, streamId, 1, 1, false);

        // TX3: ck3 and ck4 for v3 - This should trigger an eviction.
        generateData(recordMap, cache, streamId, 2, 2, 2, false);

        // Validate that (ck2, v1) was evicted.
        ConflictTxStream evicted = recordMap.entrySet().stream()
                .filter(entry -> entry.getValue() == 0)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList()).get(0);

        assertThat(cache.get(evicted)).isEqualTo(Address.NON_ADDRESS);

        recordMap = recordMap.entrySet().stream()
                .filter(entry -> entry.getValue() > 0)
                .collect(Collectors.<Map.Entry<ConflictTxStream, Long>, ConflictTxStream, Long>toMap(Map.Entry::getKey, Map.Entry::getValue));

        verifyData(recordMap, cache);
    }

    /**
     * Test the eviction of firstAddress while the cache is full.
     */
    @Test
    public void testSequencerCacheEvict() {
        SequencerServerCache cache = new SequencerServerCache(cacheSize, Address.NOT_FOUND);
        long address = 0;
        Map<ConflictTxStream, Long> recordMap = new HashMap<>();

        // put entries to the cache, make it full, some entries have the same address
        for (int i = 0; i < iterations; i++) {
            generateData(recordMap, cache, entryPerAddress, address++, false);
        }

        verifyData(recordMap, cache);

        assertThat(cache.size()).isEqualTo(cacheSize);
        // Each put should evict all streams with the same address
        for (int i = 0; i < iterations; i++) {
            generateData(recordMap, cache, 1, address++, true);
        }

        // Update the recordMap to remove entries that are no longer expected to be there.
        // All versions from 5 and onward should still be present, since we added a total of
        // iteration entries into the cache. As a result, we should have evicted the first
        // iterations / entryPerAddress versions (i.e. addresses 0 through 4).
        recordMap = recordMap.entrySet().stream()
                .filter(entry -> entry.getValue() >= 5)
                .collect(Collectors.<Map.Entry<ConflictTxStream, Long>, ConflictTxStream, Long>toMap(Map.Entry::getKey, Map.Entry::getValue));

        verifyData(recordMap, cache);

        cache.evictUpTo(address - numRemains);
        assertThat(cache.size()).isEqualTo(numRemains);

        cache.evictUpTo(address);
        assertThat(cache.size()).isZero();
    }


    /**
     * Test the value regression for the same keys.
     */
    @Test
    public void testSequencerRegression() {
        SequencerServerCache cache = new SequencerServerCache(cacheSize, Address.NOT_FOUND);
        long address = 0;
        HashMap<ConflictTxStream, Long> recordMap = new HashMap<>();

        // put entries to the cache, make it full, some entries have the same address
        while (cache.size() < cacheSize) {
            generateData(recordMap, cache, entryPerAddress, address++, false);
        }

        verifyData(recordMap, cache);

        Comparator<Map.Entry<ConflictTxStream, Long>> valueComparator = (e1, e2) -> {
            long v1 = e1.getValue();
            long v2 = e2.getValue();
            return (int)(v1 - v2);
        };

        List<Map.Entry<ConflictTxStream, Long>> listOfEntries = new ArrayList<>(recordMap.entrySet());
        Collections.sort(listOfEntries, valueComparator);

        for (Map.Entry<ConflictTxStream, Long> entryVal : listOfEntries) {
            if (entryVal.getValue() != address) {
                log.debug("txV " + entryVal.getValue() + " address " + address);
            }

            final List<ConflictTxStream> entry = Collections.singletonList(entryVal.getKey());
            assertThrows(IllegalStateException.class, () -> cache.put(entry, entryVal.getValue()));
        }

        verifyData(recordMap, cache);
    }

    private byte[] intToByteArray(int data) {
        byte[] result = new byte[4];
        result[0] = (byte) ((data & 0xFF000000) >> 24);
        result[1] = (byte) ((data & 0x00FF0000) >> 16);
        result[2] = (byte) ((data & 0x0000FF00) >> 8);
        result[3] = (byte) ((data & 0x000000FF));
        return result;
    }
}
