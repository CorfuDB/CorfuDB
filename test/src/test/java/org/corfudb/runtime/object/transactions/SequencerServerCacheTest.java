package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.SequencerServerCache;
import org.corfudb.infrastructure.SequencerServerCache.ConflictTxStream;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.view.Address;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by maithem on 7/24/17.
 */
@Slf4j
public class SequencerServerCacheTest extends AbstractObjectTest {
    @Test
    public void testSequencerCacheTrim() {

        getDefaultRuntime();

        Map<Integer, Integer> map = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<CorfuTable<Integer, Integer>>() {
                })
                .setStreamName("test")
                .open();

        final int numTxn = 500;
        final Token trimAddress = new Token(getDefaultRuntime().getLayoutView().getLayout().getEpoch(), 250);
        for (int x = 0; x < numTxn; x++) {
            getRuntime().getObjectsView().TXBegin();
            map.put(x, x);
            getRuntime().getObjectsView().TXEnd();
        }

        SequencerServer sequencerServer = getSequencer(0);
        SequencerServerCache cache = sequencerServer.getCache();
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
     * Check cache invalidation
     */
    @Test
    public void testCache() {
        final AtomicBoolean criticalVariable = new AtomicBoolean();

        SequencerServerCache cache = new SequencerServerCache(1, Address.NOT_FOUND);
        final long firstValue = 1L;
        final long secondValue = 2L;
        final int iterations = 10;
        final ConflictTxStream firstKey = new ConflictTxStream(UUID.randomUUID(), new byte[]{}, firstValue);
        final ConflictTxStream secondKey = new ConflictTxStream(UUID.randomUUID(), new byte[]{}, secondValue);

        for (int i = 0; i < iterations; i++) {
            criticalVariable.set(false);

            cache.put(firstKey);
            cache.put(secondKey);

            assertThat(cache.size()).isOne();
            assertThat(cache.getIfPresent(firstKey)).isNull();
        }
    }

    public static final int entryPerAddress = 20;
    public static final int iterations = 100;
    public static final int cacheSize = iterations * entryPerAddress;
    public static final int numRemains = 10;

    /**
     * generate data with given address and verify that the entries with firstAddress are correctly evicted
     */
    void generateData(HashMap recordMap, SequencerServerCache cache, long address, boolean verifyFirst) {
        final ConflictTxStream key = new ConflictTxStream(UUID.randomUUID(), new byte[]{}, address);
        if (verifyFirst) {
            log.debug("cache.firstAddress: " + cache.firstAddress() + " cacheSize: " + cache.size());
            assertThat(cache.firstAddress() == address - cacheSize);
        }
        cache.put(key);
        assertThat(cache.getIfPresent(key) != null);
        recordMap.put(key, address);
        assertThat(cache.size() <= cacheSize);
    }

    // Verify cache contains all the data in recordMap that address >= firstAddress.
    void verifyData(HashMap<ConflictTxStream, Long> recordMap, SequencerServerCache cache) {
        for (ConflictTxStream oldKey : recordMap.keySet()) {
            long oldAddress = oldKey.txVersion;
            if (oldAddress < cache.firstAddress())
                continue;
            ConflictTxStream key = new ConflictTxStream(oldKey.getStreamId(), oldKey.getConflictParam(), 0);
            log.debug("address " + cache.getIfPresent(key) + " expected " + oldAddress);
            assertThat(cache.getIfPresent(key) == oldAddress);
        }
    }

    @Test
    /*
        Test the evication the firstAddress while the cache is full, by generating address out of order
     */
    public void testSequencerCacheEvict1() {
        SequencerServerCache cache = new SequencerServerCache(cacheSize, Address.NOT_FOUND);
        long address = 0;
        HashMap<ConflictTxStream, Long> recordMap = new HashMap<>();

        // put entries to the cache with duplicate address not in order
        while (cache.size() < cacheSize) {
            address = 0;
            for (int i = 0; i < cacheSize / entryPerAddress; i++) {
                generateData(recordMap, cache, address++, false);
            }
        }

        assertThat(cache.size() == cacheSize);
        verifyData(recordMap, cache);

        // Each put should evict all streams with the same address
        for (int i = 0; i < iterations; i++, address++) {
            generateData(recordMap, cache, address, true);
        }

        verifyData(recordMap, cache);

        cache.invalidateUpTo(address - 1);
        assertThat(cache.size() == 1);
        cache.invalidateUpTo(address);
        assertThat(cache.size() == 0);
    }

    @Test
    /*
        Test the evication the firstAddress while the cache is full, by generating address in order
     */
    public void testSequencerCacheEvict2() {
        SequencerServerCache cache = new SequencerServerCache(cacheSize, Address.NOT_FOUND);
        long address = 0;
        HashMap<ConflictTxStream, Long> recordMap = new HashMap<>();

        // put entries to the cache, make it full, some entries have the same address
        while (cache.size() < cacheSize) {
            for (int j = 0; j < entryPerAddress; j++) {
                generateData(recordMap, cache, address++, false);
            }
        }

        verifyData(recordMap, cache);

        assertThat(cache.size() == cacheSize);
        // Each put should evict all streams with the same address
        for (int i = 0; i < iterations; i++, address++) {
            generateData(recordMap, cache, address, true);
        }

        verifyData(recordMap, cache);
        log.info("cacheSize {} cacheByteSize {} cacheEntriesBytes {} ", cache.size(), cache.byteSize(), cache.getCacheEntriesBytes());
        long entrySize = cache.byteSize() / cache.size();

        cache.invalidateUpTo(address - numRemains);
        assertThat(cache.size() == numRemains);

        // this assume that the all conflickstreams has the same size of the parameters.
        assertThat(entrySize == cache.byteSize() / cache.size());
        log.info("cacheSize {} cacheByteSize {} cacheEntriesBytes {} ", cache.size(), cache.byteSize(), cache.getCacheEntriesBytes());
        cache.invalidateUpTo(address);
        assertThat(cache.size() == 0);
    }

    @Test
    /*
        Test the value regression for the same key
     */
    public void testSequencerRegression() {
        SequencerServerCache cache = new SequencerServerCache(cacheSize, Address.NOT_FOUND);
        long address = 0;
        HashMap<ConflictTxStream, Long> recordMap = new HashMap<>();

        // put entries to the cache, make it full, some entries have the same address
        while (cache.size() < cacheSize) {
            for (int j = 0; j < entryPerAddress; j++) {
                generateData(recordMap, cache, address++, false);
            }
        }

        for (ConflictTxStream entry : recordMap.keySet()) {
            assertThat(cache.put(new ConflictTxStream(entry.getStreamId(), entry.getConflictParam(), entry.txVersion)) == false);
        }

        for (ConflictTxStream entry : recordMap.keySet()) {
            assertThat(cache.put(new ConflictTxStream(entry.getStreamId(), entry.getConflictParam(), entry.txVersion - 1)) == false);
        }
    }
}