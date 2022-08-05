package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.SequencerServerCache;
import org.corfudb.infrastructure.SequencerServerCache.ConflictTxStream;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.SMRObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
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

        ICorfuTable<Integer, Integer> map = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<Integer, Integer>>() {
                })
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setStreamName("test")
                .open();

        final int numTxn = 500;
        final Token trimAddress = new Token(getDefaultRuntime().getLayoutView().getLayout().getEpoch(), 250);
        for (int x = 0; x < numTxn; x++) {
            getRuntime().getObjectsView().TXBegin();
            map.insert(x, x);
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
            cache.put(firstKey);
            cache.put(secondKey);

            assertThat(cache.size()).isOne();
            assertThat(cache.get(firstKey)).isEqualTo(Address.NON_ADDRESS);
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
        long firstAddress = cache.firstAddress();
        long size = cache.size();

        cache.put(key);

        if (verifyFirst && cache.size() <= size) {
            log.debug("cache.firstAddress: " + cache.firstAddress() + " cacheSize: " + cache.size() + " address:" + address);
            assertThat(firstAddress).isLessThan(cache.firstAddress());
        }

        assertThat(cache.get(key)).isNotEqualTo(Address.NON_ADDRESS);
        recordMap.put(key, address);
        assertThat(cache.size()).isLessThanOrEqualTo(cacheSize);

    }

    /**
     * Verify cache contains all the data in recordMap that address >= firstAddress.
     * @param recordMap
     * @param cache
     */
    void verifyData(HashMap<ConflictTxStream, Long> recordMap, SequencerServerCache cache) {
        for (ConflictTxStream oldKey : recordMap.keySet()) {
            long oldAddress = oldKey.txVersion;
            if (oldAddress < cache.firstAddress()) {
                continue;
            }
            ConflictTxStream key = new ConflictTxStream(oldKey.getStreamId(), oldKey.getConflictParam(), 0);
            log.debug("address " + cache.get(key) + " expected " + oldAddress);
            assertThat(cache.get(key)).isEqualTo(oldAddress);
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

        assertThat(cache.size()).isEqualTo(cacheSize);
        verifyData(recordMap, cache);

        address = cacheSize;
        // Each put should evict all streams with the same address
        for (int i = 0; i < iterations; i++, address++) {
            generateData(recordMap, cache, address, true);
        }

        verifyData(recordMap, cache);

        cache.invalidateUpTo(address - 1);
        assertThat(cache.size()).isOne();
        cache.invalidateUpTo(address);
        assertThat(cache.size()).isZero();
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

        assertThat(cache.size()).isEqualTo(cacheSize);
        // Each put should evict all streams with the same address
        for (int i = 0; i < iterations; i++, address++) {
            generateData(recordMap, cache, address, true);
        }

        verifyData(recordMap, cache);
        log.info("cacheSize {} cacheByteSize {} cacheEntriesBytes {} ", cache.size(), cache.byteSize(), cache.byteSize());
        long entrySize = cache.byteSize() / cache.size();

        cache.invalidateUpTo(address - numRemains);
        assertThat(cache.size()).isEqualTo(numRemains);

        // this assume that the all conflickstreams has the same size of the parameters.
        assertThat(entrySize).isEqualTo(cache.byteSize() / cache.size());
        log.info("cacheSize {} cacheByteSize {} cacheEntriesBytes {} ", cache.size(), cache.byteSize(), cache.byteSize());
        cache.invalidateUpTo(address);
        assertThat(cache.size()).isZero();
    }

      @Test
    /*
        Test the value regression for the same key
     */
    public void testSequencerRegression() {
        SequencerServerCache cache = new SequencerServerCache(cacheSize, Address.NOT_FOUND);
        long address = 0;
        HashMap<ConflictTxStream, Long> recordMap = new HashMap<>();
        boolean result;

        // put entries to the cache, make it full, some entries have the same address
        while (cache.size() < cacheSize) {
            for (int j = 0; j < entryPerAddress && cache.size() < cacheSize; j++) {
                generateData(recordMap, cache, address++, false);
            }
        }


        Comparator<Map.Entry<ConflictTxStream, Long>> valueComparator = new Comparator<Map.Entry<ConflictTxStream, Long>>() {
            @Override public int compare(Map.Entry<ConflictTxStream, Long> e1, Map.Entry<ConflictTxStream, Long> e2)
            { long v1 = e1.getValue(); long v2 = e2.getValue(); return (int)(v1 - v2); } };

        List<Map.Entry<ConflictTxStream, Long>> listOfEntries = new ArrayList<Map.Entry<ConflictTxStream, Long>>(recordMap.entrySet());

        Collections.sort(listOfEntries, valueComparator);

        address = 0;
        for (Map.Entry<ConflictTxStream, Long> entryVal : listOfEntries) {
            ConflictTxStream entry = entryVal.getKey();
            if (entry.txVersion != address++) {
                log.debug("****txV " + entry.txVersion + " address " + address);
            }
            result = cache.put(new ConflictTxStream(entry.getStreamId(), entry.getConflictParam(), entry.txVersion));
            assertThat(result).isTrue();
        }

        int i = 0;
        for (Map.Entry<ConflictTxStream, Long> entryVal : listOfEntries) {
            ConflictTxStream entry = entryVal.getKey();
            if (entry.txVersion != address++) {
                log.debug("txV " + entry.txVersion + " address " + address);
            }

            ConflictTxStream txEle = new ConflictTxStream(entry.getStreamId(), entry.getConflictParam(), entry.txVersion - 1);
            long txVersion = cache.get(txEle);
            result = cache.put(txEle);

            i++;
            if (result) {
                log.debug("\n not false  i " + i + " txV " + txVersion + " result " + result + " size " + recordMap.keySet().size() + " cacheSize " + cache.size());
            }
            assertThat(result).isFalse();
        }
    }
}
