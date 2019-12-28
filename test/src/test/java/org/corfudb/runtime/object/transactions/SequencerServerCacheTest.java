package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.SequencerServerCache;
import org.corfudb.infrastructure.SequencerServerCache.ConflictTxStream;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

        SequencerServerCache cache = new SequencerServerCache(1);

        final ConflictTxStream firstKey = new ConflictTxStream(UUID.randomUUID(), new byte[]{});
        final ConflictTxStream secondKey = new ConflictTxStream(UUID.randomUUID(), new byte[]{});
        final long firstValue = 1L;
        final long secondValue = 2L;
        final int iterations = 10;

        for (int i = 0; i < iterations; i++) {
            criticalVariable.set(false);

            cache.put(firstKey, firstValue);
            cache.put(secondKey, secondValue);

            assertThat(cache.size()).isOne();
            assertThat(cache.getIfPresent(firstKey)).isNull();

            cache.invalidateAll();
            assertThat(cache.size()).isZero();
        }
    }
}
