package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.SequencerServerCache;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by maithem on 7/24/17.
 */
@Slf4j
public class SequencerCacheTest extends AbstractObjectTest {

    @Test
    public void testSequencerCacheTrim() throws Exception {

        getDefaultRuntime();

        Map<Integer, Integer> map = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {
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
        assertThat(cache.size()).isEqualTo((int) trimAddress.getSequence());
    }

    /**
     * Check cache eviction algorithm (it must be atomic operation).
     * Check cache invalidation
     */
    @Test
    public void testCache() {
        final AtomicBoolean criticalVariable = new AtomicBoolean();

        SequencerServerCache cache = new SequencerServerCache(1, new CacheWriter<String, Long>() {
            @Override
            public void write(@Nonnull String key, @Nonnull Long value) {
                log.info("Write: [{}, {}]. Thread: {}", key, value, Thread.currentThread().getName());
            }

            @Override
            public void delete(@Nonnull String key, @Nullable Long value, @Nonnull RemovalCause cause) {
                log.info("Delete record: {}. Thread: {}", key, Thread.currentThread().getName());
                criticalVariable.set(true);
            }
        });

        final String firstKey = "1";
        final String secondKey = "2";
        final long firstValue = 1L;
        final long secondValue = 2L;
        final int iterations = 10;

        for (int i = 0; i < iterations; i++) {
            cache.put(firstKey, firstValue);
            cache.put(secondKey, secondValue);

            assertThat(cache.getIfPresent("1")).as("iteration: %s", i).isNull();

            cache.invalidate();
            assertThat(cache.size()).isZero();
            criticalVariable.set(false);
        }
    }
}
