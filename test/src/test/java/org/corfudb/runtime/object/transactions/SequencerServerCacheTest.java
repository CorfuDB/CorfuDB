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

    /**
     * Check cache eviction algorithm (it must be atomic operation).
     * Check cache invalidation
     */
    @Test
    public void testCache() {
        final AtomicBoolean criticalVariable = new AtomicBoolean();

        SequencerServerCache cache = new SequencerServerCache(1, new CacheWriter<ConflictTxStream, Long>() {
            @Override
            public void write(@Nonnull ConflictTxStream key, @Nonnull Long value) {
                log.info("Write: [{}, {}]. Thread: {}", key, value, Thread.currentThread().getName());
            }

            @Override
            public void delete(@Nonnull ConflictTxStream key, @Nullable Long value, @Nonnull RemovalCause cause) {
                log.info("Delete record: {}. Thread: {}", key, Thread.currentThread().getName());
                criticalVariable.set(true);
            }
        });

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
