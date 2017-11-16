package org.corfudb.runtime.object.transactions;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.reflect.TypeToken;

import java.util.Map;

import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by maithem on 7/24/17.
 */
public class SequencerCacheTest extends AbstractObjectTest {

    @Test
    public void testSequencerCacheTrim() throws Exception {

        getDefaultRuntime();


        Map<Integer, Integer> map = getDefaultRuntime()
                .getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRMap<Integer, Integer>>() {})
                .setStreamName("test")
                .open();

        final int numTxn = 500;
        final int trimAddress = 250;
        for (int x = 0; x < numTxn; x++) {
            getRuntime().getObjectsView().TXBegin();
            map.put(x, x);
            getRuntime().getObjectsView().TXEnd();
        }

        SequencerServer sequencerServer = getSequencer(0);
        Cache<SequencerServer.ConflictObject, Long> cache = sequencerServer
                .getConflictToTailCache();
        assertThat(cache.asMap().size()).isEqualTo(numTxn);
        getDefaultRuntime().getAddressSpaceView().prefixTrim(trimAddress);
        assertThat(cache.asMap().size()).isEqualTo(trimAddress);
    }
}
