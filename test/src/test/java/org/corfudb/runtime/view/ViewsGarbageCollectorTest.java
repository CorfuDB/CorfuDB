package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;

import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by maithem on 11/16/18.
 */

public class ViewsGarbageCollectorTest extends AbstractViewTest {

    @Test
    public void testRuntimeGC() throws InterruptedException {

        CorfuRuntime rt = getDefaultRuntime();
        rt.getParameters().setMaxCacheEntries(MVO_CACHE_SIZE);

        PersistentCorfuTable<String, String> table = rt.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setVersioningMechanism(SMRObject.VersioningMechanism.PERSISTENT)
                .setStreamName("table1")
                .open();

        assertThat(rt.getGarbageCollector().isStarted()).isTrue();

        rt.getGarbageCollector().runRuntimeGC();

        final int numWrites = 100;

        for (int x = 0; x < numWrites; x++) {
            table.insert(String.valueOf(x), String.valueOf(x));
        }

        MultiCheckpointWriter<PersistentCorfuTable<String, String>> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(table);
        Token trimMark = mcw.appendCheckpoints(rt, "cp1");
        rt.getAddressSpaceView().prefixTrim(trimMark);
        rt.getParameters().setRuntimeGCPeriod(Duration.ofMinutes(0));
        rt.getGarbageCollector().runRuntimeGC();
        assertThat(rt.getAddressSpaceView().getReadCache().asMap()).isEmpty();

        TimeUnit.SECONDS.sleep(1); // MVOCache eviction is async
        assertThat(rt.getObjectsView().getMvoCache().getObjectCache().size()).isZero();

        rt.shutdown();
        assertThat(rt.getGarbageCollector().isStarted()).isFalse();
    }
}
