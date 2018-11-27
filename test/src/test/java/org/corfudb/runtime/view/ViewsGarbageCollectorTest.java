package org.corfudb.runtime.view;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;

import org.ehcache.sizeof.SizeOf;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by maithem on 11/16/18.
 */

public class ViewsGarbageCollectorTest extends AbstractViewTest {

    @Test
    public void testRuntimeGC() throws Exception {

        CorfuRuntime rt = getDefaultRuntime();

        CorfuTable<String, String> table = rt.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName("table1")
                .open();

        assertThat(rt.getGarbageCollector().isStarted()).isTrue();
        SizeOf sizeOf = SizeOf.newInstance();

        final int numWrites = 100;
        //TODO(Maithem): sync garbage from other clients
        for (int x = 0; x < numWrites; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        long sizeOfTableAfterWrite = sizeOf.deepSizeOf(table);

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);
        Token trimMark = mcw.appendCheckpoints(rt, "cp1");
        rt.getAddressSpaceView().prefixTrim(trimMark);
        rt.getParameters().setRuntimeGCPeriod(Duration.ofMinutes(0));
        rt.getGarbageCollector().runRuntimeGC();
        long sizeOfTableAfterGc = sizeOf.deepSizeOf(table);
        final int numOfSetsStreamSets = 3;
        assertThat(sizeOfTableAfterGc)
                .isLessThan(sizeOfTableAfterWrite - (numWrites * Long.BYTES * numOfSetsStreamSets));
        assertThat(rt.getAddressSpaceView().getReadCache().asMap()).isEmpty();
        rt.shutdown();
        assertThat(rt.getGarbageCollector().isStarted()).isFalse();
    }
}
