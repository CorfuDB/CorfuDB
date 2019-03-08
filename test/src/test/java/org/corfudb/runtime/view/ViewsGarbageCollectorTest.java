package org.corfudb.runtime.view;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;

import org.ehcache.sizeof.SizeOf;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by maithem on 11/16/18.
 */

public class ViewsGarbageCollectorTest extends AbstractViewTest {

    @Test
    @Ignore
    public void testRuntimeGC() throws Exception {

        CorfuRuntime rt = getDefaultRuntime();

        CorfuTable<String, String> table = rt.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName("table1")
                .open();

        assertThat(rt.getGarbageCollector().isStarted()).isTrue();

        SizeOf sizeOf = SizeOf.newInstance();
        long sizeAfterCreation = sizeOf.deepSizeOf(table);
        rt.getGarbageCollector().runRuntimeGC();

        final int value = 1;
        table.put(String.valueOf(value), String.valueOf(value));
        final long currentSize = sizeOf.deepSizeOf(table);

        assertThat(sizeAfterCreation).isLessThanOrEqualTo(currentSize);

        final int numWrites = 100;

        //TODO(Maithem): sync garbage from other clients
        for (int x = 0; x < numWrites; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        long sizeOfTableAfterWrite = sizeOf.deepSizeOf(table);

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);
        Token trimMark = mcw.appendCheckpoints(rt, "cp1");
        rt.getAddressSpaceView().prefixTrim(new Token(trimMark.getEpoch(), trimMark.getSequence() - 1));
        rt.getParameters().setRuntimeGCPeriod(Duration.ofMinutes(0));
        rt.getGarbageCollector().runRuntimeGC();
        long sizeOfTableAfterGc = sizeOf.deepSizeOf(table);
        final int numOfSetsStreamSets = 3;
        assertThat(sizeOfTableAfterGc)
                .isLessThan(sizeOfTableAfterWrite - (numWrites * Long.BYTES * numOfSetsStreamSets));
        assertThat(rt.getAddressSpaceView().getReadCache().asMap().size()).isOne();
        rt.shutdown();
        assertThat(rt.getGarbageCollector().isStarted()).isFalse();
    }
}
