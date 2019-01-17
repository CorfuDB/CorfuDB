package org.corfudb.runtime.view;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;

import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.util.Sleep;
import org.ehcache.sizeof.SizeOf;
import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by maithem on 11/16/18.
 */

public class ViewsMaintenanceTest extends AbstractViewTest {

    @Test
    public void testRuntimeGC() throws Exception {

        CorfuRuntime rt = getDefaultRuntime();

        CorfuTable<String, String> table = rt.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName("table1")
                .open();

        assertThat(rt.getViewsMaintenance().isStarted()).isFalse();
        rt.getViewsMaintenance().start();
        assertThat(rt.getViewsMaintenance().isStarted()).isTrue();

        SizeOf sizeOf = SizeOf.newInstance();
        long sizeAfterCreation = sizeOf.deepSizeOf(table);
        rt.getViewsMaintenance().runRuntimeGC();
        assertThat(sizeAfterCreation).isLessThanOrEqualTo(sizeOf.deepSizeOf(table));

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
        rt.getViewsMaintenance().setObjectGCPeriodInSeconds(Duration.ofMinutes(0).getSeconds());
        rt.getViewsMaintenance().runRuntimeGC();
        long sizeOfTableAfterGc = sizeOf.deepSizeOf(table);
        final int numOfSetsStreamSets = 3;
        assertThat(sizeOfTableAfterGc)
                .isLessThan(sizeOfTableAfterWrite - (numWrites * Long.BYTES * numOfSetsStreamSets));
        assertThat(rt.getAddressSpaceView().getReadCache().asMap()).isEmpty();
        rt.shutdown();
        assertThat(rt.getViewsMaintenance().isStarted()).isFalse();
    }

    @Test
    public void runSyncObjectsTest() throws Exception {
        CorfuRuntime rt1 = getDefaultRuntime();

        String streamName = "streamA";

        CorfuTable<String, String> table1Rt1 = rt1.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(streamName)
                .open();

        CorfuRuntime rt2 = getRuntime(rt1.getLayoutView().getLayout()).connect();

        final long objectRefreshPeriod = 1;
        rt2.getViewsMaintenance().setObjectSyncDelayInSeconds(objectRefreshPeriod);
        rt2.getViewsMaintenance().start();

        CorfuTable<String, String> table1Rt2 = rt2.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(streamName)
                .open();

        long rt2TableVersion = ((CorfuCompileProxy) ((ICorfuSMR) table1Rt2).getCorfuSMRProxy())
                .getUnderlyingObject().getVersionUnsafe();
        assertThat(rt2TableVersion).isEqualTo(Address.NON_ADDRESS);

        // populate the first client's map
        final int numWrites = 5;
        for (int x = 0; x < numWrites; x++) {
            table1Rt1.put(String.valueOf(x), String.valueOf(x));
        }

        // Notice that we don't access table1Rt2's map, but we expect it to be
        // synced
        final int numRefreshCycles = 3;
        Sleep.sleepUninterruptibly(Duration.ofSeconds(objectRefreshPeriod * numRefreshCycles));
        long rt2TableVersionAfterSync = ((CorfuCompileProxy) ((ICorfuSMR) table1Rt2).getCorfuSMRProxy())
                .getUnderlyingObject().getVersionUnsafe();
        // Verify that it was synced up to the tail
        assertThat(rt2TableVersionAfterSync).isEqualTo(rt2.getSequencerView().query().getToken().getSequence());
    }
}
