package org.corfudb.infrastructure;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.StreamLogParams.RECORDS_PER_SEGMENT;

public class SequencerTrimServiceTest extends AbstractViewTest {
    public CorfuRuntime setRuntime() {
        ServerContextBuilder serverContextBuilder = new ServerContextBuilder()
                .setMemory(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .setCompactionPolicyType("GARBAGE_SIZE_FIRST")
                .setSegmentGarbageRatioThreshold("0")
                .setSegmentGarbageSizeThresholdMB("0");

        return getDefaultRuntime(serverContextBuilder).connect();
    }

    @Test
    public void testAddressSpaceTrimInBatch() {
        CorfuRuntime r = setRuntime();

        // instantiates two corfu tables
        UUID streamId = UUID.randomUUID();

        Map<String, String> table = r.getObjectsView()
                .build()
                .setStreamID(streamId)
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .open();

        long sequencerTail = r.getSequencerView().query().getToken().getSequence();

        // populates table with the same key-value pairs up to compaction threshold
        final int entryNum = RECORDS_PER_SEGMENT;
        table.put("k1", "v");
        for (int i = 0; i <= entryNum; ++i) {
            table.put("k", "v");
        }

        // run auto commit
        AutoCommitService autoCommitService =
                getManagementServer(SERVERS.PORT_0).getManagementAgent().getAutoCommitService();
        autoCommitService.shutdown();

        // First invocation would only set the next commit end to the current log tail.
        autoCommitService.runAutoCommit();
        // Second invocation would do actual commit.
        autoCommitService.runAutoCommit();

        // run compaction
        r.getGarbageInformer().gcUnsafe();
        getLogUnit(SERVERS.PORT_0).runCompaction();
        r.getAddressSpaceView().resetCaches();
        r.getAddressSpaceView().invalidateServerCaches();

        // update and get compaction mark
        r.getAddressSpaceView().read(0L);
        long compactionMark = r.getAddressSpaceView().getCompactionMark().get();

        // address space trim in batch
        r.getSequencerView().addressSpaceTrimInBatch(Lists.newArrayList(streamId));

        // verify batch trim result
        Roaring64NavigableMap addressMap1 = r.getSequencerView().getStreamAddressSpace(new StreamAddressRange(streamId,
                Long.MAX_VALUE, -1)).getAddressMap();

        LongIterator iterator = addressMap1.getLongIterator();
        assertThat(iterator.next()).isEqualTo(sequencerTail + 1);
        assertThat(iterator.next()).isEqualTo(compactionMark);
        assertThat(iterator.next()).isEqualTo(compactionMark + 1);
    }
}
