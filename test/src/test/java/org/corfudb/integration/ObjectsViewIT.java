package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.NonNull;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectsViewIT extends AbstractIT {

    private void writeTx(@NonNull PersistentCorfuTable<String, String> table, @NonNull CorfuRuntime rt,
                         @NonNull String key, int offset, int numWrites) {
        for (int i = offset; i < offset + numWrites; i++) {
            rt.getObjectsView().TXBegin();
            table.insert(key, key + i);
            rt.getObjectsView().TXEnd();
        }
    }

    /**
     * This test validates that the object (MVCC) layer produces the correct view of a
     * table whenever it detects a sequencer regression. The object layer should reset
     * the state of the object and retry the sync in order to correct its in-memory view.
     */
    @Test
    public void validateObjectViewAfterRegression() throws Exception {
        final Process p = new AbstractIT.CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setLogPath(getCorfuServerLogPath(DEFAULT_HOST, DEFAULT_PORT))
                .setSingle(true)
                .runServer();

        final CorfuRuntime rt1 = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
        final String s1 = "stream1";
        final String s2 = "stream2";
        final String key = "key";

        final int singleWrite = 1;
        final int multiWrite = 10;
        final UUID s2Id = CorfuRuntime.getStreamID(s2);

        PersistentCorfuTable<String, String> table1 = rt1.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName(s1)
                .open();

        PersistentCorfuTable<String, String> table2 = rt1.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName(s2)
                .open();

        // Initial state of the log.
        // 0 (s2) | 1 (s1) | 2 (s1) | 3 (s1) | ... | 10 (s1).
        writeTx(table2, rt1, key, 0, singleWrite);
        writeTx(table1, rt1, key, 0, multiWrite);

        // Simulate a sequencer regression by issuing a few tokens (for table2).
        // 0 (s2) | 1 (s1) | 2 (s1) | 3 (s1) | ... | 10 (s1) | 11 (issued) | 12 (issued) | 13 (issued).
        rt1.getSequencerView().next(s2Id);
        rt1.getSequencerView().next(s2Id);
        rt1.getSequencerView().next(s2Id);

        // GlobalTail = 13. Perform a read on table1 using this global tail.
        // This will update resolvedUpTo = 13 in the MVO for this table.
        rt1.getObjectsView().TXBegin();
        assertThat(table1.get(key)).isEqualTo(key + (multiWrite - 1));
        rt1.getObjectsView().TXEnd();

        // Restart the server. Sequencer regresses from globalTail = 13 to globalTail = 10.
        // 0 (s2) | 1 (s1) | 2 (s1) | 3 (s1) | ... | 10 (s1).
        restartServer(rt1, DEFAULT_ENDPOINT);
        assertThat(rt1.getSequencerView().query().getToken().getSequence()).isEqualTo(multiWrite);

        // Write new data @11 for s1, and at @12, @13, @14, ..., @21 for s2.
        // We use a different runtime to avoid impacting metadata relating to rt1.
        // 0 (s2) | 1 (s1) | 2 (s1) | 3 (s1) | ... | 10 (s1) | 11 (s1) | 12 (s2) | 13 (s2) | ... | 21 (s2).
        final CorfuRuntime rt2 = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
        PersistentCorfuTable<String, String> table1rt2 = rt2.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName(s1)
                .open();

        PersistentCorfuTable<String, String> table2rt2 = rt2.getObjectsView()
                .build()
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setStreamName(s2)
                .open();

        writeTx(table1rt2, rt2, key, multiWrite, singleWrite);
        writeTx(table2rt2, rt2, key, singleWrite, multiWrite);

        // Perform a read on table1 with rt1. Since globalTail = 21 > resolvedUpTo = 13, we will trigger a sync.
        // However, when applying the updates from the object, 11 will not be >= 13, so an IllegalStateException
        // will be thrown. The object layer should detect this and reset the object in order to provide a correct view.
        rt1.getObjectsView().TXBegin();
        assertThat(table1.get(key)).isEqualTo(key + multiWrite);
        rt1.getObjectsView().TXEnd();

        rt1.shutdown();
        rt2.shutdown();
        shutdownCorfuServer(p);
    }
}
