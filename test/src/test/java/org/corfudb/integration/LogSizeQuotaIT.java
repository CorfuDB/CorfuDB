package org.corfudb.integration;

import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.view.Priority;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class LogSizeQuotaIT extends AbstractIT {

    @Test
    public void testLogSizeQuota() throws Exception {

        final int numWrites = 1000;
        final int payloadSize = 100;

        Process server_1 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .setLogPath(getCorfuServerLogPath(DEFAULT_HOST, DEFAULT_PORT))
                .setLogSizeQuota(Long.toString(numWrites * payloadSize))
                .runServer();

        byte[] payload = new byte[payloadSize];
        CorfuRuntime rt = new CorfuRuntime(DEFAULT_ENDPOINT).connect();

        Map<String, String> map = rt.getObjectsView()
                .build()
                .setStreamName("s1")
                .setType(CorfuTable.class)
                .open();

        // Create a map
        map.put("k1", "v1");
        map.put("k2", "v2");

        // Fill the log with data till the quota is reached
        IStreamView sv = rt.getStreamsView().get(UUID.randomUUID());

        boolean quotaReached = false;
        for (int x = 0; x < numWrites; x++) {
            try {
                sv.append(payload);
            } catch (QuotaExceededException e) {
                quotaReached = true;
                break;
            }
        }

        assertThat(quotaReached).isTrue();

        // Start/Commit a high priority transaction
        rt.getObjectsView().TXBuild().priority(Priority.HIGH).build().begin();
        map.put("k3", "k3");
        rt.getObjectsView().TXEnd();

        // Increment the log tail
        int newTail = StreamLogFiles.RECORDS_PER_LOG_FILE * 2;
        for (int x = 0; x < newTail; x++) {
            rt.getSequencerView().next();
        }

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(map);
        Token prefix = mcw.appendCheckpoints(rt, "cp1");
        rt.getAddressSpaceView().prefixTrim(prefix);

        assertThatThrownBy(() -> map.put("k4", "k4"))
                .isInstanceOf(QuotaExceededException.class);

        rt.getAddressSpaceView().gc();

        assertDoesNotThrow(() -> map.put("k4", "k4"));
        
        CorfuRuntime rt2 = new CorfuRuntime(DEFAULT_ENDPOINT).connect();
        Map<String, String> map2 = rt2.getObjectsView()
                .build()
                .setStreamName("s1")
                .setType(CorfuTable.class)
                .open();

        assertThat(map2.get("k1")).isEqualTo(map.get("k1"));
        assertThat(map2.get("k2")).isEqualTo(map.get("k2"));
        assertThat(map2.get("k3")).isEqualTo(map.get("k3"));
        assertThat(map2.get("k4")).isEqualTo(map.get("k4"));
        shutdownCorfuServer(server_1);
    }
}
