package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A set integration tests that exercise failure modes related to
 * large writes.
 */

public class LargeWriteIT extends AbstractIT {

    @Test
    public void largeStreamWrite() throws Exception {

        final String streamName = "s1";

        // Start node one and populate it with data
        Process server_1 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        final int maxWriteSize = 100;

        // Configure a client with a max write limit
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .maxWriteSize(maxWriteSize)
                .build();

        CorfuRuntime rt = CorfuRuntime.fromParameters(params);
        rt.parseConfigurationString(DEFAULT_ENDPOINT);
        rt.connect();

        final int bufSize = maxWriteSize * 2;

        // Attempt to write a payload that is greater than the configured limit.
        assertThatThrownBy(() -> rt.getStreamsView()
                .get(CorfuRuntime.getStreamID(streamName))
                .append(new byte[bufSize]))
                .isInstanceOf(WriteSizeException.class);
        shutdownCorfuServer(server_1);
    }

    @Test
    public void largeTransaction() throws Exception {
        final String tableName = "table1";

        // Start node one and populate it with data
        Process server_1 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        final int maxWriteSize = 100;

        // Configure a client with a max write limit
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .maxWriteSize(maxWriteSize)
                .build();

        CorfuRuntime rt = CorfuRuntime.fromParameters(params);
        rt.parseConfigurationString(DEFAULT_ENDPOINT);
        rt.connect();

        String largePayload = new String(new byte[maxWriteSize * 2]);

        Map<String, String> map = rt.getObjectsView()
                .build()
                .setType(CorfuTable.class)
                .setStreamName(tableName)
                .open();


        rt.getObjectsView().TXBegin();
        map.put("key1", largePayload);
        boolean aborted = false;
        try {
            rt.getObjectsView().TXEnd();
        } catch (TransactionAbortedException e) {
            aborted = true;
            assertThat(e.getCause()).isInstanceOf(WriteSizeException.class);
            assertThat(e.getAbortCause()).isEqualTo(AbortCause.SIZE_EXCEEDED);
        }
        assertThat(aborted).isTrue();
        shutdownCorfuServer(server_1);
    }
}
