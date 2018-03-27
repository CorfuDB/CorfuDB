package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT extends AbstractIT {

    @Test
    public void largeStreamWrite() throws Exception {
        final String host = "localhost";
        final String streamName = "s1";
        final int port = 9000;

        // Start node one and populate it with data
        Process server_1 = new CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setSingle(true)
                .runServer();

        final int maxWriteSize = 100;

        // Configure a client with a max write limit
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .maxWriteSize(maxWriteSize)
                .build();

        CorfuRuntime rt = CorfuRuntime.fromParameters(params);
        rt.parseConfigurationString(host + ":" + port);
        rt.connect();

        final int bufSize = maxWriteSize * 2;

        // Attempt to write a payload that is greater than the configured limit.
        assertThatThrownBy(() -> rt.getStreamsView()
                .get(CorfuRuntime.getStreamID(streamName))
                .append(new byte[bufSize]))
                .isInstanceOf(WriteSizeException.class);
        shutdownCorfuServer(server_1);
    }
}
