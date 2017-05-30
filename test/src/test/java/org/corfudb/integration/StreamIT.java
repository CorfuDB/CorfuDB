package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;

import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT extends AbstractIT {
    static String corfuSingleNodeHost;
    static int corfuSingleNodePort;

    @Before
    public void loadProperties() throws Exception {
        corfuSingleNodeHost = (String) PROPERTIES.get("corfuSingleNodeHost");
        corfuSingleNodePort = Integer.parseInt((String) PROPERTIES.get("corfuSingleNodePort"));
    }

//    @Test
    public void simpleStreamTest() throws Exception {

        Process corfuServerProcess = new CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(corfuSingleNodePort)
                .runServer();

        CorfuRuntime rt = createDefaultRuntime();
        rt.setCacheDisabled(true);

        Random rand = new Random();

        UUID streamId = CorfuRuntime.getStreamID(Integer.toString(rand.nextInt()));

        IStreamView s1 = rt.getStreamsView().get(streamId);

        // Verify that the stream is empty
        assertThat(s1.hasNext())
                .isFalse();

        // Generate and append random data
        int entrySize = Integer.valueOf(PROPERTIES.getProperty("largeEntrySize"));
        final int numEntries = 100;
        byte[][] data = new byte[numEntries][entrySize];

        for(int x = 0; x < numEntries; x++) {
            rand.nextBytes(data[x]);
            s1.append(data[x]);
        }

        // Read back the data and verify it is correct
        for(int x = 0; x < numEntries; x++) {
            ILogData entry = s1.nextUpTo(x);
            byte[] tmp = (byte[]) entry.getPayload(rt);

            assertThat(tmp).isEqualTo(data[x]);
        }

        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
    }
}
