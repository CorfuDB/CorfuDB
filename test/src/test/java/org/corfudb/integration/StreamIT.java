package org.corfudb.integration;

import com.google.common.io.Files;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

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

    @Test
    public void simpleStreamFailOverTest() throws Exception {

        File corfuLog = Files.createTempDir();
        corfuLog.deleteOnExit();

        Process corfuServerProcess = new CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(corfuSingleNodePort)
                .setLogPath(corfuLog.getAbsolutePath())
                .runServer();

        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();

        corfuServerProcess = new CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(corfuSingleNodePort)
                .setLogPath(corfuLog.getAbsolutePath())
                .runServer();

        simpleStreamRun();
        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
    }

    @Test
    public void simpleStreamNoFailOverTest() throws Exception {

        Process corfuServerProcess = new CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(corfuSingleNodePort)
                .runServer();

        simpleStreamRun();
        assertThat(shutdownCorfuServer(corfuServerProcess)).isTrue();
    }

    private void simpleStreamRun() {
        CorfuRuntime rt = createDefaultRuntime();
        rt.setCacheDisabled(true);

        Random rand = new Random();

        UUID streamId = CorfuRuntime.getStreamID(Integer.toString(rand.nextInt()));

        IStreamView s1 = rt.getStreamsView().get(streamId);

        // Verify that the stream is empty
        final int N_HASNEXT_TESTS = 20;
        IntStream.range(0, N_HASNEXT_TESTS).forEach(n -> assertThat(s1.hasNext()).isFalse());

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
    }
}
