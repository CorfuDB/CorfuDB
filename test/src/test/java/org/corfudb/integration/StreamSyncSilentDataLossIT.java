package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproduces the silent trim gap scenario that leads to data loss.
 *
 * The client syncs part of a stream, then a prefix trim occurs, the server
 * restarts, and more data is appended. Without the fix, the client fails to
 * detect the gap between its last synced position and the trim mark, silently
 * skipping trimmed entries. With the fix, remaining() throws TrimmedException.
 *
 * This exercises the IStreamView / AddressMapStreamView code path.
 *
 * Adapted from PR #3328 (Maithem, 2022-08-18).
 */
@SuppressWarnings("PMD.ClassNamingConventions")
public class StreamSyncSilentDataLossIT extends AbstractIT {

    @Test
    public void testIStreamViewDetectsTrimGapAfterRestart() throws Exception {
        final String host = "localhost";
        final int port = 9000;
        final String endpoint = host + ":" + port;

        Process p = new CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();

        CorfuRuntime rt = new CorfuRuntime(endpoint).connect();

        try {
            IStreamView sv1 = rt.getStreamsView().get(CorfuRuntime.getStreamID("stream1"));

            final int numWrites = 10;
            final int payloadSize = 4000;
            byte[] payload = new byte[payloadSize];

            for (int x = 0; x < numWrites; x++) {
                sv1.append(payload);
            }

            assertThat(sv1.remaining().size()).isNotZero();

            sv1.append(payload);
            sv1.append(payload);

            Token tail = rt.getSequencerView().query().getToken();

            rt.getAddressSpaceView().prefixTrim(tail);

            // Write one more entry that survives the trim (simulates a
            // checkpoint entry or a late write above the trim point).
            sv1.append(payload);

            rt.getLayoutView().getRuntimeLayout().getBaseClient(endpoint).restart().join();

            // After restart, the server rebuilds LogMetadata with the
            // initializeLogMetadata() fix which sets correct per-stream trim
            // marks. The sequencer bootstraps with correct data.
            // remaining() should detect that our position is behind the trim.
            assertThatThrownBy(sv1::remaining).isInstanceOf(TrimmedException.class);

            sv1.append(payload);

            assertThatThrownBy(sv1::remaining).isInstanceOf(TrimmedException.class);
        } finally {
            rt.shutdown();
            p.destroy();
        }
    }
}
