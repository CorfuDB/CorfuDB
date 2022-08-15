package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This test illustrates a silent failure during stream sync that can result in data loss.
 * The client syncs part of the stream then a prefix trim occurs and then the node is restarted,
 * and more data is added to the stream. This causes a gap between the latest update that the client
 * observed and the prefix trim mark. The client isn't detecting the gap, i.e., failing silently.
 * The correct behavior is to fail explicitly so that the client is able reconcile the trimmed gap
 * by reloading a new checkpoint.
 *
 */

public class StreamSyncSilentDataLoss extends AbstractIT {

    @Test
    @Ignore
    public void test() throws Exception {

        String host = "localhost";
        final int port = 9000;
        String endpoint = host + ":" + port;
        Process p = new CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();

        CorfuRuntime rt = new CorfuRuntime(endpoint).connect();


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

        rt.getLayoutView().getRuntimeLayout().getBaseClient(endpoint).restart().join();

        sv1.append(payload);

        assertThatThrownBy(sv1::remaining);
        p.destroy();
    }
}
