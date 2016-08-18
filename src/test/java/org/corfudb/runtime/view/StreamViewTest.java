package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/8/16.
 */
public class StreamViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStream()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime().connect();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        sv.write(testPayload);

        assertThat(sv.read().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.read())
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStreamConcurrent()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime().connect();
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        scheduleConcurrently(100, i -> sv.write(testPayload));
        executeScheduled(8, 10, TimeUnit.SECONDS);

        scheduleConcurrently(100, i -> assertThat(sv.read().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes()));
        executeScheduled(8, 10, TimeUnit.SECONDS);
        assertThat(sv.read())
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStreamWithoutBackpointers()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime()
                .setBackpointersDisabled(true)
                .connect();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        scheduleConcurrently(100, i -> sv.write(testPayload));
        executeScheduled(8, 10, TimeUnit.SECONDS);

        scheduleConcurrently(100, i -> assertThat(sv.read().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes()));
        executeScheduled(8, 10, TimeUnit.SECONDS);
        assertThat(sv.read())
                .isEqualTo(null);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromCachedStream()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime().connect()
                .setCacheDisabled(false);
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        sv.write(testPayload);

        assertThat(sv.read().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.read())
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void streamCanSurviveOverwriteException()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime().connect();
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // write without reserving a token
        r.getAddressSpaceView().fillHole(0);

        // Write to the stream, and read back. The hole should be filled.
        StreamView sv = r.getStreamsView().get(streamA);
        sv.write(testPayload);

        assertThat(sv.read().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.read())
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void streamWillHoleFill()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime().connect();
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // Generate a hole.
        r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        // Write to the stream, and read back. The hole should be filled.
        StreamView sv = r.getStreamsView().get(streamA);
        sv.write(testPayload);

        assertThat(sv.read().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.read())
                .isEqualTo(null);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void streamWithHoleFill()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime();
        UUID streamA = CorfuRuntime.getStreamID("stream A");

        byte[] testPayload = "hello world".getBytes();
        byte[] testPayload2 = "hello world2".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        sv.write(testPayload);

        //generate a stream hole
        TokenResponse tr =
                r.getSequencerView().nextToken(Collections.singleton(streamA), 1);
        r.getAddressSpaceView().fillHole(tr.getToken());

        tr = r.getSequencerView().nextToken(Collections.singleton(streamA), 1);
        r.getAddressSpaceView().fillHole(tr.getToken());

        sv.write(testPayload2);

        //make sure we can still read the stream.
        assertThat(sv.read().getPayload(getRuntime()))
                .isEqualTo(testPayload);

        assertThat(sv.read().getPayload(getRuntime()))
                .isEqualTo(testPayload2);
    }
}
