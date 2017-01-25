package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/8/16.
 */
public class StreamViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    public CorfuRuntime r;

    @Before
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStream()
            throws Exception {
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
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW,
                i -> sv.write(testPayload));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME,
                PARAMETERS.TIMEOUT_NORMAL);

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW,
                i -> assertThat(sv.read().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes()));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME,
                PARAMETERS.TIMEOUT_NORMAL);
        assertThat(sv.read())
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStreamWithoutBackpointers()
            throws Exception {
        r.setBackpointersDisabled(true);

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        StreamView sv = r.getStreamsView().get(streamA);
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, i ->
                sv.write(testPayload));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_NORMAL);

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, i ->
                assertThat(sv.read().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes()));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_NORMAL);
        assertThat(sv.read())
                .isEqualTo(null);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromCachedStream()
            throws Exception {
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
