package org.corfudb.runtime.view;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }

    /**
     * Test that a client can call IStreamView.remainingUpTo after a prefix trim.
     * If remainingUpTo contains trimmed addresses, then they are ignored.
     */
    @Test
    public void testRemainingUpToWithTrim() {
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(true)
                .build();

        IStreamView txStream = runtime.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID, options);
        final int firstIter = 50;
        for (int x = 0; x < firstIter; x++) {
            byte[] data = "Hello World!".getBytes();
            txStream.append(data);
        }

        List<ILogData> entries = txStream.remainingUpTo((firstIter - 1) / 2);
        assertThat(entries.size()).isEqualTo(firstIter / 2);

        runtime.getAddressSpaceView().prefixTrim((firstIter - 1) / 2);
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();

        entries = txStream.remainingUpTo((firstIter - 1) / 2);
        assertThat(entries.size()).isEqualTo(0);

        entries = txStream.remainingUpTo(firstIter);
        assertThat(entries.size()).isEqualTo((firstIter / 2));

        // Open the stream with a new client
        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                                        .connect();
        txStream = rt2.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID, options);
        entries = txStream.remainingUpTo(Long.MAX_VALUE);
        assertThat(entries.size()).isEqualTo((firstIter / 2));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStreamConcurrent()
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW,
                i -> sv.append(testPayload));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME,
                PARAMETERS.TIMEOUT_NORMAL);

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW,
                i -> assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes()));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME,
                PARAMETERS.TIMEOUT_NORMAL);
        assertThat(sv.next())
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteFromStreamWithoutBackpointers()
            throws Exception {
        r.setBackpointersDisabled(true);

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, i ->
                sv.append(testPayload));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_NORMAL);

        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, i ->
                assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes()));
        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_NORMAL);
        assertThat(sv.next())
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

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }

    @Test
    public void canSeekOnStream()
        throws Exception
    {
        CorfuRuntime r = getDefaultRuntime().connect();
        IStreamView sv = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  A"));

        // Append some entries
        sv.append("a".getBytes());
        sv.append("b".getBytes());
        sv.append("c".getBytes());

        // Try reading two entries
        assertThat(sv.next().getPayload(r))
                .isEqualTo("a".getBytes());
        assertThat(sv.next().getPayload(r))
                .isEqualTo("b".getBytes());

        // Seeking to the beginning
        sv.seek(0);
        assertThat(sv.next().getPayload(r))
                .isEqualTo("a".getBytes());

        // Seeking to the end
        sv.seek(2);
        assertThat(sv.next().getPayload(r))
                .isEqualTo("c".getBytes());

        // Seeking to the middle
        sv.seek(1);
        assertThat(sv.next().getPayload(r))
                .isEqualTo("b".getBytes());
    }

    @Test
    public void canFindInStream()
            throws Exception
    {
        CorfuRuntime r = getDefaultRuntime().connect();
        IStreamView svA = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  A"));
        IStreamView svB = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  B"));

        // Append some entries
        final long A_GLOBAL = 0;
        svA.append("a".getBytes());
        final long B_GLOBAL = 1;
        svB.append("b".getBytes());
        final long C_GLOBAL = 2;
        svA.append("c".getBytes());
        final long D_GLOBAL = 3;
        svB.append("d".getBytes());
        final long E_GLOBAL = 4;
        svA.append("e".getBytes());

        // See if we can find entries:
        // Should find entry "c"
        assertThat(svA.find(B_GLOBAL,
                IStreamView.SearchDirection.FORWARD))
                .isEqualTo(C_GLOBAL);
        // Should find entry "a"
        assertThat(svA.find(B_GLOBAL,
                IStreamView.SearchDirection.REVERSE))
                .isEqualTo(A_GLOBAL);
        // Should find entry "e"
        assertThat(svA.find(E_GLOBAL,
                IStreamView.SearchDirection.FORWARD_INCLUSIVE))
                .isEqualTo(E_GLOBAL);
        // Should find entry "c"
        assertThat(svA.find(C_GLOBAL,
                IStreamView.SearchDirection.REVERSE_INCLUSIVE))
                .isEqualTo(C_GLOBAL);

        // From existing to existing:
        // Should find entry "b"
        assertThat(svB.find(D_GLOBAL,
                IStreamView.SearchDirection.REVERSE))
                .isEqualTo(B_GLOBAL);
        // Should find entry "d"
        assertThat(svB.find(B_GLOBAL,
                IStreamView.SearchDirection.FORWARD))
                .isEqualTo(D_GLOBAL);

        // Bounds:
        assertThat(svB.find(D_GLOBAL,
                IStreamView.SearchDirection.FORWARD))
                .isEqualTo(Address.NOT_FOUND);
    }

    @Test
    public void canDoPreviousOnStream()
            throws Exception
    {
        CorfuRuntime r = getDefaultRuntime().connect();
        IStreamView sv = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  A"));

        // Append some entries
        sv.append("a".getBytes());
        sv.append("b".getBytes());
        sv.append("c".getBytes());

        // Move backward should return null
        assertThat(sv.previous())
                .isNull();

        // Move forward
        sv.next(); // "a"
        sv.next(); // "b"

        // Should be now "a"
        assertThat(sv.previous().getPayload(r))
                .isEqualTo("a".getBytes());

        // Move forward, should be now "b"
        assertThat(sv.next().getPayload(r))
                .isEqualTo("b".getBytes());

        sv.next(); // "c"
        sv.next(); // null

        // Should be now "b"
        assertThat(sv.previous().getPayload(r))
                .isEqualTo("b".getBytes());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void streamCanSurviveOverwriteException()
            throws Exception {
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(0L);

        // Write to the stream, and read back. The hole should be filled.
        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
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
        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void streamWithHoleFill()
            throws Exception {
        UUID streamA = CorfuRuntime.getStreamID("stream A");

        byte[] testPayload = "hello world".getBytes();
        byte[] testPayload2 = "hello world2".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        //generate a stream hole
        TokenResponse tr =
                r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(tr.getToken().getTokenValue());


        tr = r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(tr.getToken().getTokenValue());


        sv.append(testPayload2);

        //make sure we can still read the stream.
        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo(testPayload);

        assertThat(sv.next().getPayload(getRuntime()))
                .isEqualTo(testPayload2);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void prefixTrimThrowsException()
            throws Exception {
        //begin tests
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // Write to the stream
        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        // Trim the entry
        runtime.getAddressSpaceView().prefixTrim(0);
        runtime.getAddressSpaceView().gc();
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();

        // We should get a prefix trim exception when we try to read
        assertThatThrownBy(() -> sv.next())
                .isInstanceOf(TrimmedException.class);
    }
}
