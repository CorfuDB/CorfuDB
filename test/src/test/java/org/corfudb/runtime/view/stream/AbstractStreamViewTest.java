package org.corfudb.runtime.view.stream;

import static org.assertj.core.api.Assertions.assertThat;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.util.Sleep;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests the BackpointerStreamView
 * <p>
 * Created by zlokhandwala on 5/24/17.
 */
public abstract class AbstractStreamViewTest extends AbstractViewTest {

    public AbstractStreamViewTest(boolean followBackpointers) {
        super(followBackpointers);
    }

    /**
     * Tests the hasNext functionality of the streamView.
     */
    @Test
    public void hasNextTest() {
        CorfuRuntime runtime = getDefaultRuntime();

        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        sv.append("hello world".getBytes());

        assertThat(sv.hasNext()).isTrue();
        sv.next();
        assertThat(sv.hasNext()).isFalse();
    }

    /**
     * tests navigating forward/backward on a stream,
     * with intermittent appends to the stream.
     *
     * in addition to correctness assertions, this test can be used for
     * single-stepping with a debugger and observing stream behavior.
     */
    @Test
    public void readQueueTest() {
        CorfuRuntime runtime = getDefaultRuntime();
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        final int ten = 10;

        // initially, populate the stream with appends
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sv.append(String.valueOf(i).getBytes());
        }

        // traverse the stream forward while periodically (every ten
        // iterations) appending to it
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            assertThat(sv.hasNext()).isTrue();
            byte[] payLoad = (byte[]) sv.next().getPayload(runtime);
            assertThat(new String(payLoad).equals(String.valueOf(i)))
                    .isTrue();
            assertThat(sv.getCurrentGlobalPosition()).isEqualTo(i);

            if (i % ten == 1) {
                for (int j = 0; j < PARAMETERS.NUM_ITERATIONS_VERY_LOW; j++) {
                    sv.append(String.valueOf(i).getBytes());
                }

            }
        }

        // traverse the stream backwards, while periodically (every ten
        // iterations) appending to it
        for (int i = PARAMETERS.NUM_ITERATIONS_LOW - 1; i >= 0; i--) {
            byte[] payLoad = (byte[]) sv.current().getPayload(runtime);
            assertThat(new String(payLoad).equals(String.valueOf(i)))
                    .isTrue();
            assertThat(sv.getCurrentGlobalPosition()).isEqualTo(i);
            sv.previous();

            if (i % ten == 1) {
                for (int j = 0; j < PARAMETERS.NUM_ITERATIONS_VERY_LOW; j++) {
                    sv.append(String.valueOf(i).getBytes());
                }

            }
        }
    }

    /** Test if seeking the stream after resetting and then calling previous
     *  returns the correct entry.
     */
    @Test
    public void seekSkipTest() {
        CorfuRuntime runtime = getDefaultRuntime();
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        final byte[] ENTRY_0 = {0};
        final byte[] ENTRY_1 = {1};
        final byte[] ENTRY_2 = {2};

        sv.append(ENTRY_0);
        sv.append(ENTRY_1);
        sv.append(ENTRY_2);
        sv.reset();

        // This moves the stream pointer so the NEXT read will be 2
        // (the pointer is at 1).
        sv.seek(2);

        // The previous entry should be ENTRY_0
        assertThat((byte[])sv.previous().getPayload(runtime))
                .isEqualTo(ENTRY_0);
    }


    @Test
    public void moreReadQueueTest() {
        CorfuRuntime runtime = getDefaultRuntime();
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));

        // initially, populate the stream with appends
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            sv.append(String.valueOf(i).getBytes());
        }

        // simple traverse to end of stream
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            assertThat(sv.hasNext()).isTrue();
            sv.next();
        }

        // add two entries on alternate steps, and traverse forward one at a
        // time
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            if (i % 2 == 0) {
                assertThat(sv.hasNext()).isFalse();
                sv.append(String.valueOf(i).getBytes());
                sv.append(String.valueOf(i).getBytes());
            }
            byte[] payLoad = (byte[]) sv.next().getPayload(runtime);
            assertThat(new String(payLoad).equals(String.valueOf(i)));
        }
    }

    /**
     *  test proper backpointer termination at the head of a stream
     *
     * */
    @Test
    public void headOfStreamBackpointerTermination() {

        final int totalEntries = PARAMETERS.NUM_ITERATIONS_LOW + 1;
        CorfuRuntime runtime = getDefaultRuntime();

        // Create StreamA (100 entries)
        IStreamView svA = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            svA.append(String.valueOf(i).getBytes());
        }

        // Create StreamB (1 entry)
        IStreamView svB = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamB"));
        svB.append(String.valueOf(0).getBytes());

        // Fetch Stream B and verify backpointer count (which requires 1 read = 1 entry)
        svB.remainingUpTo(totalEntries);
        assertThat(((ThreadSafeStreamView) svB).getUnderlyingStream().getTotalUpdates()).isEqualTo(1L);
    }

    @Ignore
    @Test
    public void testStreamGC() throws Exception {
        CorfuRuntime runtime = getDefaultRuntime();

        IStreamView svA = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        IStreamView svB = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));

        // Since both steam views open the same stream, a write to one stream
        // should be reflected in the other
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            svA.append(String.valueOf(i).getBytes());
        }

        // Make sure that the stream is built in-memory
        IStreamView bpsvA = ((ThreadSafeStreamView) svA).getUnderlyingStream();
        IStreamView bpsvB = ((ThreadSafeStreamView) svA).getUnderlyingStream();
        assertThat(svA.remaining()).hasSize(PARAMETERS.NUM_ITERATIONS_LOW);
        assertThat(svB.remaining()).hasSize(PARAMETERS.NUM_ITERATIONS_LOW);
        assertThat(((AbstractQueuedStreamView) bpsvA).getContext().resolvedQueue).hasSize(PARAMETERS.NUM_ITERATIONS_LOW);
        assertThat(((AbstractQueuedStreamView) bpsvB).getContext().resolvedQueue).hasSize(PARAMETERS.NUM_ITERATIONS_LOW);
        TokenResponse tail = runtime.getSequencerView().query();
        runtime.getAddressSpaceView().prefixTrim(tail.getToken());
        // First Runtime GC
        runtime.getGarbageCollector().runRuntimeGC();

        // Additional append to move the pointer
        svA.append(String.valueOf(PARAMETERS.NUM_ITERATIONS_LOW).getBytes());
        bpsvA = ((ThreadSafeStreamView) svA).getUnderlyingStream();
        bpsvB = ((ThreadSafeStreamView) svA).getUnderlyingStream();
        assertThat(svA.remaining()).hasSize(1);
        assertThat(svB.remaining()).hasSize(1);

        // Second Runtime GC
        runtime.getGarbageCollector().runRuntimeGC();
        assertThat(((AbstractQueuedStreamView) bpsvA).getContext().resolvedQueue).hasSize(1);
        assertThat(((AbstractQueuedStreamView) bpsvA).getContext().readQueue).isEmpty();
        assertThat(((AbstractQueuedStreamView) bpsvA).getContext().readCpQueue).isEmpty();
    }

    final int trimMark = PARAMETERS.NUM_ITERATIONS_LOW / 2;
    final int traverseMark = PARAMETERS.NUM_ITERATIONS_LOW / 4;

    /**
     * This test verifies that we cannot traverse a trimmed stream. Steps to reproduce this test are:
     *
     * (1) Append 100 entries to stream.
     * (2) Traverse stream to a given point (up to address 24 using the 'next' api)
     * (3) Trim the stream at prefix 50.
     * (4) Traverse the remaining of the stream (up to 99)
     *
     * TrimmedException should be thrown.
     */
    @Test
    public void traverseTrimmedStreamIgnoreTrim() throws Exception {

        // Append entries to stream and traverse to some point before the trim mark
        // leaving a gap between the traversed point and the trim mark.
        IStreamView sv = traverseStreamBeforeTrimMark(true);

        // Traverse remaining of the stream, because trimmed exceptions are ignored
        // we should get the remaining entries in the stream (trimMark - end of stream).
        List<ILogData> stream = sv.remaining();
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(PARAMETERS.NUM_ITERATIONS_LOW - 1);
        assertThat(stream.size()).isEqualTo(trimMark - 1);

        int remainingCounter = trimMark + 1;
        for (ILogData data: stream) {
            byte[] payLoad = (byte[]) data.getPayload(this.getRuntime());
            assertThat(new String(payLoad)).isEqualTo(String.valueOf(remainingCounter));
            remainingCounter++;
        }
    }

    /**
     * This test verifies that we can traverse a trimmed stream when
     * the ignore trimmed flag is set. Steps to reproduce this test are:
     * (1) Append 100 entries to stream.
     * (2) Traverse stream to a given point (up to address 24 using the 'next' api)
     * (3) Trim the stream at prefix 50.
     * (4) Traverse the remaining of the stream (up to 99)
     *
     * TrimmedException should be ignored and step (4) should retrieve only 49 entries (as 25-50 are trimmed).
     */
    @Test(expected = TrimmedException.class)
    public void traverseTrimmedStreamDontIgnoreTrim() throws Exception {
        // Append entries to stream and traverse to some point before the trim mark
        // leaving a gap between the traversed point and the trim mark.
        IStreamView sv = traverseStreamBeforeTrimMark(false);

        // Attempt to traverse remaining of the stream
        sv.remaining();
    }

    private IStreamView traverseStreamBeforeTrimMark(boolean ignoreTrimmed)
            throws InterruptedException {

        CorfuRuntime runtime = getDefaultRuntime();
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(ignoreTrimmed)
                .build();
        IStreamView sv = runtime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"), options);
        final long epoch = 0L;
        final Duration waitForTrim = Duration.ofSeconds(5);

        // Populate stream with NUM_ITERATIONS_LOW entries
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
            sv.append(String.valueOf(i).getBytes());
        }

        // Traverse stream until syncMark
        for (int i = 0; i < traverseMark; i++) {
            assertThat(sv.hasNext()).isTrue();
            byte[] payLoad = (byte[]) sv.next().getPayload(runtime);
            assertThat(new String(payLoad).equals(String.valueOf(i)))
                    .isTrue();
            assertThat(sv.getCurrentGlobalPosition()).isEqualTo(i);
        }

        // Force a prefix trim
        runtime.getAddressSpaceView().prefixTrim(new Token(epoch, trimMark));

        // Wait until log is trimmed
        TimeUnit.MILLISECONDS.sleep(waitForTrim.toMillis());
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();

        return sv;
    }

}
