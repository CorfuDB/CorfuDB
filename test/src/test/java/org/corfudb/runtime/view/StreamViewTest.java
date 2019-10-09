package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 1/8/16.
 */
public class StreamViewTest extends AbstractViewTest {

    public CorfuRuntime r;

    @Before
    public void setRuntime() throws Exception {
        r = getDefaultRuntime().connect();
    }

    @Test
    public void nonCacheableStream() {
        // Create a producer/consumer, where the consumer client opens two streams, one that is
        // suppose to be cached and the other shouldn't and verify that the address space cache
        // only contains entries for one stream
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        CorfuRuntime producer = getNewRuntime(getDefaultNode())
                .connect();

        IStreamView sv1 = producer.getStreamsView().get(id1);
        IStreamView sv2 = producer.getStreamsView().get(id2);

        final int numWrites = 300;
        final int payloadSize = 100;
        final byte[] payload = new byte[payloadSize];

        for (int x = 0; x < numWrites; x++) {
            sv1.append(payload);
            sv2.append(payload);
        }

        CorfuRuntime consumer = getNewRuntime(getDefaultNode())
                .connect();

        StreamOptions cacheStreamOption = StreamOptions.builder()
                .cacheEntries(true)
                .build();

        StreamOptions noCacheStreamOption = StreamOptions.builder()
                .cacheEntries(false)
                .build();

        IStreamView cachedStream = consumer.getStreamsView().get(id1, cacheStreamOption);
        IStreamView nonCacheableStream = consumer.getStreamsView().get(id2, noCacheStreamOption);

        cachedStream.next();
        nonCacheableStream.next();

        final long syncAddress1 = 5;
        final long syncAddress2 = 100;
        final long syncAddress3 = 200;


        cachedStream.nextUpTo(syncAddress1);
        nonCacheableStream.nextUpTo(syncAddress1);

        cachedStream.remainingUpTo(syncAddress2);
        nonCacheableStream.remainingUpTo(syncAddress2);

        cachedStream.streamUpTo(syncAddress3);
        nonCacheableStream.streamUpTo(syncAddress3);

        cachedStream.remaining();
        nonCacheableStream.remaining();

        // After syncing to the tail verify that the cache only contains stream entries from the cached stream
        assertThat(consumer.getAddressSpaceView().getReadCache().size()).isEqualTo(numWrites);

        for (ILogData ld : consumer.getAddressSpaceView().getReadCache().asMap().values()) {
            assertThat(ld.hasBackpointer(id1)).isTrue();
            assertThat(ld.getBackpointerMap()).hasSize(1);
        }
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

        Token token = new Token(runtime.getLayoutView().getLayout().getEpoch(), (firstIter - 1) / 2);
        runtime.getAddressSpaceView().prefixTrim(token);
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();

        entries = txStream.remainingUpTo((firstIter - 1) / 2);
        assertThat(entries.size()).isEqualTo(0);

        entries = txStream.remainingUpTo(firstIter);
        assertThat(entries.size()).isEqualTo((firstIter / 2));

        // Open the stream with a new client
        CorfuRuntime rt2 = getNewRuntime(getDefaultNode())
                                        .connect();
        IStreamView txStream2 = rt2.getStreamsView()
                 .get(ObjectsView.TRANSACTION_STREAM_ID, options);
        assertThatThrownBy(() -> txStream2.remaining())
                .isInstanceOf(TrimmedException.class);

        txStream2.seek(firstIter / 2);
        entries = txStream2.remainingUpTo(Long.MAX_VALUE);
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
        r.getSequencerView().next(streamA);

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
                r.getSequencerView().next(streamA);

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(tr.getToken().getSequence());


        tr = r.getSequencerView().next(streamA);

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(tr.getToken().getSequence());


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
        Token token = new Token(runtime.getLayoutView().getLayout().getEpoch(), 0);
        runtime.getAddressSpaceView().prefixTrim(token);
        runtime.getAddressSpaceView().gc();
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();

        // We should get a prefix trim exception when we try to read
        assertThatThrownBy(() -> sv.next())
                .isInstanceOf(TrimmedException.class);
    }

    @Test
    public void testPreviousWithNoCheckpoints() {
        IStreamView sv = r.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(Address.NON_ADDRESS);
        assertThat(sv.current()).isNull();
        final long pos0 = 0l;
        byte[] payload = "entry1".getBytes();
        assertThat(sv.append(payload)).isEqualTo(pos0);
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(Address.NON_ADDRESS);
        assertThat(sv.next().getPayload(r)).isEqualTo(payload);
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(pos0);
        assertThat(sv.previous()).isNull();
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(Address.NON_ADDRESS);
        // Calling previous(again) when the stream pointer is pointing to the beginning shouldn't
        // change the stream pointer
        assertThat(sv.previous()).isNull();
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(Address.NON_ADDRESS);
    }

    @Test
    public void testPreviousWithStreamCheckpoint() throws Exception {
        String stream = "stream1";
        Map<String, String> map = r.getObjectsView()
                .build()
                .setStreamName(stream)
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .open();

        map.put("k1", "k1");

        UUID streamId = CorfuRuntime.getStreamID(stream);
        long baseVersion = r.getSequencerView().query(streamId);

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(map);
        Token token = mcw.appendCheckpoints(r, "cp");

        // Add some more write after the checkpoint
        map.put("k2", "k2");
        map.put("k3", "k3");

        Map<String, String> mapCopy = r.getObjectsView()
                .build()
                .setStreamName(stream)
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setOptions(Collections.singleton(ObjectOpenOptions.NO_CACHE))
                .open();

        mapCopy.size();
        VersionLockedObject vlo = ((CorfuCompileProxy) ((ICorfuSMR) mapCopy).
                getCorfuSMRProxy()).getUnderlyingObject();

        Token finalVersion = r.getSequencerView().query().getToken();

        IStreamView sv = r.getStreamsView().get(CorfuRuntime.getStreamID(stream));

        // Need to call this to bump up the stream pointer
        // TODO(Maithem): other streaming APIs seem to be broken (i.e. they don't increment
        // the stream pointer).
        while (sv.nextUpTo(Long.MAX_VALUE) != null);

        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(finalVersion.getSequence());
        assertThat(sv.previous()).isNotNull();
        assertThat(sv.previous()).isNull();
        // This +1 represents the checkpoint NO_OP entry
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(baseVersion + 1);
        // Calling previous on a stream when the pointer points to a a base checkpoint
        // should throw a TrimmedException
        assertThatThrownBy(() -> sv.previous()).isInstanceOf(TrimmedException.class);
        assertThat(sv.getCurrentGlobalPosition()).isEqualTo(baseVersion + 1);
    }

    /**
     * Perform a prefix trim on the given runtime, using the provided parameters. Once th trim
     * has been issue, wait for the change to take an effect.
     *
     * @param runtime on which we are operating
     * @param epoch associated with the prefix trim
     * @param address associated with the prefix trim
     */
    private void synchronousPrefixTrim(CorfuRuntime runtime, long epoch, long address) {
        final Token prevTrimMark = runtime.getAddressSpaceView().getTrimMark();
        runtime.getAddressSpaceView().prefixTrim(new Token(epoch, address));
        while (prevTrimMark.equals(runtime.getAddressSpaceView().getTrimMark())) { }
    }

    /**
     * Ensure that transaction stream semantics are correct with respect to different
     * combinations of trim points and seek positions.
     *
     * @throws InterruptedException
     */
    @Test
    public void txLogTrim() {
        final CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();
        final CorfuRuntime localRuntime = CorfuRuntime.fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(getDefaultConfigurationString())
                .connect();
        final CorfuTable<String, String>
                instance1 = localRuntime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                .setStreamName("txTestMap")
                .open();

        // Populate the Transaction Stream up to NUM_ITERATIONS_LOW entries.
        IntStream.range(0, PARAMETERS.NUM_ITERATIONS_LOW).forEach(idx -> {
            localRuntime.getObjectsView().TXBegin();
            instance1.put(String.valueOf(idx), String.valueOf(idx));
            localRuntime.getObjectsView().TXEnd();
        });

        // Force a prefix trim.
        final long epoch = 0L;
        synchronousPrefixTrim(localRuntime, epoch, PARAMETERS.NUM_ITERATIONS_LOW/2/2);

        // Wait until log is trimmed.
        localRuntime.getAddressSpaceView().invalidateServerCaches();
        localRuntime.getAddressSpaceView().invalidateClientCache();

        IStreamView txStream = localRuntime.getStreamsView()
                .get(ObjectsView.TRANSACTION_STREAM_ID);

        // If the current pointer is below the trim point, we should see the TrimmedException.
        assertThatThrownBy(() -> txStream.remaining()).isInstanceOf(TrimmedException.class);

        // Seek the transaction stream beyond the trim point. We should not see any issues.
        txStream.seek(PARAMETERS.NUM_ITERATIONS_LOW/2);
        txStream.remaining();

        // Populate the transaction stream with additional NUM_ITERATIONS_LOW entries.
        IntStream.range(0, PARAMETERS.NUM_ITERATIONS_LOW).forEach(idx -> {
            localRuntime.getObjectsView().TXBegin();
            instance1.put(String.valueOf(idx), String.valueOf(idx));
            localRuntime.getObjectsView().TXEnd();
        });

        // Force a prefix trim beyond our current pointer.
        synchronousPrefixTrim(localRuntime, epoch,
                PARAMETERS.NUM_ITERATIONS_LOW + PARAMETERS.NUM_ITERATIONS_LOW/2);

        // Wait until log is trimmed.
        assertThatThrownBy(() -> txStream.remaining()).isInstanceOf(TrimmedException.class);

        // Ensure that we can recover.
        txStream.seek(localRuntime.getSequencerView().query().getSequence());
        txStream.remaining();
    }
}
