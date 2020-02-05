package org.corfudb.runtime.view;

import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.logprotocol.SMRGarbageEntry;
import org.corfudb.protocols.logprotocol.SMRGarbageRecord;
import org.corfudb.protocols.logprotocol.SMRLogEntry;
import org.corfudb.protocols.logprotocol.SMRRecord;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.GarbageInformer;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.log.StreamLogParams.RECORDS_PER_SEGMENT;

/**
 * Created by mwei on 1/8/16.
 */
public class StreamViewTest extends AbstractViewTest {

    public CorfuRuntime r;

    @Before
    public void setRuntime() throws Exception {
        ServerContextBuilder serverContextBuilder = new ServerContextBuilder()
                .setMemory(false)
                .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                .setCompactionPolicyType("GARBAGE_SIZE_FIRST")
                .setSegmentGarbageRatioThreshold("0")
                .setSegmentGarbageSizeThresholdMB("0");

        r = getDefaultRuntime(serverContextBuilder).connect();
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
    public void testCompactionMark() {
        UUID streamId = CorfuRuntime.getStreamID("stream A");
        IStreamView sv = r.getStreamsView().get(streamId);

        final int entryNum = RECORDS_PER_SEGMENT + 1;
        final long garbageAddress = 1L;
        int smrEntrySize = 0;

        // writes data to global address
        for (int i = 0; i <= entryNum; ++i) {
            SMRRecord smrRecord = new SMRRecord("hi", new Object[]{("hello" + i)}, Serializers.JSON);
            SMRLogEntry smrLogEntry = new SMRLogEntry();
            smrLogEntry.addTo(streamId, Collections.singletonList(smrRecord));
            sv.append(smrLogEntry);
            if (i == garbageAddress) {
                smrEntrySize = smrRecord.getSerializedSize();
            }
        }

        // Update committed tail so that compactor can run.
        Utils.updateCommittedTail(r.getLayoutView().getLayout(), r, entryNum - 1);

        // write one synthesized garbage decision
        final long markerAddress = 3L;
        SMRGarbageRecord garbageRecord = new SMRGarbageRecord(markerAddress, smrEntrySize);
        SMRGarbageEntry smrGarbageEntry = new SMRGarbageEntry();
        smrGarbageEntry.add(streamId, 0, garbageRecord);
        smrGarbageEntry.setGlobalAddress(garbageAddress);
        GarbageInformer.GarbageBatch garbageBatch =
                new GarbageInformer.GarbageBatch(Collections.singletonList(smrGarbageEntry));
        GarbageInformer garbageInformer = new GarbageInformer(r);
        garbageInformer.sendGarbageBatch(garbageBatch);

        // run compaction on LogUnit servers
        getLogUnit(SERVERS.PORT_0).runCompaction();
        getRuntime().getAddressSpaceView().resetCaches();
        getRuntime().getAddressSpaceView().invalidateServerCaches();

        // Assert skipping compacted entry
        sv.seek(0L);
        sv.next();
        ILogData data = sv.next();
        SMRLogEntry logEntry = (SMRLogEntry) data.getPayload(r);
        assertThat(logEntry.getSMRUpdates(streamId).get(0).getSMRArguments())
                .isEqualTo(new Object[]{("hello" + 2)});
    }
}
