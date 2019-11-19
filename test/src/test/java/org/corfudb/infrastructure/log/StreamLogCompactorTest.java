package org.corfudb.infrastructure.log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.SMRGarbageEntry;
import org.corfudb.protocols.logprotocol.SMRGarbageRecord;
import org.corfudb.protocols.logprotocol.SMRLogEntry;
import org.corfudb.protocols.logprotocol.SMRRecord;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IToken;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by WenbinZhu on 9/6/19.
 */
public class StreamLogCompactorTest extends AbstractCorfuTest {

    private static final ISerializer SERIALIZER = Serializers.PRIMITIVE;

    private final UUID streamAId = CorfuRuntime.getStreamID("s1");
    private final UUID streamBId = CorfuRuntime.getStreamID("s2");

    private String getDirPath() {
        return PARAMETERS.TEST_TEMP_DIR;
    }

    private ServerContextBuilder getNewContextBuilder() {
        String path = getDirPath();
        return new ServerContextBuilder()
                .setLogPath(path)
                .setMemory(false);
    }

    private LogData getLogData(LogEntry entry, DataType dataType, IToken token) {
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize(entry, b);
        LogData ld = new LogData(dataType, b);
        ld.useToken(token);

        return ld;
    }

    private void generateEntry(long address, long markerAddress,
                               List<LogData> streamEntries, List<LogData> garbageEntries) {
        SMRLogEntry smrLogEntry = new SMRLogEntry();
        SMRRecord recordA = new SMRRecord("put", new Object[]{"a", String.valueOf(address)}, SERIALIZER);
        SMRRecord recordB = new SMRRecord("put", new Object[]{"b", String.valueOf(address)}, SERIALIZER);
        SMRRecord recordC = new SMRRecord("put", new Object[]{"c", String.valueOf(address)}, SERIALIZER);

        smrLogEntry.addTo(streamAId, Arrays.asList(recordA, recordB));
        smrLogEntry.addTo(streamBId, recordC);

        Map<UUID, Long> backpointerMap = new HashMap<>();
        backpointerMap.put(streamAId, address == 0 ? Address.NON_EXIST : address - 1);
        backpointerMap.put(streamBId, address == 0 ? Address.NON_EXIST : address - 1);
        TokenResponse token = new TokenResponse(new Token(-1, address), backpointerMap);
        streamEntries.add(getLogData(smrLogEntry, DataType.DATA, token));

        SMRGarbageEntry garbageEntry = new SMRGarbageEntry();
        garbageEntry.add(streamAId, 0, new SMRGarbageRecord(markerAddress, recordA.getSerializedSize()));
        garbageEntry.add(streamAId, 1, new SMRGarbageRecord(markerAddress, recordB.getSerializedSize()));
        garbageEntry.add(streamBId, 0, new SMRGarbageRecord(markerAddress, recordC.getSerializedSize()));
        garbageEntries.add(getLogData(garbageEntry, DataType.GARBAGE, new Token(-1, address)));
    }

    @Test
    public void compactionSmokeTest() {
        ServerContext sc = getNewContextBuilder()
                .setCompactionPolicyType("SNAPSHOT_LENGTH_FIRST")
                .build();
        StreamLogParams params = sc.getStreamLogParams();
        StreamLogFiles log = new StreamLogFiles(params, sc.getStreamLogDataStore());

        // Write to three segments, two segments can be compacted, the last segment is protected.
        final int numSegments = 2;
        final int numIter = StreamLogParams.RECORDS_PER_SEGMENT * numSegments + 1;

        final long skipAddress = 25L;
        List<LogData> streamEntries = new ArrayList<>();
        List<LogData> garbageEntries = new ArrayList<>();
        for (long i = 0; i < numIter; i++) {
            generateEntry(i, Address.NON_ADDRESS, streamEntries, garbageEntries);
        }

        // Un-mark one SMRRecord at skipAddress as garbage.
        SMRGarbageEntry garbageEntry = (SMRGarbageEntry) garbageEntries.get((int) skipAddress).getPayload(null);
        garbageEntry.remove(streamAId, 1);
        garbageEntries.set((int) skipAddress, getLogData(garbageEntry, DataType.GARBAGE, new Token(-1, skipAddress)));

        for (long i = 0; i < numIter; i++) {
            log.append(i, streamEntries.get((int) i));
            log.append(i, garbageEntries.get((int) i));
        }

        // This step is required as compactor can only compact addresses up to committed tail.
        log.updateCommittedTail(numIter - 1);
        log.sync(true);

        long initialQuotaAvailable = log.getLogSizeQuota().getAvailable();

        // Fist compact() will only set compaction upper bound if using SnapshotLengthFirstPolicy.
        log.getCompactor().compact();
        log.getCompactor().compact();

        // Verify stream address space bitmap.
        Map<UUID, StreamAddressSpace> streamsAddressMap = log.getLogMetadata().getStreamsAddressSpaceMap();
        Roaring64NavigableMap bitmapA = streamsAddressMap.get(streamAId).getAddressMap();
        Roaring64NavigableMap bitmapB = streamsAddressMap.get(streamBId).getAddressMap();
        assertThat(bitmapA.getLongCardinality()).isEqualTo(2);
        assertThat(bitmapA.contains(skipAddress)).isEqualTo(true);
        assertThat(bitmapA.contains(numIter - 1)).isEqualTo(true);
        assertThat(bitmapB.getLongCardinality()).isEqualTo(1);
        assertThat(bitmapB.contains(numIter - 1)).isEqualTo(true);

        // Verify the address at skipAddress is partially compacted.
        LogData ld = log.read(skipAddress);
        SMRLogEntry entry = (SMRLogEntry) ld.getPayload(null);
        SMRRecord recordB = new SMRRecord("put", new Object[]{"b", String.valueOf(skipAddress)}, SERIALIZER);
        recordB.setGlobalAddress(skipAddress);
        assertThat(entry.getStreams().size()).isEqualTo(1);
        assertThat(entry.getSMRUpdates(streamAId)).isEqualTo(Arrays.asList(SMRRecord.COMPACTED_RECORD, recordB));

        // Verify reading a compacted address can return a compacted LogData.
        for (long i = 0; i < numIter; i++) {
            if (i != skipAddress && i < StreamLogParams.RECORDS_PER_SEGMENT * numSegments) {
                assertThat(log.read(i).getType()).isEqualTo(DataType.COMPACTED);
            } else {
                assertThat(log.read(i).getType()).isEqualTo(DataType.DATA);
            }
        }

        assertThat(log.getLogSizeQuota().getAvailable()).isGreaterThan(initialQuotaAvailable);
    }

    @Test
    public void testConcurrentReadWriteDuringCompaction() throws Exception {
        ServerContext sc = getNewContextBuilder()
                .setCompactionPolicyType("SNAPSHOT_LENGTH_FIRST")
                .build();
        StreamLogParams params = sc.getStreamLogParams();
        StreamLogFiles log = new StreamLogFiles(params, sc.getStreamLogDataStore());

        // Write to at least two segments, second segment is protected (not compacted).
        final int numIter = StreamLogParams.RECORDS_PER_SEGMENT + 1;

        List<LogData> streamEntries = new ArrayList<>();
        List<LogData> garbageEntries = new ArrayList<>();

        for (long i = 0; i < numIter; i++) {
            generateEntry(i, Address.NON_ADDRESS, streamEntries, garbageEntries);
        }

        List<LogData> evenGarbageEntries = garbageEntries.stream()
                .filter(e -> e.getGlobalAddress() % 2 == 0)
                .collect(Collectors.toList());

        for (LogData ld : streamEntries) {
            log.append(ld.getGlobalAddress(), ld);
        }
        // Write half of the garbage entries to the log.
        for (LogData ld : evenGarbageEntries) {
            log.append(ld.getGlobalAddress(), ld);
        }

        // This step is required as compactor can only compact addresses up to committed tail.
        log.updateCommittedTail(numIter - 1);
        log.sync(true);

        final int numThreads = 4;
        AtomicBoolean compactionFinished = new AtomicBoolean(false);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        // Do compaction.
        executor.submit(() -> {
            // Fist compact() will only set compaction upper bound if using SnapshotLengthFirstPolicy.
            log.getCompactor().compact();
            log.getCompactor().compact();
            compactionFinished.set(true);
        });

        // Verify concurrent writes will not break compaction.
        executor.submit(() -> {
            while (!compactionFinished.get()) {
                for (long i = 0; i < StreamLogParams.RECORDS_PER_SEGMENT; i++) {
                    final long address = i;
                    assertThatThrownBy(() -> log.append(address, LogData.getHole(address)))
                            .hasCauseExactlyInstanceOf(OverwriteException.class);
                }
            }
        });

        // Verify concurrent reads can succeed.
        executor.submit(() -> {
            while (!compactionFinished.get()) {
                for (long i = 0; i < StreamLogParams.RECORDS_PER_SEGMENT; i++) {
                    final long address = i;
                    assertThatCode(() -> log.read(address)).doesNotThrowAnyException();
                }
            }
        });

        // Verify concurrent reads can succeed.
        executor.submit(() -> {
            while (!compactionFinished.get()) {
                for (long i = StreamLogParams.RECORDS_PER_SEGMENT - 1; i >= 0; i--) {
                    final long address = i;
                    assertThatCode(() -> log.read(address)).doesNotThrowAnyException();
                }
            }
        });

        final int waitTimeout = 60;
        executor.shutdown();
        executor.awaitTermination(waitTimeout, TimeUnit.SECONDS);

        // Verify address either compacted or remains.
        for (long i = 0; i < StreamLogParams.RECORDS_PER_SEGMENT; i++) {
            if (i % 2 == 0) {
                assertThat(log.read(i).getType()).isEqualTo(DataType.COMPACTED);
            } else {
                assertThat(log.read(i).getType()).isEqualTo(DataType.DATA);
            }
        }
    }

    @Test
    public void testMultipleCompactionCycles() {
        ServerContext sc = getNewContextBuilder()
                .setCompactionPolicyType("SNAPSHOT_LENGTH_FIRST")
                .build();
        StreamLogParams params = sc.getStreamLogParams();
        StreamLogFiles log = new StreamLogFiles(params, sc.getStreamLogDataStore());

        // Write to at least two segments, second segment is protected (not compacted).
        final int numIter = StreamLogParams.RECORDS_PER_SEGMENT + 1;

        List<LogData> streamEntries = new ArrayList<>();
        List<LogData> garbageEntries = new ArrayList<>();

        for (long i = 0; i < numIter; i++) {
            generateEntry(i, Address.NON_ADDRESS, streamEntries, garbageEntries);
        }

        List<LogData> evenGarbageEntries = garbageEntries.stream()
                .filter(e -> e.getGlobalAddress() % 2 == 0)
                .collect(Collectors.toList());
        List<LogData> oddGarbageEntries = garbageEntries.stream()
                .filter(e -> e.getGlobalAddress() % 2 != 0)
                .collect(Collectors.toList());

        // Write all stream entries and half of the garbage entries to the log.
        log.append(streamEntries);
        for (LogData ld : evenGarbageEntries) {
            log.append(ld.getGlobalAddress(), ld);
        }

        // This step is required as compactor can only compact addresses up to committed tail.
        log.updateCommittedTail(numIter - 1);
        log.sync(true);

        // Fist compact() will only set compaction upper bound if using SnapshotLengthFirstPolicy.
        log.getCompactor().compact();
        log.getCompactor().compact();

        for (long i = 0; i < StreamLogParams.RECORDS_PER_SEGMENT; i++) {
            if (i % 2 == 0) {
                assertThat(log.read(i).getType()).isEqualTo(DataType.COMPACTED);
            } else {
                assertThat(log.read(i).getType()).isEqualTo(DataType.DATA);
            }
        }

        // Write the rest of garbage entries to the log.
        for (LogData ld : oddGarbageEntries) {
            log.append(ld.getGlobalAddress(), ld);
        }
        log.sync(true);

        // Verify all entries are compacted after the second compaction cycle.
        log.getCompactor().compact();
        for (long i = 0; i < StreamLogParams.RECORDS_PER_SEGMENT; i++) {
            assertThat(log.read(i).getType()).isEqualTo(DataType.COMPACTED);
        }

        // Verify the third compaction cycle does not do anything wrong.
        log.getCompactor().compact();
        for (long i = 0; i < StreamLogParams.RECORDS_PER_SEGMENT; i++) {
            assertThat(log.read(i).getType()).isEqualTo(DataType.COMPACTED);
        }
    }

    @Test
    public void testCompactionMark() {
        ServerContext sc = getNewContextBuilder()
                .setCompactionPolicyType("SNAPSHOT_LENGTH_FIRST")
                .build();
        StreamLogParams params = sc.getStreamLogParams();
        StreamLogFiles log = new StreamLogFiles(params, sc.getStreamLogDataStore());

        // Write to at least two segments, second segment is protected (not compacted).
        final int numIter = StreamLogParams.RECORDS_PER_SEGMENT + 1;
        final long uncompactedAddress = 25L;
        final long uncompactedMarkerAddress = StreamLogParams.RECORDS_PER_SEGMENT / 2 + 1;
        final long maxCompactionMarkAddress = 75;
        final long maxCompactionMark = maxCompactionMarkAddress + 1;

        List<LogData> streamEntries = new ArrayList<>();
        List<LogData> garbageEntries = new ArrayList<>();

        for (long i = 0; i < numIter; i++) {
            if (i == uncompactedAddress) {
                generateEntry(i, uncompactedMarkerAddress, streamEntries, garbageEntries);
            } else if (i == maxCompactionMarkAddress) {
                generateEntry(i, maxCompactionMark, streamEntries, garbageEntries);
            } else {
                generateEntry(i, Address.NON_ADDRESS, streamEntries, garbageEntries);
            }
        }

        List<LogData> firstHalfStreamEntries = streamEntries.stream()
                .filter(e -> e.getGlobalAddress() < StreamLogParams.RECORDS_PER_SEGMENT / 2)
                .collect(Collectors.toList());
        List<LogData> secondHalfStreamEntries = streamEntries.stream()
                .filter(e -> e.getGlobalAddress() >= StreamLogParams.RECORDS_PER_SEGMENT / 2)
                .collect(Collectors.toList());

        log.append(firstHalfStreamEntries);

        // This step is required as compactor can only compact addresses up to committed tail.
        log.updateCommittedTail(secondHalfStreamEntries.get(0).getGlobalAddress() - 1);
        log.sync(true);

        // Fist compact() will set compaction upper bound if using SnapshotLengthFirstPolicy.
        log.getCompactor().compact();

        assertThat(log.getLogTail()).isEqualTo(StreamLogParams.RECORDS_PER_SEGMENT / 2 - 1);
        assertThat(log.getGlobalCompactionMark()).isEqualTo(Address.NON_ADDRESS);

        log.append(secondHalfStreamEntries);
        log.append(garbageEntries);

        // This step is required as compactor can only compact addresses up to committed tail.
        log.updateCommittedTail(numIter - 1);
        log.sync(true);

        // Compact entries whose marker address is up to StreamLogParams.RECORDS_PER_SEGMENT / 2 - 1
        log.getCompactor().compact();

        for (long i = 0; i < StreamLogParams.RECORDS_PER_SEGMENT; i++) {
            if (i == uncompactedAddress) {
                assertThat(log.read(i).getType()).isEqualTo(DataType.DATA);
            } else {
                assertThat(log.read(i).getType()).isEqualTo(DataType.COMPACTED);
            }
        }
        assertThat(log.getGlobalCompactionMark()).isEqualTo(maxCompactionMark);
    }
}
