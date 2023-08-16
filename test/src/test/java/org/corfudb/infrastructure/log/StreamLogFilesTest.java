package org.corfudb.infrastructure.log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.BatchProcessor.BatchProcessorContext;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.log.FileSystemAgent.FileSystemConfig;
import org.corfudb.infrastructure.log.LogFormat.LogHeader;
import org.corfudb.infrastructure.log.LogFormat.Metadata;
import org.corfudb.infrastructure.log.StreamLog.PersistenceMode;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Address;
import org.corfudb.test.LsofSpec;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.Segment.METADATA_SIZE;
import static org.corfudb.infrastructure.log.Segment.VERSION;
import static org.corfudb.infrastructure.log.SegmentUtils.getByteBufferWithMetaData;
import static org.corfudb.infrastructure.log.StreamLogFiles.RECORDS_PER_LOG_FILE;
import static org.corfudb.infrastructure.utils.Crc32c.getChecksum;
import static org.mockito.Mockito.when;

/**
 * Created by maithem on 11/2/16.
 */
@SuppressWarnings("checkstyle:magicnumber")
public class StreamLogFilesTest extends AbstractCorfuTest {

    private String getDirPath() {
        return PARAMETERS.TEST_TEMP_DIR;
    }

    private ServerContext getContext() {
        String path = getDirPath();
        return new ServerContextBuilder()
                .setLogPath(path)
                .setMemory(false)
                .build();
    }

    /**
     * Reproduces the issue, with an exception happens during segment handle creation.
     * The test checks that there are no any open file channels due to exception.
     *
     * @throws Exception spec exception
     */
    @Test
    public void testProtectionFromDataLossInCaseOfExceptionDuringDataLoad() throws Exception {
        String logDir = com.google.common.io.Files.createTempDir().getAbsolutePath();
        Path logPath = Paths.get(logDir, "log", "0.log");

        ServerContext context = new ServerContextBuilder()
                .setLogPath(logDir)
                .setMemory(false)
                .build();

        StreamLogFiles log = new StreamLogFiles(context, new BatchProcessorContext());

        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        long address0 = 0;
        log.append(address0, new LogData(DataType.DATA, b));

        final int OVERWRITE_BYTES = 4;

        try (RandomAccessFile logFile = new RandomAccessFile(logPath.toFile(), "rw")) {
            logFile.seek(1);
            logFile.writeInt(OVERWRITE_BYTES);
        }

        log.closeSegmentHandlers(0);
        try {
            log.read(address0);
            throw new IllegalStateException("Must not get here");
        } catch (DataCorruptionException ex) {
            //ignore
        }

        LsofSpec lsOfSpec = new LsofSpec();
        lsOfSpec.check(logPath);

        log.close();
    }
    
    @Test
    public void testWriteReadWithChecksum() throws Exception {
        // Enable checksum, then append and read the same entry
        StreamLog log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        long address0 = 0;
        log.append(address0, new LogData(DataType.DATA, b));
        assertThat(log.read(address0).getPayload(getRuntime())).isEqualTo(streamEntry);

        // Disable checksum, then append and read then same entry
        // An overwrite exception should occur, since we are writing the
        // same entry.
        final StreamLog newLog = new StreamLogFiles(getContext(), new BatchProcessorContext());
        assertThatThrownBy(() -> newLog.append(address0, new LogData(DataType.DATA, b)))
                .isInstanceOf(OverwriteException.class);

        assertThat(log.read(address0).getPayload(getRuntime())).isEqualTo(streamEntry);
    }

    @Test
    public void testBatchWrite() throws Exception {
        ServerContext sc = getContext();
        StreamLog log = new StreamLogFiles(sc, new BatchProcessorContext());

        // A range write that spans two segments
        final int numSegments = 2;
        final int numIter = StreamLogFiles.RECORDS_PER_LOG_FILE * numSegments;
        List<LogData> writeEntries = new ArrayList<>();
        for (int x = 0; x < numIter; x++) {
            writeEntries.add(getEntry(x));
        }

        log.append(writeEntries);
        log.sync(true);

        StreamLog log2 = new StreamLogFiles(sc, new BatchProcessorContext());
        List<LogData> readEntries = readRange(0, numIter, log2);
        assertThat(writeEntries).isEqualTo(readEntries);
    }

    @Test
    public void testRangeWriteTrim() throws Exception {
        StreamLog log = new StreamLogFiles(getContext(), new BatchProcessorContext());

        // A range write that has the first 25% of its entries trimmed
        final int numEntries = 1000;
        final double trimmedRatio = .25;
        List<LogData> entries = new ArrayList<>();

        for (int x = 0; x < numEntries; x++) {
            if (x < trimmedRatio * numEntries) {
                entries.add(LogData.getTrimmed(x));
            } else {
                entries.add(getEntry(x));
            }
        }

        long trimMarkBeforeWrite = log.getTrimMark();

        assertThatThrownBy(() -> log.append(entries))
                .isInstanceOf(OverwriteException.class);
        // Verify that the range wasn't partially written
        assertThat(log.getLogTail()).isEqualTo(Address.NON_ADDRESS);
        assertThat(log.getTrimMark()).isEqualTo(trimMarkBeforeWrite);
    }

    @Test
    public void testRangeWriteAfterPrefixTrim() throws Exception {
        // This test tries to write a range right after a part of the range to be
        // written is prefix trimmed.
        ServerContext sc = getContext();
        StreamLog log = new StreamLogFiles(sc, new BatchProcessorContext());
        final long trimMark = 1000;
        final long trimOverlap = 100;
        final long numEntries = 200;
        log.prefixTrim(trimMark);

        List<LogData> entries = new ArrayList<>();
        for (long x = 0; x < numEntries; x++) {
            long address = trimMark - trimOverlap + x;
            entries.add(getEntry(address));
        }

        log.append(entries);

        StreamLog log2 = new StreamLogFiles(sc, new BatchProcessorContext());
        List<LogData> readEntries = readRange(0L, trimMark - trimOverlap + numEntries, log2);

        List<LogData> notTrimmed = new ArrayList<>();
        for (LogData entry : readEntries) {
            if (!entry.isTrimmed()) {
                notTrimmed.add(entry);
            }
        }

        assertThat((long) notTrimmed.size()).isEqualTo(numEntries - trimOverlap - 1);
    }

    @Test
    public void badRangeWrite() throws Exception {
        ServerContext sc = getContext();
        StreamLog log = new StreamLogFiles(sc, new BatchProcessorContext());

        List<LogData> largeRange = new ArrayList<>();
        List<LogData> nonSequentialRange = new ArrayList<>();
        final int numSegments = 3;
        final int skipGap = 10;
        final int numEntries = numSegments * StreamLogFiles.RECORDS_PER_LOG_FILE;
        // Generate two invalid ranges, a large range and a non-sequential range
        for (long address = 0; address < numEntries; address++) {
            largeRange.add(getEntry(address));
            if (address % skipGap == 0) {
                nonSequentialRange.add(getEntry(address));
            }
        }

        String logDir = sc.getServerConfig().get("--log-path") + File.separator + "log";
        File file = new File(logDir);
        long logSize = FileUtils.sizeOfDirectory(file);

        // Verify that range writes that span more than 2 segments are rejected
        assertThatThrownBy(() -> log.append(largeRange))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("range too big!");
        log.sync(true);

        // Verify that a non-sequential range write is rejected
        assertThatThrownBy(() -> log.append(nonSequentialRange))
                .isInstanceOf(IllegalArgumentException.class);
        log.sync(true);

        // Verify that no entries have been written to the log
        long logSize2 = FileUtils.sizeOfDirectory(file);
        assertThat(logSize2).isEqualTo(logSize);
    }

    @Test
    public void testRangeOverwrite() throws Exception {
        ServerContext sc = getContext();
        StreamLog log = new StreamLogFiles(sc, new BatchProcessorContext());
        final int numIter = 500;

        List<LogData> entries = new ArrayList<>();
        for (long x = 0; x < numIter; x++) {
            entries.add(getEntry(x));
        }

        // Write the same range twice
        log.append(entries);
        log.sync(true);

        Assertions.assertThatExceptionOfType(OverwriteException.class)
                .isThrownBy(() -> log.append(entries));
    }

    @Test
    public void writingTrimmedEntries() throws Exception {
        ServerContext sc = getContext();
        StreamLog log = new StreamLogFiles(sc, new BatchProcessorContext());

        final long trimMark = 400;
        final long numEntries = trimMark + 200;
        log.prefixTrim(trimMark);

        List<LogData> entries = new ArrayList<>();
        for (int x = 0; x < numEntries; x++) {
            entries.add(LogData.getTrimmed(x));
        }

        String logDir = sc.getServerConfig().get("--log-path") + File.separator + "log";
        File file = new File(logDir);
        long logSize = FileUtils.sizeOfDirectory(file);
        assertThatThrownBy(() -> log.append(entries))
                .isInstanceOf(OverwriteException.class);
        log.sync(true);
        long logSize2 = FileUtils.sizeOfDirectory(file);
        assertThat(logSize2).isEqualTo(logSize);
    }

    private CorfuRuntime getRuntime() {
        CorfuRuntime mockRuntime = Mockito.mock(CorfuRuntime.class);
        CorfuRuntime.CorfuRuntimeParameters mockParameters = Mockito.mock(CorfuRuntime.CorfuRuntimeParameters.class);
        when(mockRuntime.getParameters()).thenReturn(mockParameters);
        when(mockParameters.isRetainSerializedDataInCache()).thenReturn(false);
        return mockRuntime;
    }

    /**
     * Reads the range [a, b) from a specific log
     */
    List<LogData> readRange(long a, long b, StreamLog log) {
        List<LogData> entries = new ArrayList<>();
        for (long x = a; x < b; x++) {
            entries.add(log.read(x));
        }
        return entries;
    }

    LogData getEntry(long x) {
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogData ld = new LogData(DataType.DATA, b);
        ld.setGlobalAddress(x);
        return ld;
    }

    @Test
    public void testOverwriteException() {
        StreamLog log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        long address0 = 0;
        log.append(address0, new LogData(DataType.DATA, b));

        assertThatThrownBy(() -> log.append(address0, new LogData(DataType.DATA, b)))
                .isInstanceOf(OverwriteException.class);
    }

    @Test
    public void testReadingUnknownAddress() {
        StreamLog log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);

        long address0 = 0;
        long address1 = 1;
        long address2 = 2;

        log.append(address0, new LogData(DataType.DATA, b));
        log.append(address2, new LogData(DataType.DATA, b));
        assertThat(log.read(address1)).isNull();
    }

    @Test
    public void testStreamLogDataCorruption() throws Exception {
        // This test manipulates a log file directly and manipulates
        // log records by overwriting some parts of the record simulating
        // different data corruption scenarios
        String logDir = getContext().getServerConfig().get("--log-path") + File.separator + "log";
        StreamLog log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        // Write to two segments
        long address0 = 0;
        long address1 = StreamLogFiles.RECORDS_PER_LOG_FILE + 1L;
        log.append(address0, new LogData(DataType.DATA, b));
        log.append(address1, new LogData(DataType.DATA, b));

        assertThat(log.read(address0).getPayload(getRuntime())).isEqualTo(streamEntry);
        log.close();

        final int OVERWRITE_BYTES = 4;

        // Overwrite 2 bytes of the checksum and 2 bytes of the entry's address
        String logFilePath1 = logDir + File.separator + 0 + ".log";
        String logFilePath2 = logDir + File.separator + 1 + ".log";
        RandomAccessFile file1 = new RandomAccessFile(logFilePath1, "rw");
        RandomAccessFile file2 = new RandomAccessFile(logFilePath2, "rw");
        ByteBuffer metaDataBuf = ByteBuffer.allocate(METADATA_SIZE);
        file1.getChannel().read(metaDataBuf);
        metaDataBuf.flip();

        Metadata metadata = Metadata.parseFrom(metaDataBuf.array());

        final int offset = METADATA_SIZE + metadata.getLength() + OVERWRITE_BYTES;

        // Corrupt metadata in the second segment
        file2.seek(offset);
        file2.writeInt(OVERWRITE_BYTES);
        file2.close();

        assertThatThrownBy(() -> new StreamLogFiles(getContext(), new BatchProcessorContext()))
                .isInstanceOf(DataCorruptionException.class);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSync() throws Exception {
        StreamLogFiles log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        long seg1 = StreamLogFiles.RECORDS_PER_LOG_FILE * 0 + 1;
        long seg2 = StreamLogFiles.RECORDS_PER_LOG_FILE * 1 + 1;
        long seg3 = StreamLogFiles.RECORDS_PER_LOG_FILE * 2 + 1;

        log.append(seg1, new LogData(DataType.DATA, b));
        log.append(seg2, new LogData(DataType.DATA, b));
        log.append(seg3, new LogData(DataType.DATA, b));

        assertThat(log.getDirtySegments().size()).isEqualTo(3);

        log.sync(true);

        assertThat(log.getDirtySegments().size()).isEqualTo(0);
    }

    private void writeToLog(StreamLog log, long address, UUID streamId) {
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogData logData = new LogData(DataType.DATA, b);
        Map<UUID, Long> map = new HashMap<>();
        map.put(streamId, 0L);
        logData.setBackpointerMap(map);
        logData.setGlobalAddress(address);
        log.append(address, logData);
    }

    private void writeToLog(StreamLog log, long address) {
        writeToLog(log, address, UUID.randomUUID());
    }

    @Test
    public void testWritingFileHeader() throws Exception {
        StreamLogFiles log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        writeToLog(log, 0L);
        log.sync(true);
        StreamLogFiles log2 = new StreamLogFiles(getContext(), new BatchProcessorContext());
        writeToLog(log2, 1L);
        log2.sync(true);
        StreamLogFiles log3 = new StreamLogFiles(getContext(), new BatchProcessorContext());
        assertThat(log3.read(1L)).isNotNull();
    }

    @Test
    public void testGetGlobalTail() {
        StreamLogFiles log = new StreamLogFiles(getContext(), new BatchProcessorContext());

        assertThat(log.getLogTail()).isEqualTo(Address.NON_ADDRESS);

        // Write to multiple segments
        final int segments = 3;
        long lastAddress = segments * StreamLogFiles.RECORDS_PER_LOG_FILE;
        for (long x = 0; x <= lastAddress; x++) {
            writeToLog(log, x);
            assertThat(log.getLogTail()).isEqualTo(x);
        }

        // Restart and try to retrieve the global tail
        log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        assertThat(log.getLogTail()).isEqualTo(lastAddress);

        // Advance the tail some more
        final long tailDelta = 5;
        for (long x = lastAddress + 1; x <= lastAddress + tailDelta; x++){
            writeToLog(log, x);
            assertThat(log.getLogTail()).isEqualTo(x);
        }

        // Restart and try to retrieve the global tail one last time
        log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        assertThat(log.getLogTail()).isEqualTo(lastAddress + tailDelta);
    }

    @Test
    public void testPrefixTrim() {
        String logDir = getContext().getServerConfig().get("--log-path") + File.separator + "log";
        StreamLogFiles log = new StreamLogFiles(getContext(), new BatchProcessorContext());

        // Write 50 segments and trim the first 25
        final long numSegments = 50;
        for(long x = 0; x < numSegments * StreamLogFiles.RECORDS_PER_LOG_FILE; x++) {
            writeToLog(log, x);
        }

        File logs = new File(logDir);

        assertThat((long) logs.list().length).isEqualTo(numSegments);

        final long endSegment = 25;
        long trimAddress = endSegment * StreamLogFiles.RECORDS_PER_LOG_FILE + 1;

        // Get references to the segments that will be trimmed
        Set<Segment> trimmedHandles = new HashSet<>();
        for (Segment sh : log.getOpenSegments()) {
            if (sh.id < endSegment) {
                trimmedHandles.add(sh);
            }
        }

        log.prefixTrim(trimAddress);
        log.compact();

        // Verify that the segments have been removed
        assertThat(log.getOpenSegments().size()).isEqualTo((int) endSegment);

        // Verify that first 25 segments have been deleted
        String[] afterTrimFiles = logs.list();
        assertThat(afterTrimFiles).hasSize((int) (numSegments - endSegment));

        Set<String> fileNames = new HashSet<>(Arrays.asList(afterTrimFiles));
        for (long x = endSegment + 1; x < numSegments; x++) {
            String logFile = Long.toString(x) + ".log";
            assertThat(fileNames).contains(logFile);
        }

        // Try to trim an address that is less than the new starting address
        // This shouldn't throw an exception
        log.prefixTrim(trimAddress);

        long trimmedExceptions = 0;

        // Try to read trimmed addresses
        for(long x = 0; x < numSegments * StreamLogFiles.RECORDS_PER_LOG_FILE; x++) {
            ILogData logData = log.read(x);
            if(logData.isTrimmed()) {
                trimmedExceptions++;
            }
        }

        // Verify that the trimmed segment channels are closed
        for (Segment sh : trimmedHandles) {
            assertThat(sh.getWriteChannel().isOpen()).isFalse();
        }

        // Address 0 is not reflected in trimAddress
        assertThat(trimmedExceptions).isEqualTo(trimAddress + 1);
    }

    @Test
    public void testPrefixTrimAndStartUp() {
        StreamLog log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        log.prefixTrim(StreamLogFiles.RECORDS_PER_LOG_FILE / 2);
        log.compact();
        log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        final long midSegmentAddress = RECORDS_PER_LOG_FILE + 5;
        log.prefixTrim(midSegmentAddress);
        log.compact();
        log = new StreamLogFiles(getContext(), new BatchProcessorContext());

        assertThat(log.getLogTail()).isEqualTo(midSegmentAddress);
        assertThat(log.getTrimMark()).isEqualTo(midSegmentAddress + 1);
    }

    @Test
    public void testPrefixTrimAfterRestart() {
        String logDir = getContext().getServerConfig().get("--log-path") + File.separator + "log";
        StreamLog log = new StreamLogFiles(getContext(), new BatchProcessorContext());

        final long numSegments = 3;
        for (long x = 0; x < RECORDS_PER_LOG_FILE * numSegments; x++) {
            writeToLog(log, x);
        }

        final long trimMark = RECORDS_PER_LOG_FILE * 2 + 100;
        log.prefixTrim(trimMark);
        log.close();

        log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        log.compact();

        File logs = new File(logDir);
        final int numFilesLeft = 1;
        assertThat(logs.list()).hasSize(numFilesLeft);
    }

    /**
     * Generates and writes 3 files worth data. Then resets the stream log and verifies that the
     * files and data is cleared.
     */
    @Test
    public void testResetStreamLog() {
        ServerContext sc = getContext();
        String logDir = sc.getServerConfig().get("--log-path") + File.separator + "log";
        StreamLogFiles log = new StreamLogFiles(sc, new BatchProcessorContext());

        final long numSegments = 3;
        for (long x = 0; x < RECORDS_PER_LOG_FILE * numSegments; x++) {
            writeToLog(log, x);
        }
        final long filesToBeTrimmed = 1;
        log.prefixTrim(RECORDS_PER_LOG_FILE * filesToBeTrimmed - 1);
        log.compact();

        File logsDir = new File(logDir);

        final int expectedFilesBeforeReset = (int) (numSegments - filesToBeTrimmed);
        final long globalTailBeforeReset = (RECORDS_PER_LOG_FILE * numSegments) - 1;
        final long trimMarkBeforeReset = RECORDS_PER_LOG_FILE * filesToBeTrimmed;
        assertThat(logsDir.list()).hasSize(expectedFilesBeforeReset);
        assertThat(log.getLogTail()).isEqualTo(globalTailBeforeReset);
        assertThat(log.getTrimMark()).isEqualTo(trimMarkBeforeReset);

        log.updateCommittedTail(22000);

        log.reset();
        assertThat(log.getOpenSegments()).hasSize(0);

        final int expectedFilesAfterReset = 1;
        assertThat(logsDir.list()).hasSize(expectedFilesAfterReset);

        final long globalTailAfterReset = 20000 - 1;
        assertThat(log.getLogTail()).isEqualTo(globalTailAfterReset);

        final long trimMarkAfterReset = trimMarkBeforeReset;
        assertThat(log.getTrimMark()).isEqualTo(trimMarkAfterReset);
    }

    /**
     * 1. Generate 3 log segment data.
     * 2. Trim the 1st segment.
     * 3. Persist log metadata.
     * 4. Generate 4th log segment.
     * 5. Reconstruct StreamLogFiles from persisted log metadata.
     */
    @Test
    public void testLogMetadataReconstructionBasic() {
        ServerContext sc = getContext();
        StreamLogFiles log = new StreamLogFiles(sc, new BatchProcessorContext());

        List<UUID> streamIds = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            streamIds.add(UUID.randomUUID());
        }

        // Write 30K entries
        final long numSegments = 3;
        Map<UUID, List<Long>> addressMap = new HashMap<>();
        for (int address = 0; address < RECORDS_PER_LOG_FILE * numSegments; address++) {
            UUID streamId = streamIds.get(address%1000);
            writeToLog(log, address, streamId);
            addressMap.computeIfAbsent(streamId, k -> new ArrayList<>()).add((long) address);
        }

        // Perform prefixTrim and delete the first log segment
        final long filesToBeTrimmed = 1;
        long trimAddress = RECORDS_PER_LOG_FILE * filesToBeTrimmed - 1; // inclusive
        log.prefixTrim(trimAddress);
        log.compact();

        final Map<UUID, List<Long>> trimmedAddressMap = addressMap.entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().stream()
                    .filter(val -> val > trimAddress)
                    .collect(Collectors.toList())
            ));

        log.persistLogMetadata();

        // Write 10K more addresses after the log metadata has been persisted
        for (int address = (int) (RECORDS_PER_LOG_FILE * numSegments);
             address < RECORDS_PER_LOG_FILE * (numSegments+1); address++) {
            UUID streamId = streamIds.get(address%1000);
            writeToLog(log, address, streamId);
            trimmedAddressMap.computeIfAbsent(streamId, k -> new ArrayList<>()).add((long) address);
        }
        log.updateCommittedTail(RECORDS_PER_LOG_FILE * (numSegments+1) - 1);
        log.close();

        // Reconstruct log
        log = new StreamLogFiles(sc, new BatchProcessorContext());

        // Check the reconstructed log metadata
        assertThat(log.getLogTail()).isEqualTo(RECORDS_PER_LOG_FILE * (numSegments+1) - 1);
        assertThat(log.getCommittedTail()).isEqualTo(RECORDS_PER_LOG_FILE * (numSegments+1) - 1);
        assertThat(log.getTrimMark()).isEqualTo(trimAddress + 1);

        assertThat(log.getStreamsAddressSpace().getAddressMap()).hasSize(1000);
        log.getStreamsAddressSpace().getAddressMap().forEach((uuid, streamAddressSpace) -> {
            assertThat(streamAddressSpace.size()).isEqualTo(30);
            assertThat(streamAddressSpace.toArray()).containsExactly(trimmedAddressMap.get(uuid).toArray(new Long[0]));
        });
    }

    /**
     * 1. Generate 0.5 segment of data to stream A.
     * 2. Generate 1.5 segment of data to stream B.
     * 3. Trim and delete the 1st segment. Now stream A is trimmed.
     * 4. Reconstruct StreamLogFiles => prePersisting.
     * 4. Persist log metadata.
     * 5. Reconstruct StreamLogFiles => postPersisting.
     * 6. Verify prePersisting and postPersisting.
     */
    @Test
    public void testLogMetadataReconstructionWithEmptyStream() {
        ServerContext sc = getContext();
        StreamLogFiles log = new StreamLogFiles(sc, new BatchProcessorContext());

        UUID streamA = UUID.randomUUID();
        UUID streamB = UUID.randomUUID();

        // Write 5K entries to stream id in [0..499]
        Map<UUID, List<Long>> addressMap = new HashMap<>();
        for (int address = 0; address < 0.5 * RECORDS_PER_LOG_FILE; address++) {
            writeToLog(log, address, streamA);
            addressMap.computeIfAbsent(streamA, k -> new ArrayList<>()).add((long) address);
        }

        // Write 15K entries to stream id in [500..999]
        for (int address = (int) (0.5 * RECORDS_PER_LOG_FILE); address < 2 * RECORDS_PER_LOG_FILE; address++) {
            writeToLog(log, address, streamB);
            addressMap.computeIfAbsent(streamB, k -> new ArrayList<>()).add((long) address);
        }
        log.updateCommittedTail(RECORDS_PER_LOG_FILE * 2 - 1);

        // Trim and delete the first log segment
        final long filesToBeTrimmed = 1;
        long trimAddress = RECORDS_PER_LOG_FILE * filesToBeTrimmed - 1; // inclusive
        log.prefixTrim(trimAddress);
        log.compact();

        StreamLogFiles prePersisting = new StreamLogFiles(sc, new BatchProcessorContext());

        // Persist log metadata
        log.persistLogMetadata();

        // Reconstruct log from the persisted log metadata
        StreamLogFiles postPersisting = new StreamLogFiles(sc, new BatchProcessorContext());

        // Check the reconstructed log metadata and verify the stream [0..499] doesn't exist
        for (StreamLogFiles reconstructed : new StreamLogFiles[]{prePersisting, postPersisting}) {
            assertThat(reconstructed.getLogTail()).isEqualTo(RECORDS_PER_LOG_FILE * 2 - 1);
            assertThat(reconstructed.getCommittedTail()).isEqualTo(RECORDS_PER_LOG_FILE * 2 - 1);
            assertThat(reconstructed.getTrimMark()).isEqualTo(RECORDS_PER_LOG_FILE * filesToBeTrimmed);
            assertThat(reconstructed.getStreamsAddressSpace().getAddressMap()).doesNotContainKey(streamA);
        }
    }

    /**
     * 1. Generate 1 segment of data (10K entries).
     * 2. Persist log metadata.
     * 3. Generate additional 1 segment of data.
     * 4. Trim at (15K-1) and delete the first segment.
     * 5. Reconstruct StreamLogFiles from persisted log metadata.
     */
    @Test
    public void testLogMetadataReconstructionWithHigherTrimMark() {
        ServerContext sc = getContext();
        StreamLogFiles log = new StreamLogFiles(sc, new BatchProcessorContext());

        List<UUID> streamIds = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            streamIds.add(UUID.randomUUID());
        }

        // Write 10K entries
        final long numSegments = 1;
        Map<UUID, List<Long>> addressMap = new HashMap<>();
        for (int address = 0; address < RECORDS_PER_LOG_FILE * numSegments; address++) {
            UUID streamId = streamIds.get(address%1000);
            writeToLog(log, address, streamId);
            addressMap.computeIfAbsent(streamId, k -> new ArrayList<>()).add((long) address);
        }

        // Persist log metadata
        log.persistLogMetadata();

        // Write additional 10K entries
        long start = RECORDS_PER_LOG_FILE * numSegments;
        long end = RECORDS_PER_LOG_FILE * numSegments * 2;
        for (long address = start; address < end; address++) {
            UUID streamId = streamIds.get((int) (address%1000));
            writeToLog(log, address, streamId);
            addressMap.computeIfAbsent(streamId, k -> new ArrayList<>()).add(address);
        }
        log.updateCommittedTail(RECORDS_PER_LOG_FILE * 2 - 1);

        // Perform prefixTrim at 15K and delete the first log segment
        long trimAddress = (long) (1.5 * RECORDS_PER_LOG_FILE - 1); // inclusive
        log.prefixTrim(trimAddress);
        log.compact();

        final Map<UUID, List<Long>> trimmedAddressMap = addressMap.entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().stream()
                    .filter(val -> val > trimAddress)
                    .collect(Collectors.toList())
            ));

        // Reconstruct log
        log = new StreamLogFiles(sc, new BatchProcessorContext());

        // Check the reconstructed log metadata
        String logDir = sc.getServerConfig().get("--log-path") + File.separator + "log";
        File logsDir = new File(logDir);
        assertThat(logsDir.list()).hasSize(1);

        assertThat(log.getLogTail()).isEqualTo(RECORDS_PER_LOG_FILE * 2 - 1);
        assertThat(log.getCommittedTail()).isEqualTo(RECORDS_PER_LOG_FILE * 2 - 1);
        assertThat(log.getTrimMark()).isEqualTo(trimAddress + 1);

        assertThat(log.getStreamsAddressSpace().getAddressMap()).hasSize(1000);
        log.getStreamsAddressSpace().getAddressMap().forEach((uuid, streamAddressSpace) -> {
            assertThat(streamAddressSpace.size()).isEqualTo(5);
            assertThat(streamAddressSpace.toArray()).containsExactly(trimmedAddressMap.get(uuid).toArray(new Long[0]));
        });
    }

    @Test
    public void partialHeaderMetadataTest() throws Exception {
        String logDir = getContext().getServerConfig().get("--log-path") + File.separator + "log";
        String logFilePath = logDir + File.separator + 0 + ".log";

        File dir = new File(logDir);
        dir.mkdir();
        RandomAccessFile logFile = new RandomAccessFile(logFilePath, "rw");

        LogHeader header = LogHeader.newBuilder()
                .setVersion(VERSION)
                .setVerifyChecksum(false)
                .build();

        // Simulate a partial metadata write for the log header
        ByteBuffer buf = getByteBufferWithMetaData(header);
        buf.limit(METADATA_SIZE - 1);
        logFile.getChannel().write(buf);
        logFile.close();

        // Open a StreamLog and write an entry in the segment that has the partial metadata write
        StreamLog log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        long address0 = 0;
        assertThat(log.read(address0)).isNull();
        log.close();
    }

    @Test
    public void partialHeaderTest() throws Exception {
        String logDir = getContext().getServerConfig().get("--log-path") + File.separator + "log";
        String logFilePath = logDir + File.separator + 0 + ".log";

        File dir = new File(logDir);
        dir.mkdir();
        RandomAccessFile logFile = new RandomAccessFile(logFilePath, "rw");

        LogHeader header = LogHeader.newBuilder()
                .setVersion(VERSION)
                .setVerifyChecksum(false)
                .build();

        // Simulate a partial log header write
        ByteBuffer buf = getByteBufferWithMetaData(header);
        buf.limit(buf.capacity() - 1);
        logFile.getChannel().write(buf);
        logFile.close();

        StreamLog log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        // Write to segment 0
        long address0 = 0;
        log.append(address0, new LogData(DataType.DATA, b));
        log.close();

        // Open the segment again and verify that the entry write can be read (i.e. log file can be
        // parsed correctly).
        log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        assertThat(log.read(address0).getPayload(getRuntime())).isEqualTo(streamEntry);
    }

    @Test
    public void partialEntryTest() throws Exception {
        StreamLog log = new StreamLogFiles(getContext(), new BatchProcessorContext());

        // Force the creation of segment 0
        long address0 = 0;
        log.read(address0);
        log.close();

        // Simulate a partially written log entry
        final int serializedEntrySize = 100;
        byte[] entryBytes = new byte[serializedEntrySize];
        Metadata metadata = Metadata.newBuilder()
                .setPayloadChecksum(getChecksum(serializedEntrySize))
                .setLengthChecksum(getChecksum(entryBytes.length))
                .setLength(entryBytes.length)
                .build();

        ByteBuffer entryBuf = ByteBuffer.allocate(entryBytes.length + metadata.getSerializedSize());
        entryBuf.put(metadata.toByteArray());
        entryBuf.put(entryBytes);
        entryBuf.flip();

        String logDir = getContext().getServerConfig().get("--log-path") + File.separator + "log";
        String logFilePath = logDir + File.separator + 0 + ".log";

        RandomAccessFile logFile = new RandomAccessFile(logFilePath, "rw");

        // Write a partial buffer
        entryBuf.limit(entryBuf.capacity() - 1);
        System.out.println("cap " + entryBuf.capacity());
        System.out.println("limit " + entryBuf.limit());
        // Append the buffer after the header
        long end = logFile.getChannel().size();
        logFile.getChannel().position(end);
        System.out.println("position " + logFile.getChannel().position());
        int bytesWritten = logFile.getChannel().write(entryBuf);
        System.out.println("bytesWritten " + bytesWritten);
        System.out.println("position " + logFile.getChannel().position());
        logFile.close();

        // Verify that the segment address space can be parsed and that the partial write is ignored
        log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        assertThat(log.read(address0)).isNull();

        // Attempt to write after the partial write and very that it can be read back
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        log.append(address0, new LogData(DataType.DATA, b));
        log.close();

        log = new StreamLogFiles(getContext(), new BatchProcessorContext());
        assertThat(log.read(address0).getPayload(getRuntime())).isEqualTo(streamEntry);
    }

    @Test
    public void estimateSizeTest() throws IOException {
        // Create two nested directories and create files in each,
        // the estimated size should reflect the total sum of all file sizes
        File parentDir = com.google.common.io.Files.createTempDir();
        File childDir = new File(parentDir.getAbsolutePath() + File.separator + "logs");

        childDir.mkdir();

        RandomAccessFile parentDirFile = new RandomAccessFile(parentDir.getAbsolutePath()
                + File.separator + "file1", "rw");

        RandomAccessFile childDirFile = new RandomAccessFile(childDir.getAbsolutePath()
                + File.separator + "file2", "rw");

        final int parentDirFilePayloadSize = 4300;
        final int childDirFilePayloadSize = 3200;

        byte[] parentDirFilePayload = new byte[parentDirFilePayloadSize];
        byte[] childDirFilePayload = new byte[childDirFilePayloadSize];

        parentDirFile.write(parentDirFilePayload);
        childDirFile.write(childDirFilePayload);

        parentDirFile.close();
        childDirFile.close();


        final double limit = 100.0;
        final PersistenceMode mode = PersistenceMode.DISK;
        FileSystemAgent.init(new FileSystemConfig(parentDir.toPath(), limit, 0, mode), new BatchProcessorContext());
        long parentSize = FileSystemAgent.getResourceQuota().getUsed().get();

        FileSystemAgent.init(new FileSystemConfig(childDir.toPath(), limit, 0, mode), new BatchProcessorContext());
        long childDirSize = FileSystemAgent.getResourceQuota().getUsed().get();

        assertThat(parentSize).isEqualTo(parentDirFilePayloadSize + childDirFilePayloadSize);
        assertThat(childDirSize).isEqualTo(childDirFilePayloadSize);
    }
}
