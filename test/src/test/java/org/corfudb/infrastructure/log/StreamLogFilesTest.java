package org.corfudb.infrastructure.log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.format.Types;
import org.corfudb.format.Types.Metadata;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.log.StreamLogFiles.Checksum;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.StreamLogParams.RECORDS_PER_SEGMENT;

/**
 * Created by maithem on 11/2/16.
 */
public class StreamLogFilesTest extends AbstractCorfuTest {

    private ServerContext sc;

    private String getDirPath() {
        return PARAMETERS.TEST_TEMP_DIR;
    }

    @Before
    public void initSeverContext() {
        String path = getDirPath();
        sc = new ServerContextBuilder()
                .setLogPath(path)
                .setMemory(false)
                .build();
    }

    @Test
    public void testWriteReadWithChecksum() {
        // Enable checksum, then append and read the same entry
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        long address0 = 0;
        log.append(address0, new LogData(DataType.DATA, b));
        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);

        // Disable checksum, then append and read then same entry
        // An overwrite exception should occur, since we are writing the
        // same entry.
        StreamLogParams params = sc.getStreamLogParams();
        params.verifyChecksum = false;
        final StreamLog newLog = new StreamLogFiles(params, sc.getStreamLogDataStore());
        assertThatThrownBy(() -> {
            newLog.append(address0, new LogData(DataType.DATA, b));
        })
                .isInstanceOf(OverwriteException.class);
        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);
    }

    @Test
    public void testBatchWrite() throws Exception {
        StreamLogParams params = sc.getStreamLogParams();
        StreamLogFiles log = new StreamLogFiles(params, sc.getStreamLogDataStore());

        // A range write that spans two segments
        final int numSegments = 2;
        final int numIter = RECORDS_PER_SEGMENT * numSegments;
        List<LogData> writeEntries = new ArrayList<>();
        for (int x = 0; x < numIter; x++) {
            writeEntries.add(getEntry(x));
        }

        log.append(writeEntries);
        log.sync(true);

        StreamLog log2 = new StreamLogFiles(params, sc.getStreamLogDataStore());
        List<LogData> readEntries = readRange(0, numIter, log2);
        assertThat(writeEntries).isEqualTo(readEntries);
    }

    @Test
    public void badRangeWrite() throws Exception {
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());

        List<LogData> largeRange = new ArrayList<>();
        List<LogData> nonSequentialRange = new ArrayList<>();
        final int numSegments = 3;
        final int skipGap = 10;
        final int numEntries = numSegments * RECORDS_PER_SEGMENT;
        // Generate two invalid ranges, a large range and a non-sequential range
        for (long address = 0; address < numEntries; address++) {
            largeRange.add(getEntry(address));
            if (address % skipGap == 0) {
                nonSequentialRange.add(getEntry(address));
            }
        }

        File file = log.getLogDir().toFile();
        long logSize = FileUtils.sizeOfDirectory(file);

        // Verify that range writes that span more than 2 segments are rejected
        assertThatThrownBy(() -> log.append(largeRange))
                .isInstanceOf(IllegalArgumentException.class);
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
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
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
        ld.setGlobalAddress((long) x);
        return ld;
    }

    @Test
    public void testOverwriteException() {
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
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
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
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
    public void testStreamLogBadChecksum() {
        // This test generates a stream log file without computing checksums, then
        // tries to read from the same log file with checksum enabled. The expected
        // behaviour is to throw a DataCorruptionException because a checksum cannot
        // be computed for stream entries that haven't been written with a checksum
        StreamLogParams params = sc.getStreamLogParams();
        params.verifyChecksum = false;
        StreamLogFiles log = new StreamLogFiles(params, sc.getStreamLogDataStore());
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        long address0 = 0;
        log.append(address0, new LogData(DataType.DATA, b));

        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);

        log.close();

        // Re-open stream log with checksum enabled
        assertThatThrownBy(() -> new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore()))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testStreamLogDataCorruption() throws Exception {
        // This test manipulates a log file directly and manipulates
        // log records by overwriting some parts of the record simulating
        // different data corruption scenarios
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        // Write to two segments
        long address0 = 0;
        long address1 = RECORDS_PER_SEGMENT + 1L;
        log.append(address0, new LogData(DataType.DATA, b));
        log.append(address1, new LogData(DataType.DATA, b));

        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);
        log.close();

        final int OVERWRITE_BYTES = 4;

        // Overwrite 2 bytes of the checksum and 2 bytes of the entry's address
        String logDir = log.getLogDir().toString();
        String logFilePath1 = logDir + File.separator + 0 + ".log";
        String logFilePath2 = logDir + File.separator + 1 + ".log";
        RandomAccessFile file1 = new RandomAccessFile(logFilePath1, "rw");
        RandomAccessFile file2 = new RandomAccessFile(logFilePath2, "rw");
        ByteBuffer metaDataBuf = ByteBuffer.allocate(StreamLogParams.METADATA_SIZE);
        file1.getChannel().read(metaDataBuf);
        metaDataBuf.flip();

        Metadata metadata = Metadata.parseFrom(metaDataBuf.array());

        final int offset = StreamLogParams.METADATA_SIZE + metadata.getLength() + OVERWRITE_BYTES;

        // Corrupt metadata in the second segment
        file2.seek(offset);
        file2.writeInt(OVERWRITE_BYTES);
        file2.close();

        assertThatThrownBy(() -> new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore()))
                .isInstanceOf(DataCorruptionException.class);
    }

    @Test
    @Ignore
    // TODO: change this test to single writer, multiple reader and then add back.
    public void multiThreadedReadWrite() throws Exception {
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());

        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);

        final int num_threads = PARAMETERS.CONCURRENCY_SOME;
        final int num_entries = PARAMETERS.NUM_ITERATIONS_LOW;

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_entries;
            for (int i = base; i < base + num_entries; i++) {
                long address = (long) i;
                log.append(address, new LogData(DataType.DATA, b));
            }
        });

        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);

        // verify that addresses 0 to 2000 have been used up
        for (int x = 0; x < num_entries * num_threads; x++) {
            long address = (long) x;
            LogData data = log.read(address);
            byte[] bytes = (byte[]) data.getPayload(null);
            assertThat(bytes).isEqualTo(streamEntry);
        }
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSync() throws Exception {
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        long seg1 = RECORDS_PER_SEGMENT * 0 + 1;
        long seg2 = RECORDS_PER_SEGMENT * 1 + 1;
        long seg3 = RECORDS_PER_SEGMENT * 2 + 1;

        log.append(seg1, new LogData(DataType.DATA, b));
        log.append(seg2, new LogData(DataType.DATA, b));
        log.append(seg3, new LogData(DataType.DATA, b));

        assertThat(log.getSegmentsToSync().size()).isEqualTo(3);

        log.sync(true);

        assertThat(log.getSegmentsToSync().size()).isEqualTo(0);
    }

    private void writeToLog(StreamLog log, long address) {
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        log.append(address, new LogData(DataType.DATA, b));
    }

    @Test
    public void testWritingFileHeader() throws Exception {
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        writeToLog(log, 0L);
        log.sync(true);
        StreamLogFiles log2 = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        writeToLog(log2, 1L);
        log2.sync(true);
        StreamLogFiles log3 = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        assertThat(log3.read(1L)).isNotNull();
    }

    @Test
    public void testGetGlobalTail() {
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());

        assertThat(log.getLogTail()).isEqualTo(Address.NON_ADDRESS);

        // Write to multiple segments
        final int segments = 3;
        long lastAddress = RECORDS_PER_SEGMENT;
        for (long x = 0; x <= lastAddress; x++){
            writeToLog(log, x);
            assertThat(log.getLogTail()).isEqualTo(x);
        }

        // Restart and try to retrieve the global tail
        log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        assertThat(log.getLogTail()).isEqualTo(lastAddress);

        // Advance the tail some more
        final long tailDelta = 5;
        for (long x = lastAddress + 1; x <= lastAddress + tailDelta; x++){
            writeToLog(log, x);
            assertThat(log.getLogTail()).isEqualTo(x);
        }

        // Restart and try to retrieve the global tail one last time
        log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        assertThat(log.getLogTail()).isEqualTo(lastAddress + tailDelta);
    }

    /**
     * Generates and writes 3 files worth data. Then resets the stream log and verifies that the
     * files and data is cleared.
     */
    @Test
    public void testResetStreamLog() {
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());

        final long numSegments = 3;
        for (long x = 0; x < RECORDS_PER_SEGMENT * numSegments; x++) {
            writeToLog(log, x);
        }

        final long globalTailBeforeReset = (RECORDS_PER_SEGMENT * numSegments) - 1;
        assertThat(log.getLogTail()).isEqualTo(globalTailBeforeReset);

        log.reset();

        final long globalTailAfterReset = Address.NON_ADDRESS;
        assertThat(log.getLogTail()).isEqualTo(globalTailAfterReset);
    }

    @Test
    public void partialHeaderMetadataTest() throws Exception {
        String logDir = sc.getStreamLogParams().logPath + File.separator + "log";
        String logFilePath = logDir + File.separator + 0 + ".log";

        File dir = new File(logDir);
        dir.mkdir();
        RandomAccessFile logFile = new RandomAccessFile(logFilePath, "rw");

        Types.LogHeader header = Types.LogHeader.newBuilder()
                .setVersion(StreamLogParams.VERSION)
                .setVerifyChecksum(false)
                .build();

        // Simulate a partial metadata write for the log header
        ByteBuffer buf = StreamLogFiles.getByteBufferWithMetaData(header);
        buf.limit(StreamLogParams.METADATA_SIZE - 1);
        logFile.getChannel().write(buf);
        logFile.close();

        // Open a StreamLog and write an entry in the segment that has the partial metadata write
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        long address0 = 0;
        assertThat(log.read(address0)).isNull();
        log.close();
    }

    @Test
    public void partialHeaderTest() throws Exception {
        String logDir = sc.getStreamLogParams().logPath + File.separator + "log";
        String logFilePath = logDir + File.separator + 0 + ".log";

        File dir = new File(logDir);
        dir.mkdir();
        RandomAccessFile logFile = new RandomAccessFile(logFilePath, "rw");

        Types.LogHeader header = Types.LogHeader.newBuilder()
                .setVersion(StreamLogParams.VERSION)
                .setVerifyChecksum(false)
                .build();

        // Simulate a partial log header write
        ByteBuffer buf = StreamLogFiles.getByteBufferWithMetaData(header);
        buf.limit(buf.capacity() - 1);
        logFile.getChannel().write(buf);
        logFile.close();

        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());

        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        // Write to segment 0
        long address0 = 0;
        log.append(address0, new LogData(DataType.DATA, b));
        log.close();

        // Open the segment again and verify that the entry write can be read (i.e. log file can be
        // parsed correctly).
        log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);
    }

    @Test
    public void partialEntryTest() throws Exception {
        StreamLogFiles log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());

        // Force the creation of segment 0
        long address0 = 0;
        log.read(address0);
        log.close();

        // Simulate a partially written log entry
        final int serializedEntrySize = 100;
        byte[] entryBytes = new byte[serializedEntrySize];
        Metadata metadata = Metadata.newBuilder()
                .setPayloadChecksum(Checksum.getChecksum(serializedEntrySize))
                .setLengthChecksum(Checksum.getChecksum(entryBytes.length))
                .setLength(entryBytes.length)
                .build();

        ByteBuffer entryBuf = ByteBuffer.allocate(entryBytes.length + metadata.getSerializedSize());
        entryBuf.put(metadata.toByteArray());
        entryBuf.put(entryBytes);
        entryBuf.flip();

        String logDir = log.getLogParams().logPath + File.separator + "log";
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
        log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        assertThat(log.read(address0)).isNull();

        // Attempt to write after the partial write and very that it can be read back
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        log.append(address0, new LogData(DataType.DATA, b));
        log.close();

        log = new StreamLogFiles(sc.getStreamLogParams(), sc.getStreamLogDataStore());
        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);
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

        long parentSize = StreamLogFiles.estimateSize(parentDir.toPath());
        long childDirSize = StreamLogFiles.estimateSize(childDir.toPath());

        assertThat(parentSize).isEqualTo(parentDirFilePayloadSize + childDirFilePayloadSize);
        assertThat(childDirSize).isEqualTo(childDirFilePayloadSize);
    }
}
