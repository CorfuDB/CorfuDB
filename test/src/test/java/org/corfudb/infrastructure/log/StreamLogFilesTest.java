package org.corfudb.infrastructure.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.StreamLogFiles.METADATA_SIZE;

import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.FileFilter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import io.netty.buffer.Unpooled;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.format.Types.Metadata;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;


/**
 * Created by maithem on 11/2/16.
 */
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

    @Test
    public void testWriteReadWithChecksum() {
        // Enable checksum, then append and read the same entry
        StreamLog log = new StreamLogFiles(getContext(), false);
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogAddress address0 = new LogAddress((long) 0, null);
        log.append(address0, new LogData(DataType.DATA, b));
        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);

        // Disable checksum, then append and read then same entry
        // An overwrite exception should occur, since we are writing the
        // same entry.
        final StreamLog newLog = new StreamLogFiles(getContext(), true);
        assertThatThrownBy(() -> {
            newLog
                    .append(address0, new LogData(DataType.DATA, b));
        })
                .isInstanceOf(OverwriteException.class);
        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);
    }

    @Test
    public void testOverwriteException() {
        StreamLog log = new StreamLogFiles(getContext(), false);
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogAddress address0 = new LogAddress((long) 0, null);
        log.append(address0, new LogData(DataType.DATA, b));

        assertThatThrownBy(() -> log.append(address0, new LogData(DataType.DATA, b)))
                .isInstanceOf(OverwriteException.class);
    }

    @Test
    public void testReadingUnknownAddress() {
        StreamLog log = new StreamLogFiles(getContext(), false);
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);

        LogAddress address0 = new LogAddress((long) 0, null);
        LogAddress address1 = new LogAddress((long) 1, null);
        LogAddress address2 = new LogAddress((long) 2, null);

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
        StreamLog log = new StreamLogFiles(getContext(), true);
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogAddress address0 = new LogAddress((long) 0, null);
        log.append(address0, new LogData(DataType.DATA, b));

        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);

        log.close();

        // Re-open stream log with checksum enabled
        assertThatThrownBy(() -> new StreamLogFiles(getContext(), false))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testStreamLogDataCorruption() throws Exception {
        // This test manipulates a log file directly and manipulates
        // log records by overwriting some parts of the record simulating
        // different data corruption scenarios
        String logDir = getContext().getServerConfig().get("--log-path") + File.separator + "log";
        StreamLog log = new StreamLogFiles(getContext(), false);
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        // Write to two segments
        LogAddress address0 = new LogAddress((long) 0, null);
        LogAddress address1 = new LogAddress(StreamLogFiles.RECORDS_PER_LOG_FILE + 1L, null);
        log.append(address0, new LogData(DataType.DATA, b));
        log.append(address1, new LogData(DataType.DATA, b));

        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);
        log.close();

        final int OVERWRITE_DELIMITER = 0xFFFF;
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

        final int offset1 = METADATA_SIZE + metadata.getLength();
        final int offset2 = METADATA_SIZE + metadata.getLength() + Short.BYTES + OVERWRITE_BYTES;

        // Corrupt delimiter in the first segment

        file1.seek(offset1);
        file1.writeShort(0);
        file1.close();

        assertThatThrownBy(() -> new StreamLogFiles(getContext(), false).read(new LogAddress(0L, null)))
                .isInstanceOf(DataCorruptionException.class);

        // Corrupt metadata in the second segment
        file2.seek(offset2);
        file2.writeInt(OVERWRITE_DELIMITER);
        file2.close();

        assertThatThrownBy(() -> new StreamLogFiles(getContext(), false))
                .isInstanceOf(DataCorruptionException.class);
    }

    @Test
    public void multiThreadedReadWrite() throws Exception {
        String logDir = getDirPath();
        StreamLog log = new StreamLogFiles(getContext(), false);

        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);

        final int num_threads = PARAMETERS.CONCURRENCY_SOME;
        final int num_entries = PARAMETERS.NUM_ITERATIONS_LOW;

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_entries;
            for (int i = base; i < base + num_entries; i++) {
                LogAddress address = new LogAddress((long) i, null);
                log.append(address, new LogData(DataType.DATA, b));
            }
        });

        executeScheduled(num_threads, PARAMETERS.TIMEOUT_LONG);

        // verify that addresses 0 to 2000 have been used up
        for (int x = 0; x < num_entries * num_threads; x++) {
            LogAddress address = new LogAddress((long) x, null);
            LogData data = log.read(address);
            byte[] bytes = (byte[]) data.getPayload(null);
            assertThat(bytes).isEqualTo(streamEntry);
        }
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testSync() throws Exception {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        long seg1 = StreamLogFiles.RECORDS_PER_LOG_FILE * 0 + 1;
        long seg2 = StreamLogFiles.RECORDS_PER_LOG_FILE * 1 + 1;
        long seg3 = StreamLogFiles.RECORDS_PER_LOG_FILE * 2 + 1;

        log.append(new LogAddress(seg1, null), new LogData(DataType.DATA, b));
        log.append(new LogAddress(seg2, null), new LogData(DataType.DATA, b));
        log.append(new LogAddress(seg3, null), new LogData(DataType.DATA, b));

        assertThat(log.getChannelsToSync().size()).isEqualTo(3);

        log.sync(true);

        assertThat(log.getChannelsToSync().size()).isEqualTo(0);
    }

    @Test
    public void testSameAddressTrim() throws Exception {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);

        // Trim an unwritten address
        LogAddress logAddress = new LogAddress(0L, null);
        log.trim(logAddress);

        // Verify that the unwritten address trim is not persisted
        StreamLogFiles.SegmentHandle sh = log.getSegmentHandleForAddress(logAddress);
        assertThat(sh.getPendingTrims().size()).isEqualTo(0);

        // Write to the same address
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);

        log.append(logAddress, new LogData(DataType.DATA, b));

        // Verify that the address has been written
        assertThat(log.read(logAddress)).isNotNull();

        // Trim the address
        log.trim(logAddress);
        sh = log.getSegmentHandleForAddress(logAddress);
        assertThat(sh.getPendingTrims().contains(logAddress.address)).isTrue();

        // Write to a trimmed address
        assertThatThrownBy(() -> log.append(logAddress, new LogData(DataType.DATA, b)))
                .isInstanceOf(OverwriteException.class);

        // Read trimmed address
        assertThatThrownBy(() -> log.read(logAddress))
                .isInstanceOf(TrimmedException.class);
    }

    private void writeToLog(StreamLog log, Long addr) {
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogAddress address = new LogAddress(addr, null);
        log.append(address, new LogData(DataType.DATA, b));
    }

    @Test
    public void testTrim() throws Exception {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        final int logChunk = StreamLogFiles.RECORDS_PER_LOG_FILE / 2;

        // Write to the addresses then trim the addresses that span two log files
        for (long x = 0; x < logChunk; x++) {
            writeToLog(log, x);
        }

        // Verify that an incomplete log segment isn't compacted
        for (long x = 0; x < logChunk; x++) {
            LogAddress logAddress = new LogAddress(x, null);
            log.trim(logAddress);
        }

        log.compact();

        StreamLogFiles.SegmentHandle sh = log.getSegmentHandleForAddress(new LogAddress((long) logChunk, null));

        assertThat(logChunk).isGreaterThan(StreamLogFiles.TRIM_THRESHOLD);
        assertThat(sh.getPendingTrims().size()).isEqualTo(logChunk);
        assertThat(sh.getTrimmedAddresses().size()).isEqualTo(0);

        // Fill the rest of the log segment and compact
        for (long x = logChunk; x < logChunk * 2; x++) {
            writeToLog(log, x);
        }

        // Verify the pending trims are compacted after the log segment has filled
        File file = new File(sh.getFileName());
        long sizeBeforeCompact = file.length();
        log.compact();
        file = new File(sh.getFileName());
        long sizeAfterCompact = file.length();

        assertThat(sizeAfterCompact).isLessThan(sizeBeforeCompact);

        // Reload the segment handler and check that the first half of the segment has been trimmed
        sh = log.getSegmentHandleForAddress(new LogAddress((long) logChunk, null));
        assertThat(sh.getTrimmedAddresses().size()).isEqualTo(logChunk);
        assertThat(sh.getKnownAddresses().size()).isEqualTo(logChunk);

        for (long x = logChunk; x < logChunk * 2; x++) {
            assertThat(sh.getKnownAddresses().get(x)).isNotNull();
        }

        // Verify that the trimmed addresses cannot be written to or read from after compaction
        for (long x = 0; x < logChunk; x++) {
            LogAddress logAddress = new LogAddress(x, null);
            assertThatThrownBy(() -> log.read(logAddress))
                    .isInstanceOf(TrimmedException.class);

            final long address = x;
            assertThatThrownBy(() -> writeToLog(log, address))
                    .isInstanceOf(OverwriteException.class);
        }
    }

    @Test
    public void testWritingFileHeader() throws Exception {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);
        writeToLog(log, 0L);
        log.sync(true);
        StreamLogFiles log2 = new StreamLogFiles(getContext(), false);
        writeToLog(log2, 1L);
        log2.sync(true);
        StreamLogFiles log3 = new StreamLogFiles(getContext(), false);
        assertThat(log3.read(new LogAddress(1L, null))).isNotNull();
    }

    @Test
    public void testGetGlobalTail() {
        StreamLogFiles log = new StreamLogFiles(getContext(), false);

        assertThat(log.getGlobalTail()).isEqualTo(0);

        // Write to multiple segments
        final int segments = 3;
        long lastAddress = segments * StreamLogFiles.RECORDS_PER_LOG_FILE;
        for (long x = 0; x <= lastAddress; x++){
            writeToLog(log, x);
            assertThat(log.getGlobalTail()).isEqualTo(x);
        }

        // Restart and try to retrieve the global tail
        log = new StreamLogFiles(getContext(), false);
        assertThat(log.getGlobalTail()).isEqualTo(lastAddress);

        // Advance the tail some more
        final long tailDelta = 5;
        for (long x = lastAddress + 1; x <= lastAddress + tailDelta; x++){
            writeToLog(log, x);
            assertThat(log.getGlobalTail()).isEqualTo(x);
        }

        // Restart and try to retrieve the global tail one last time
        log = new StreamLogFiles(getContext(), false);
        assertThat(log.getGlobalTail()).isEqualTo(lastAddress + tailDelta);
    }

    @Test
    public void testPrefixTrim() {
        String logDir = getContext().getServerConfig().get("--log-path") + File.separator + "log";
        StreamLog log = new StreamLogFiles(getContext(), false);

        // Write 50 segments and trim the first 25
        final long numSegments = 50;
        final long filesPerSegment = 3;
        for(long x = 0; x < numSegments * StreamLogFiles.RECORDS_PER_LOG_FILE; x++) {
            writeToLog(log, x);
        }

        File logs = new File(logDir);

        assertThat((long) logs.list().length).isEqualTo(numSegments * filesPerSegment);

        final long endSegment = 25;
        long trimAddress = endSegment * StreamLogFiles.RECORDS_PER_LOG_FILE + 1;
        log.prefixTrim(new LogAddress(trimAddress, null));
        log.compact();

        // Verify that first 25 segments have been deleted
        String[] afterTrimFiles = logs.list();
        assertThat((long) afterTrimFiles.length).isEqualTo((numSegments - endSegment) * filesPerSegment);

        Set<String> fileNames = new HashSet(Arrays.asList(afterTrimFiles));
        for (long x = endSegment + 1; x < numSegments; x++) {
            String logFile = Long.toString(x) + ".log";
            String trimmedLogFile = StreamLogFiles.getTrimmedFilePath(logFile);
            String pendingLogFile = StreamLogFiles.getPendingTrimsFilePath(logFile);

            assertThat(fileNames).contains(logFile);
            assertThat(fileNames).contains(trimmedLogFile);
            assertThat(fileNames).contains(pendingLogFile);
        }

        // Try to trim an address that is less than the new starting address
        assertThatThrownBy(() -> log.prefixTrim(new LogAddress(trimAddress, null)))
                .isInstanceOf(TrimmedException.class);

        long trimmedExceptions = 0;

        // Try to read trimmed addresses
        for(long x = 0; x < numSegments * StreamLogFiles.RECORDS_PER_LOG_FILE; x++) {
            try {
                log.read(new LogAddress(x, null));
            } catch (TrimmedException e) {
                trimmedExceptions++;
            }
        }

        // Address 0 is not reflected in trimAddress
        assertThat(trimmedExceptions).isEqualTo(trimAddress + 1);
    }
}