package org.corfudb.infrastructure.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;


/**
 * Created by maithem on 11/2/16.
 */
public class StreamLogFilesTest extends AbstractCorfuTest {

    private String getDirPath() {
        return getTempDir() + File.separator;
    }

    @Test
    public void testWriteReadWithChecksum() {
        // Enable checksum, then write and read the same entry
        StreamLog log = new StreamLogFiles(getDirPath(), false);
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogAddress address0 = new LogAddress((long) 0, null);
        log.append(address0, new LogData(DataType.DATA, b));
        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);

        // Disable checksum, then write and read then same entry
        log = new StreamLogFiles(getDirPath(), true);
        log.append(address0, new LogData(DataType.DATA, b));
        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);
    }

    @Test
    public void testOverwriteException() {
        StreamLog log = new StreamLogFiles(getDirPath(), false);
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogAddress address0 = new LogAddress((long) 0, null);
        log.append(address0, new LogData(DataType.DATA, b));

        assertThatThrownBy(() -> log.append(address0, new LogData(DataType.DATA, b)))
                .isInstanceOf(OverwriteException.class);
    }

    @Test
    public void testSync() throws Exception {
        StreamLogFiles log = new StreamLogFiles(getDirPath(), false);
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        long seg1 = StreamLogFiles.RECORDS_PER_LOG_FILE * 0 + 1;
        long seg2 = StreamLogFiles.RECORDS_PER_LOG_FILE * 1 + 1;
        long seg3 = StreamLogFiles.RECORDS_PER_LOG_FILE * 2 + 1;

        log.append(new LogAddress(seg1, null), new LogData(DataType.DATA, b));
        log.append(new LogAddress(seg2, null), new LogData(DataType.DATA, b));
        log.append(new LogAddress(seg3, null), new LogData(DataType.DATA, b));

        assertThat(log.getChannelsToSync().size()).isEqualTo(3);

        log.sync();

        assertThat(log.getChannelsToSync().size()).isEqualTo(0);
    }

    @Test
    public void testReadingUnknownAddress() {
        StreamLog log = new StreamLogFiles(getDirPath(), false);
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
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
        String logDir = getDirPath();
        StreamLog log = new StreamLogFiles(logDir, true);
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogAddress address0 = new LogAddress((long) 0, null);
        log.append(address0, new LogData(DataType.DATA, b));

        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);

        log.close();

        // Re-open stream log with checksum enabled
        assertThatThrownBy(() -> new StreamLogFiles(logDir, false))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testStreamLogDataCorruption() throws Exception {
        // This test manipulates a log file directly and manipulates
        // log records by overwriting some parts of the record simulating
        // different data corruption scenarios
        String logDir = getDirPath();
        StreamLog log = new StreamLogFiles(logDir, false);
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogAddress address0 = new LogAddress((long) 0, null);
        log.append(address0, new LogData(DataType.DATA, b));

        assertThat(log.read(address0).getPayload(null)).isEqualTo(streamEntry);
        log.close();

        // Overwrite 2 bytes of the checksum and 2 bytes of the entry's address
        String logFilePath = logDir + 0 + ".log";
        RandomAccessFile file = new RandomAccessFile(logFilePath, "rw");
        file.seek(StreamLogFiles.LogFileHeader.size + 4);
        file.writeInt(0xffff);
        file.close();

        StreamLog log2 = new StreamLogFiles(logDir, false);
        assertThatThrownBy(() -> log2.read(address0))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(DataCorruptionException.class);
        log2.close();

        // Overwrite the delimiter
        file = new RandomAccessFile(logFilePath, "rw");
        file.seek(StreamLogFiles.LogFileHeader.size );
        file.writeInt(0xffff);
        file.close();

        StreamLog log3 = new StreamLogFiles(logDir, false);
        assertThat(log3.read(address0)).isNull();
    }

    @Test
    public void multiThreadedReadWrite() throws Exception {
        String logDir = getDirPath();
        StreamLog log = new StreamLogFiles(logDir, false);

        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);

        final int num_threads = 2;
        final int num_entries = 100;

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_entries;
            for (int i = base; i < base + num_entries; i++) {
                LogAddress address = new LogAddress((long) i, null);
                log.append(address, new LogData(DataType.DATA, b));
            }
        });

        executeScheduled(num_threads, 30, TimeUnit.SECONDS);

        // verify that addresses 0 to 2000 have been used up
        for (int x = 0; x < num_entries * num_threads; x++) {
            LogAddress address = new LogAddress((long) x, null);
            LogData data = log.read(address);
            byte[] bytes = (byte[]) data.getPayload(null);
            assertThat(bytes).isEqualTo(streamEntry);
        }
    }
}