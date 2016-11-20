package org.corfudb.infrastructure.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.File;
import java.io.RandomAccessFile;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;
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
        log.append(0, new LogData(DataType.DATA, b));
        assertThat(log.read(0).getPayload(null)).isEqualTo(streamEntry);

        // Disable checksum, then write and read then same entry
        log = new StreamLogFiles(getDirPath(), true);
        log.append(0, new LogData(DataType.DATA, b));
        assertThat(log.read(0).getPayload(null)).isEqualTo(streamEntry);
    }

    @Test
    public void testOverwriteException() {
        StreamLog log = new StreamLogFiles(getDirPath(), false);
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        log.append(0, new LogData(DataType.DATA, b));

        assertThatThrownBy(() -> log.append(0, new LogData(DataType.DATA, b)))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void testReadingUnknownAddress() {
        StreamLog log = new StreamLogFiles(getDirPath(), false);
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        log.append(0, new LogData(DataType.DATA, b));
        log.append(2, new LogData(DataType.DATA, b));
        assertThat(log.read(1)).isNull();
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
        log.append(0, new LogData(DataType.DATA, b));

        assertThat(log.read(0).getPayload(null)).isEqualTo(streamEntry);

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
        log.append(0, new LogData(DataType.DATA, b));

        assertThat(log.read(0).getPayload(null)).isEqualTo(streamEntry);
        log.close();

        // Overwrite 2 bytes of the checksum and 2 bytes of the entry's address
        String logFilePath = logDir + 0 + ".log";
        RandomAccessFile file = new RandomAccessFile(logFilePath, "rw");
        file.seek(StreamLogFiles.LogFileHeader.size + 4);
        file.writeInt(0xffff);
        file.close();

        StreamLog log2 = new StreamLogFiles(logDir, false);
        assertThatThrownBy(() -> log2.read(0))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(DataCorruptionException.class);
        log2.close();

        // Overwrite the delimiter
        file = new RandomAccessFile(logFilePath, "rw");
        file.seek(StreamLogFiles.LogFileHeader.size );
        file.writeInt(0xffff);
        file.close();

        StreamLog log3 = new StreamLogFiles(logDir, false);
        assertThat(log3.read(0)).isNull();
    }
}