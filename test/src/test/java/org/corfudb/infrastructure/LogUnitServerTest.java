package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.LoadingCache;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;

/**
 * Created by mwei on 2/4/16.
 */
public class LogUnitServerTest extends AbstractServerTest {

    @Override
    public AbstractServer getDefaultServer() {
        return new LogUnitServer(new ServerContextBuilder().build());
    }

    @Test
    public void checkOverwritesFail() throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        LogUnitServer s1 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        this.router.reset();
        this.router.addServer(s1);

        final long ADDRESS_0 = 0L;
        final long ADDRESS_1 = 100L;
        //write at 0
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(ADDRESS_0);
        // m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setStreams(Collections.EMPTY_SET);
        m.setRank(ADDRESS_0);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));

        assertThat(s1)
                .containsDataAtAddress(ADDRESS_0);
        assertThat(s1)
                .isEmptyAtAddress(ADDRESS_1);


        // repeat: this should throw an exception
        WriteRequest m2 = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m2.setGlobalAddress(ADDRESS_0);
        // m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m2.setStreams(Collections.EMPTY_SET);
        m2.setRank(ADDRESS_0);
        m2.setBackpointerMap(Collections.emptyMap());

        sendMessage(CorfuMsgType.WRITE.payloadMsg(m2));
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.ERROR_OVERWRITE);

    }

    @Test
    public void checkHeapLeak() throws Exception {

        LogUnitServer s1 = new LogUnitServer(ServerContextBuilder.emptyContext());

        this.router.reset();
        this.router.addServer(s1);
        long address = 0L;
        final byte TEST_BYTE = 42;
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer(1);
        b.writeByte(TEST_BYTE);
        WriteRequest wr = WriteRequest.builder()
                            .writeMode(WriteMode.NORMAL)
                            .data(new LogData(DataType.DATA, b))
                            .build();
        //write at 0
        wr.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        wr.setRank(0L);
        wr.setBackpointerMap(Collections.emptyMap());
        wr.setGlobalAddress(0L);

        sendMessage(CorfuMsgType.WRITE.payloadMsg(wr));

        LoadingCache<LogAddress, LogData> dataCache = s1.getDataCache();
        // Make sure that extra bytes are truncated from the payload byte buf
        Assertions.assertThat(dataCache.get(new LogAddress(address, null)).getData().capacity()).isEqualTo(1);
    }

    @Test
    public void checkThatWritesArePersisted()
            throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        LogUnitServer s1 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        this.router.reset();
        this.router.addServer(s1);

        final long LOW_ADDRESS = 0L;
        final long MID_ADDRESS = 100L;
        final long HIGH_ADDRESS = 10000000L;
        //write at 0
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(LOW_ADDRESS);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));
        //100
        b = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize("100".getBytes(), b);
        m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(MID_ADDRESS);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));
        //and 10000000
        b = ByteBufAllocator.DEFAULT.buffer();
        Serializers.CORFU.serialize("10000000".getBytes(), b);
        m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(HIGH_ADDRESS);
        m.setStreams(Collections.singleton(CorfuRuntime.getStreamID("a")));
        m.setRank(0L);
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));

        assertThat(s1)
                .containsDataAtAddress(LOW_ADDRESS)
                .containsDataAtAddress(MID_ADDRESS)
                .containsDataAtAddress(HIGH_ADDRESS);
        assertThat(s1)
                .matchesDataAtAddress(LOW_ADDRESS, "0".getBytes())
                .matchesDataAtAddress(MID_ADDRESS, "100".getBytes())
                .matchesDataAtAddress(HIGH_ADDRESS, "10000000".getBytes());

        s1.shutdown();

        LogUnitServer s2 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());
        this.router.reset();
        this.router.addServer(s2);

        assertThat(s2)
                .containsDataAtAddress(LOW_ADDRESS)
                .containsDataAtAddress(MID_ADDRESS)
                .containsDataAtAddress(HIGH_ADDRESS);
        assertThat(s2)
                .matchesDataAtAddress(LOW_ADDRESS, "0".getBytes())
                .matchesDataAtAddress(MID_ADDRESS, "100".getBytes())
                .matchesDataAtAddress(HIGH_ADDRESS, "10000000".getBytes());
    }

    private String createLogFile(String path, int version, boolean noVerify) throws IOException {
        // Generate a log file and manually change the version
        File logDir = new File(path + File.separator + "log");
        logDir.mkdir();

        // Create a log file with an invalid log version
        String logFilePath = logDir.getAbsolutePath() + File.separator + 0 + ".log";
        File logFile = new File(logFilePath);
        logFile.createNewFile();
        RandomAccessFile file = new RandomAccessFile(logFile, "rw");
        StreamLogFiles.writeHeader(file.getChannel(), version, noVerify);
        file.close();

        return logFile.getAbsolutePath();
    }

    @Test (expected = RuntimeException.class)
    public void testInvalidLogVersion() throws Exception {
        // Create a log file with an invalid version
        String tempDir = PARAMETERS.TEST_TEMP_DIR;
        createLogFile(tempDir, StreamLogFiles.VERSION + 1, false);

        // Start a new logging version
        ServerContextBuilder builder = new ServerContextBuilder();
        builder.setMemory(false);
        builder.setLogPath(tempDir);
        ServerContext context = builder.build();
        LogUnitServer logunit = new LogUnitServer(context);
    }

    @Test (expected = RuntimeException.class)
    public void testVerifyWithNoVerifyLog() throws Exception {
        boolean noVerify = true;

        // Generate a log file without computing the checksum for log entries
        String tempDir = PARAMETERS.TEST_TEMP_DIR;
        createLogFile(tempDir, StreamLogFiles.VERSION + 1, noVerify);

        // Start a new logging version
        ServerContextBuilder builder = new ServerContextBuilder();
        builder.setMemory(false);
        builder.setLogPath(tempDir);
        builder.setNoVerify(!noVerify);
        ServerContext context = builder.build();
        LogUnitServer logunit = new LogUnitServer(context);
    }
}

