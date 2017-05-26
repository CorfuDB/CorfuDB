package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Random;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 2/4/16.
 */
public class LogUnitServerTest extends AbstractServerTest {

    private static final double minHeapRatio = 0.1;
    private static final double maxHeapRatio = 0.9;

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
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(ADDRESS_0);
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
        m2.setBackpointerMap(Collections.emptyMap());

        sendMessage(CorfuMsgType.WRITE.payloadMsg(m2));
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.ERROR_OVERWRITE);

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

        final long LOW_ADDRESS = 0L; final String low_payload = "0";
        final long MID_ADDRESS = 100L; final String mid_payload = "100";
        final long HIGH_ADDRESS = 10000000L; final String high_payload =
                "100000";
        final String streamName = "a";

        //write at 0
        rawWrite(LOW_ADDRESS, low_payload, streamName);

        //100
        rawWrite(MID_ADDRESS, mid_payload, streamName);

        //and 10000000
        rawWrite(HIGH_ADDRESS, high_payload, streamName);

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
                .matchesDataAtAddress(LOW_ADDRESS, low_payload.getBytes())
                .matchesDataAtAddress(MID_ADDRESS, mid_payload.getBytes())
                .matchesDataAtAddress(HIGH_ADDRESS, high_payload.getBytes());
    }

    protected void rawWrite(long addr, String s, String streamName) {
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize(s.getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(addr);
        m.setBackpointerMap(Collections.singletonMap(CorfuRuntime.getStreamID(streamName),
                Address.NO_BACKPOINTER));
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));

    }

    @Test
    public void checkThatMoreWritesArePersisted()
            throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        LogUnitServer s1 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        this.router.reset();
        this.router.addServer(s1);

        final long START_ADDRESS = 0L; final String low_payload = "0";
        final int num_iterations_very_low = PARAMETERS.NUM_ITERATIONS_VERY_LOW;
        final String streamName = "a";

        for (int i = 0; i < num_iterations_very_low; i++)
            rawWrite(START_ADDRESS+i, low_payload+i, streamName);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s1)
                .containsDataAtAddress(START_ADDRESS+i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s1)
                .matchesDataAtAddress(START_ADDRESS+i, (low_payload+i)
                    .getBytes());

        s1.shutdown();

        LogUnitServer s2 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());
        this.router.reset();
        this.router.addServer(s2);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s2)
                    .containsDataAtAddress(START_ADDRESS+i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s2)
                    .matchesDataAtAddress(START_ADDRESS+i, (low_payload+i)
                            .getBytes());

        for (int i = 0; i < num_iterations_very_low; i++)
            rawWrite(START_ADDRESS+num_iterations_very_low+i, low_payload+i, streamName);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s2)
                    .containsDataAtAddress
                            (START_ADDRESS+num_iterations_very_low+i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s2)
                    .matchesDataAtAddress
                            (START_ADDRESS+num_iterations_very_low+i,
                                    (low_payload+i)
                            .getBytes());

        LogUnitServer s3 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());
        this.router.reset();
        this.router.addServer(s3);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s3)
                    .containsDataAtAddress(START_ADDRESS+i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s3)
                    .matchesDataAtAddress(START_ADDRESS+i, (low_payload+i)
                            .getBytes());

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s3)
                    .containsDataAtAddress
                            (START_ADDRESS+num_iterations_very_low+i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s3)
                    .matchesDataAtAddress
                            (START_ADDRESS+num_iterations_very_low+i,
                                    (low_payload+i)
                                            .getBytes());

    }

    @Test
    public void checkUnCachedWrites() {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        LogUnitServer s1 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        this.router.reset();
        this.router.addServer(s1);

        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        final Long globalAddress = 0L;
        m.setGlobalAddress(globalAddress);
        Set<UUID> streamSet = new HashSet(Collections.singleton(CorfuRuntime.getStreamID("a")));
        Map<UUID, Long> uuidLongMap = new HashMap();
        UUID uuid = new UUID(1,1);
        final Long address = 5L;
        uuidLongMap.put(uuid, address);
        m.setBackpointerMap(uuidLongMap);
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));

        s1 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        this.router.reset();
        this.router.addServer(s1);

        ILogData entry = s1.getDataCache().get(globalAddress);

        // Verify that the meta data can be read correctly
        assertThat(entry.getBackpointerMap()).isEqualTo(uuidLongMap);
        assertThat(entry.getGlobalAddress()).isEqualTo(globalAddress);
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


    @Test
    public void checkOverwriteExceptionIsNotThrownWhenTheRankIsHigher() throws Exception {
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
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(ADDRESS_0);
        m.setRank(new IMetadata.DataRank(0));
        m.setBackpointerMap(Collections.emptyMap());
        sendMessage(CorfuMsgType.WRITE.payloadMsg(m));

        assertThat(s1)
                .containsDataAtAddress(ADDRESS_0);
        assertThat(s1)
                .isEmptyAtAddress(ADDRESS_1);


        // repeat: do not throw exception, the overwrite is forced
        b.clear();
        b = Unpooled.buffer();
        Serializers.CORFU.serialize("1".getBytes(), b);
        m = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(ADDRESS_0);
        m.setBackpointerMap(Collections.emptyMap());


        WriteRequest m2 = WriteRequest.builder()
                .writeMode(WriteMode.NORMAL)
                .data(new LogData(DataType.DATA, b))
                .build();

        m2.setGlobalAddress(ADDRESS_0);
        m2.setRank(new IMetadata.DataRank(1));
        m2.setBackpointerMap(Collections.emptyMap());

        sendMessage(CorfuMsgType.WRITE.payloadMsg(m2));
        Assertions.assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.WRITE_OK);

        // now let's read again and see what we have, we should have the second value (not the first)

        assertThat(s1)
                .containsDataAtAddress(ADDRESS_0);
        assertThat(s1)
                .matchesDataAtAddress(ADDRESS_0, "1".getBytes());

        // and now without the local cache
        LogUnitServer s2 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());
        this.router.reset();
        this.router.addServer(s2);

        assertThat(s2)
                .containsDataAtAddress(ADDRESS_0);
        assertThat(s2)
                .matchesDataAtAddress(ADDRESS_0, "1".getBytes());

    }

    @Test
    public void CheckCacheSizeIsCorrectRatio() throws Exception {

        Random r = new Random(System.currentTimeMillis());
        double randomCacheRatio = minHeapRatio + (maxHeapRatio - minHeapRatio) * r.nextDouble();
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        LogUnitServer s1 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setCacheSizeHeapRatio(String.valueOf(randomCacheRatio))
                .build());


        assertThat(s1).hasCorrectCacheSize(randomCacheRatio);
    }


}

