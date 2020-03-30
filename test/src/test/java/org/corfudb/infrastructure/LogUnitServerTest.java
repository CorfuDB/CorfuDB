package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LogUnitException;

import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

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

        ServerContext sc = new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setSingle(true)
                .setMemory(false)
                .build();

        sc.installSingleNodeLayoutIfAbsent();
        sc.setServerRouter(router);
        sc.setServerEpoch(sc.getCurrentLayout().getEpoch(), router);

        LogUnitServer s1 = new LogUnitServer(sc);

        setServer(s1);
        setContext(sc);

        final long ADDRESS_0 = 0L;
        final long ADDRESS_1 = 100L;
        //write at 0
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(ADDRESS_0);
        m.setBackpointerMap(Collections.emptyMap());
        sendRequest(CorfuMsgType.WRITE.payloadMsg(m)).join();
        assertThat(s1)
                .containsDataAtAddress(ADDRESS_0);
        assertThat(s1)
                .isEmptyAtAddress(ADDRESS_1);


        // repeat: this should throw an exception
        WriteRequest m2 = WriteRequest.builder()
                .data(new LogData(DataType.DATA, b))
                .build();
        m2.setGlobalAddress(ADDRESS_0);
        m2.setBackpointerMap(Collections.emptyMap());

        CompletableFuture<Boolean> future = sendRequest(CorfuMsgType.WRITE.payloadMsg(m2));
        assertThatThrownBy(future::join).hasCauseExactlyInstanceOf(OverwriteException.class);
    }

    /**
     * Test that corfu refuses to start if the filesystem/directory is/becomes read-only
     *
     * @throws Exception
     */
    @Test
    public void cantOpenReadOnlyLogFiles() throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        ServerContext sc = new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setSingle(true)
                .setMemory(false)
                .build();

        sc.installSingleNodeLayoutIfAbsent();
        sc.setServerRouter(router);
        sc.setServerEpoch(sc.getCurrentLayout().getEpoch(), router);

        LogUnitServer s1 = new LogUnitServer(sc);

        setServer(s1);
        setContext(sc);

        final long LOW_ADDRESS = 0L; final String low_payload = "0";
        final long MID_ADDRESS = 100L; final String mid_payload = "100";
        final long HIGH_ADDRESS = 10000000L; final String high_payload = "100000";
        final String streamName = "a";
        //write at 0, 100 & 10_000_000
        rawWrite(LOW_ADDRESS, low_payload, streamName);
        rawWrite(MID_ADDRESS, mid_payload, streamName);
        rawWrite(HIGH_ADDRESS, high_payload, streamName);

        s1.shutdown();

        try {
            File serviceDirectory = new File(serviceDir);
            serviceDirectory.setWritable(false);
        } catch(SecurityException e) {
            fail("Should not hit security exception"+e.toString());
        }

        try {
            LogUnitServer s2 = new LogUnitServer(new ServerContextBuilder()
                    .setLogPath(serviceDir)
                    .setMemory(false)
                    .build());
            fail("Should have failed to startup in read-only mode");
        } catch (LogUnitException e) {
            // Correctly failed to open on read-only directory
        }
        // In case the directory is re-used for other tests, restore its write permissions.
        File serviceDirectory = new File(serviceDir);
        serviceDirectory.setWritable(true);
    }

    @Test
    public void checkThatWritesArePersisted()
            throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        ServerContext sc = new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setSingle(true)
                .setMemory(false)
                .build();

        sc.installSingleNodeLayoutIfAbsent();
        sc.setServerRouter(router);
        sc.setServerEpoch(sc.getCurrentLayout().getEpoch(), router);

        LogUnitServer s1 = new LogUnitServer(sc);

        setServer(s1);
        setContext(sc);

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
        rawWrite(HIGH_ADDRESS, high_payload, streamName).join();

        s1.shutdown();

        LogUnitServer s2 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        setServer(s2);

        assertThat(s2)
                .containsDataAtAddress(LOW_ADDRESS)
                .containsDataAtAddress(MID_ADDRESS)
                .containsDataAtAddress(HIGH_ADDRESS);
        assertThat(s2)
                .matchesDataAtAddress(LOW_ADDRESS, low_payload.getBytes())
                .matchesDataAtAddress(MID_ADDRESS, mid_payload.getBytes())
                .matchesDataAtAddress(HIGH_ADDRESS, high_payload.getBytes());
    }

    protected CompletableFuture<Boolean> rawWrite(long addr, String s, String streamName) {
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize(s.getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(addr);
        m.setBackpointerMap(Collections.singletonMap(CorfuRuntime.getStreamID(streamName),
                Address.NO_BACKPOINTER));
        return sendRequest(CorfuMsgType.WRITE.payloadMsg(m));
    }

    @Test
    public void checkThatMoreWritesArePersisted()
            throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        ServerContext sc = new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setSingle(true)
                .setMemory(false)
                .build();

        sc.installSingleNodeLayoutIfAbsent();
        sc.setServerRouter(router);
        sc.setServerEpoch(sc.getCurrentLayout().getEpoch(), router);

        LogUnitServer s1 = new LogUnitServer(sc);

        setServer(s1);
        setContext(sc);

        final long START_ADDRESS = 0L; final String low_payload = "0";
        final int num_iterations_very_low = PARAMETERS.NUM_ITERATIONS_VERY_LOW;
        final String streamName = "a";

        CompletableFuture<Boolean> future = null;
        for (int i = 0; i < num_iterations_very_low; i++)
            future = rawWrite(START_ADDRESS+i, low_payload+i, streamName);

        future.join();

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

        setServer(s2);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s2)
                    .containsDataAtAddress(START_ADDRESS+i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s2)
                    .matchesDataAtAddress(START_ADDRESS+i, (low_payload+i)
                            .getBytes());

        for (int i = 0; i < num_iterations_very_low; i++)
            future = rawWrite(START_ADDRESS+num_iterations_very_low+i, low_payload+i, streamName);

        future.join();
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

        setServer(s3);

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

    /**
     * This test verifies that on Log Unit reset/restart, a stream address map and its corresponding
     * trim mark is properly set.
     *
     * @throws Exception
     */
    @Test
    public void checkLogUnitResetStreamRebuilt() throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        final long maxAddress = 20000L;
        final long minAddress = 10000L;
        final long trimMark = 15000L;
        UUID streamID = UUID.randomUUID();

        ServerContext sc = new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setSingle(true)
                .setMemory(false)
                .build();

        sc.installSingleNodeLayoutIfAbsent();
        sc.setServerRouter(router);
        sc.setServerEpoch(sc.getCurrentLayout().getEpoch(), router);

        LogUnitServer s1 = new LogUnitServer(sc);

        setServer(s1);
        setContext(sc);

        // Write 10K entries in descending order for the range 20k-10K
        CompletableFuture<Boolean> future = null;
        for (long i = maxAddress; i >= minAddress; i--) {
            ByteBuf b = Unpooled.buffer();
            Serializers.CORFU.serialize("Payload".getBytes(), b);
            WriteRequest m = WriteRequest.builder()
                    .data(new LogData(DataType.DATA, b))
                    .build();
            m.setGlobalAddress(i);
            long backpointer = i - 1;
            if (i == minAddress) {
                // Last entry, backpointer is -6 (non-exist).
                backpointer = Address.NON_EXIST;
            }
            m.setBackpointerMap(Collections.singletonMap(streamID, backpointer));
            future = sendRequest(CorfuMsgType.WRITE.payloadMsg(m));
        }

        future.join();

        // Retrieve address space from current log unit server (write path)
        StreamAddressSpace addressSpace = s1.getStreamAddressSpace(streamID);
        assertThat(addressSpace.getTrimMark()).isEqualTo(Address.NON_EXIST);
        assertThat(addressSpace.getAddressMap().getLongCardinality()).isEqualTo(minAddress + 1);

        // Instantiate new log unit server (restarts) so the log is read and address maps are rebuilt.
        LogUnitServer newServer = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        // Retrieve address space from new initialized log unit server (bootstrap path)
        addressSpace = newServer.getStreamAddressSpace(streamID);
        assertThat(addressSpace.getTrimMark()).isEqualTo(Address.NON_EXIST);
        assertThat(addressSpace.getAddressMap().getLongCardinality()).isEqualTo(minAddress + 1);

        // Trim the log, and verify that trim mark is updated on log unit
        newServer.prefixTrim(trimMark);
        sendRequest(CorfuMsgType.PREFIX_TRIM.payloadMsg(new TrimRequest(new Token(0l, trimMark)))).join();


        // Retrieve address space from current log unit server (after a prefix trim)
        addressSpace = newServer.getStreamAddressSpace(streamID);
        assertThat(addressSpace.getTrimMark()).isEqualTo(trimMark);
        assertThat(addressSpace.getAddressMap().getLongCardinality()).isEqualTo(maxAddress - trimMark);
    }

    @Test
    public void checkUnCachedWrites() throws Exception {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        ServerContext sc = new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setSingle(true)
                .setMemory(false)
                .build();

        sc.installSingleNodeLayoutIfAbsent();
        sc.setServerRouter(router);
        sc.setServerEpoch(sc.getCurrentLayout().getEpoch(), router);

        LogUnitServer s1 = new LogUnitServer(sc);

        setServer(s1);
        setContext(sc);

        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .data(new LogData(DataType.DATA, b))
                .build();
        final Long globalAddress = 0L;
        m.setGlobalAddress(globalAddress);
        Map<UUID, Long> uuidLongMap = new HashMap();
        UUID uuid = new UUID(1,1);
        final Long address = 5L;
        uuidLongMap.put(uuid, address);
        m.setBackpointerMap(uuidLongMap);
        sendRequest(CorfuMsgType.WRITE.payloadMsg(m)).join();

        s1 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        setServer(s1);

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
        writeHeader(file.getChannel(), version, noVerify);
        file.close();

        return logFile.getAbsolutePath();
    }

    public void writeHeader(FileChannel fileChannel, int version, boolean verify) throws IOException {

        Types.LogHeader header = Types.LogHeader.newBuilder()
                .setVersion(version)
                .setVerifyChecksum(verify)
                .build();

        ByteBuffer buf = StreamLogFiles.getByteBufferWithMetaData(header);
        do {
            fileChannel.write(buf);
        } while (buf.hasRemaining());
        fileChannel.force(true);
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

        ServerContext sc = new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setSingle(true)
                .setMemory(false)
                .build();

        sc.installSingleNodeLayoutIfAbsent();
        sc.setServerRouter(router);
        sc.setServerEpoch(sc.getCurrentLayout().getEpoch(), router);

        LogUnitServer s1 = new LogUnitServer(sc);

        setServer(s1);
        setContext(sc);

        final long ADDRESS_0 = 0L;
        final long ADDRESS_1 = 100L;
        //write at 0
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        WriteRequest m = WriteRequest.builder()
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(ADDRESS_0);
        m.setRank(new IMetadata.DataRank(0));
        m.setBackpointerMap(Collections.emptyMap());
        sendRequest(CorfuMsgType.WRITE.payloadMsg(m)).join();
        assertThat(s1)
                .containsDataAtAddress(ADDRESS_0);
        assertThat(s1)
                .isEmptyAtAddress(ADDRESS_1);


        // repeat: do not throw exception, the overwrite is forced
        b.clear();
        b = Unpooled.buffer();
        Serializers.CORFU.serialize("1".getBytes(), b);
        m = WriteRequest.builder()
                .data(new LogData(DataType.DATA, b))
                .build();
        m.setGlobalAddress(ADDRESS_0);
        m.setBackpointerMap(Collections.emptyMap());


        WriteRequest m2 = WriteRequest.builder()
                .data(new LogData(DataType.DATA, b))
                .build();

        m2.setGlobalAddress(ADDRESS_0);
        m2.setRank(new IMetadata.DataRank(1));
        m2.setBackpointerMap(Collections.emptyMap());

        assertThat(sendRequest(CorfuMsgType.WRITE.payloadMsg(m2)).join()).isEqualTo(true);

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
        setServer(s2);

        assertThat(s2)
                .containsDataAtAddress(ADDRESS_0);
        assertThat(s2)
                .matchesDataAtAddress(ADDRESS_0, "1".getBytes());

    }
}

