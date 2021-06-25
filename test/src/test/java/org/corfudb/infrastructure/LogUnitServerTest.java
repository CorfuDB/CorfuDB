package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.infrastructure.log.LogFormat.LogHeader;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LogUnitException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.proto.service.LogUnit.TailRequestMsg.Type;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getReadLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTailRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getTrimLogRequestMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogUnit.getWriteLogRequestMsg;
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
    public void verifyLogTailQueriesEpoch() {
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

        TailsResponse req1 = (TailsResponse) sendRequest(getTailRequestMsg(Type.LOG_TAIL), ClusterIdCheck.CHECK, EpochCheck.CHECK).join();
        TailsResponse req2 = (TailsResponse) sendRequest(getTailRequestMsg(Type.ALL_STREAMS_TAIL), ClusterIdCheck.CHECK, EpochCheck.CHECK).join();

        assertThat(req1.getEpoch()).isEqualTo(sc.getCurrentLayout().getEpoch());
        assertThat(req2.getEpoch()).isEqualTo(sc.getCurrentLayout().getEpoch());
    }

    @Test
    public void checkOverwritesFail() {
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
        // write at 0
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize("0".getBytes(), b);
        LogData ld = new LogData(DataType.DATA, b);
        ld.setGlobalAddress(ADDRESS_0);
        ld.setBackpointerMap(Collections.emptyMap());
        sendRequest(getWriteLogRequestMsg(ld), ClusterIdCheck.CHECK, EpochCheck.CHECK).join();
        assertThat(s1)
                .containsDataAtAddress(ADDRESS_0);
        assertThat(s1)
                .isEmptyAtAddress(ADDRESS_1);

        // repeat: this should throw an exception
        LogData ld2 = new LogData(DataType.DATA, b);
        ld2.setGlobalAddress(ADDRESS_0);
        ld2.setBackpointerMap(Collections.emptyMap());

        CompletableFuture<Boolean> future = sendRequest(getWriteLogRequestMsg(ld2), ClusterIdCheck.CHECK, EpochCheck.CHECK);
        assertThatThrownBy(future::join).hasCauseExactlyInstanceOf(OverwriteException.class);
    }

    /**
     * Test that corfu refuses to start if the filesystem/directory is/becomes read-only
     *
     * @throws Exception
     */
    @Test
    public void cantOpenReadOnlyLogFiles() {
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

        final long LOW_ADDRESS = 0L;
        final String low_payload = "0";
        final long MID_ADDRESS = 100L;
        final String mid_payload = "100";
        final long HIGH_ADDRESS = 10000000L;
        final String high_payload = "100000";
        final String streamName = "a";
        //write at 0, 100 & 10_000_000
        rawWrite(LOW_ADDRESS, low_payload, streamName);
        rawWrite(MID_ADDRESS, mid_payload, streamName);
        rawWrite(HIGH_ADDRESS, high_payload, streamName);

        s1.shutdown();

        try {
            File serviceDirectory = new File(serviceDir);
            serviceDirectory.setWritable(false);
        } catch (SecurityException e) {
            fail("Should not hit security exception" + e.toString());
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

        final long LOW_ADDRESS = 0L;
        final String low_payload = "0";
        final long MID_ADDRESS = 100L;
        final String mid_payload = "100";
        final long HIGH_ADDRESS = 10000000L;
        final String high_payload =
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
        LogData ld = new LogData(DataType.DATA, b);
        ld.setGlobalAddress(addr);
        ld.setBackpointerMap(Collections.singletonMap(CorfuRuntime.getStreamID(streamName),
                Address.NO_BACKPOINTER));
        return sendRequest(getWriteLogRequestMsg(ld), ClusterIdCheck.CHECK, EpochCheck.CHECK);
    }

    @Test
    public void checkThatMoreWritesArePersisted() {
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

        final long START_ADDRESS = 0L;
        final String low_payload = "0";
        final int num_iterations_very_low = PARAMETERS.NUM_ITERATIONS_VERY_LOW;
        final String streamName = "a";

        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < num_iterations_very_low; i++) {
            futures.add(rawWrite(START_ADDRESS + i, low_payload + i, streamName));
        }

        futures.forEach(CompletableFuture::join);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s1)
                    .containsDataAtAddress(START_ADDRESS + i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s1)
                    .matchesDataAtAddress(START_ADDRESS + i, (low_payload + i)
                            .getBytes());

        s1.shutdown();

        LogUnitServer s2 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        setServer(s2);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s2)
                    .containsDataAtAddress(START_ADDRESS + i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s2)
                    .matchesDataAtAddress(START_ADDRESS + i, (low_payload + i).getBytes());

        futures = new ArrayList<>();
        for (int i = 0; i < num_iterations_very_low; i++)
            futures.add(rawWrite(START_ADDRESS + num_iterations_very_low + i, low_payload + i, streamName));

        futures.forEach(CompletableFuture::join);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s2)
                    .containsDataAtAddress(START_ADDRESS + num_iterations_very_low + i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s2)
                    .matchesDataAtAddress(
                            START_ADDRESS + num_iterations_very_low + i,
                            (low_payload + i).getBytes()
                    );

        LogUnitServer s3 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        setServer(s3);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s3)
                    .containsDataAtAddress(START_ADDRESS + i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s3)
                    .matchesDataAtAddress(START_ADDRESS + i, (low_payload + i).getBytes());

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s3)
                    .containsDataAtAddress(START_ADDRESS + num_iterations_very_low + i);

        for (int i = 0; i < num_iterations_very_low; i++)
            assertThat(s3)
                    .matchesDataAtAddress(
                            START_ADDRESS + num_iterations_very_low + i,
                            (low_payload + i).getBytes()
                    );

    }

    /**
     * This test verifies that on Log Unit reset/restart, a stream address map and its corresponding
     * trim mark is properly set.
     *
     */
    @Test
    public void checkLogUnitResetStreamRebuilt() {
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
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (long i = maxAddress; i >= minAddress; i--) {
            ByteBuf b = Unpooled.buffer();
            Serializers.CORFU.serialize("Payload".getBytes(), b);
            LogData ld = new LogData(DataType.DATA, b);
            ld.setGlobalAddress(i);
            long backpointer = i - 1;
            if (i == minAddress) {
                // Last entry, backpointer is -6 (non-exist).
                backpointer = Address.NON_EXIST;
            }
            ld.setBackpointerMap(Collections.singletonMap(streamID, backpointer));
            futures.add(sendRequest(getWriteLogRequestMsg(ld), ClusterIdCheck.CHECK, EpochCheck.CHECK));
        }

        futures.forEach(CompletableFuture::join);

        // Retrieve address space from current log unit server (write path)
        StreamAddressSpace addressSpace = s1.getStreamAddressSpace(streamID);
        assertThat(addressSpace.getTrimMark()).isEqualTo(Address.NON_EXIST);
        assertThat(addressSpace.size()).isEqualTo(minAddress + 1);

        // Instantiate new log unit server (restarts) so the log is read and address maps are rebuilt.
        LogUnitServer newServer = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());

        // Retrieve address space from new initialized log unit server (bootstrap path)
        addressSpace = newServer.getStreamAddressSpace(streamID);
        assertThat(addressSpace.getTrimMark()).isEqualTo(Address.NON_EXIST);
        assertThat(addressSpace.size()).isEqualTo(minAddress + 1);

        // Trim the log, and verify that trim mark is updated on log unit
        newServer.prefixTrim(trimMark);
        sendRequest(getTrimLogRequestMsg(new Token(0L, trimMark)), ClusterIdCheck.CHECK, EpochCheck.CHECK).join();

        // Retrieve address space from current log unit server (after a prefix trim)
        addressSpace = newServer.getStreamAddressSpace(streamID);
        assertThat(addressSpace.getTrimMark()).isEqualTo(trimMark);
        assertThat(addressSpace.size()).isEqualTo(maxAddress - trimMark);
    }

    @Test
    public void checkUnCachedWrites() {
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
        LogData ld = new LogData(DataType.DATA, b);
        final Long globalAddress = 0L;
        ld.setGlobalAddress(globalAddress);
        Map<UUID, Long> uuidLongMap = new HashMap<>();
        UUID uuid = new UUID(1, 1);
        final Long address = 5L;
        uuidLongMap.put(uuid, address);
        ld.setBackpointerMap(uuidLongMap);
        sendRequest(getWriteLogRequestMsg(ld), ClusterIdCheck.CHECK, EpochCheck.CHECK).join();

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

    private String createLogFile(String path, int version, boolean noVerify) throws Exception {
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

    public void writeHeader(FileChannel fileChannel, int version, boolean verify) throws Exception {

        LogHeader header = LogHeader.newBuilder()
                .setVersion(version)
                .setVerifyChecksum(verify)
                .build();

        ByteBuffer buf = StreamLogFiles.getByteBufferWithMetaData(header);
        do {
            fileChannel.write(buf);
        } while (buf.hasRemaining());
        fileChannel.force(true);
    }

    @Test(expected = RuntimeException.class)
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

    @Test(expected = RuntimeException.class)
    public void testVerifyWithNoVerifyLog() throws Exception {
        boolean verifyChecksum = false;

        // Generate a log file without computing the checksum for log entries
        String tempDir = PARAMETERS.TEST_TEMP_DIR;
        createLogFile(tempDir, StreamLogFiles.VERSION + 1, !verifyChecksum);

        // Start a new logging version
        ServerContextBuilder builder = new ServerContextBuilder();
        builder.setMemory(false);
        builder.setLogPath(tempDir);
        builder.setVerifyChecksum(verifyChecksum);
        ServerContext context = builder.build();
        LogUnitServer logunit = new LogUnitServer(context);
    }

    @Test
    public void testLogDataRWLatency() {
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

        final int iterations = 1000;
        long ADDRESS;

        long start = System.currentTimeMillis();
        for (int i = 0; i < iterations; i++) {
            ByteBuf buf = Unpooled.buffer();
            Serializers.CORFU.serialize(Integer.toString(i).getBytes(), buf);
            LogData ld = new LogData(DataType.DATA, buf);
            ADDRESS = i;
            ld.setGlobalAddress(ADDRESS);
            ld.setBackpointerMap(populateBackpointerMap());
            ld.setCheckpointedStreamId(UUID.randomUUID());
            ld.setCheckpointedStreamId(UUID.randomUUID());
            ld.setCheckpointId(UUID.randomUUID());
            sendRequest(getWriteLogRequestMsg(ld), ClusterIdCheck.CHECK, EpochCheck.CHECK).join();
        }
        long end = System.currentTimeMillis();
        System.out.println("Total Write Time - " + (end - start));

        start = System.currentTimeMillis();
        for (int i = 0; i < iterations; i++) {
            ADDRESS = i;
            sendRequest(getReadLogRequestMsg(Collections.singletonList(ADDRESS), true), ClusterIdCheck.CHECK, EpochCheck.CHECK).join();
        }
        end = System.currentTimeMillis();
        System.out.println("Total Read Time -" + (end - start));
    }

    private Map<UUID, Long> populateBackpointerMap() {
        final int numBackpointers = 20;
        Map<UUID, Long> backPointerMap = new HashMap<>();
        for (int i = 0; i < numBackpointers; i++) {
            backPointerMap.put(UUID.randomUUID(), new Random().nextLong());
        }
        return backPointerMap;
    }
}
