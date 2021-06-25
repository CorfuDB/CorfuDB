package org.corfudb.runtime.clients;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.StreamLogFiles.METADATA_SIZE;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.log.LogFormat.Metadata;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

/**
 * Created by mwei on 12/14/15.
 */
public class LogUnitHandlerTest extends AbstractClientTest {

    private LogUnitClient client;
    private ServerContext serverContext;

    private final UUID clientId1 = UUID.fromString("7903ba37-e3e7-407b-b9e9-f8eacaa94d5e");
    private final UUID clientId2 = UUID.fromString("a15bbffc-91cb-4a96-bb30-e1ecc5f522da");

    private LogData getLogDataWithoutId(long address) {
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogData ld = new LogData(DataType.DATA, b);
        ld.setGlobalAddress(address);

        return ld;
    }

    @Override
    Set<AbstractServer> getServersForTest() {
        String dirPath = PARAMETERS.TEST_TEMP_DIR;
        serverContext = new ServerContextBuilder()
                .setSingle(true)
                .setVerifyChecksum(true)
                .setMemory(false)
                .setLogPath(dirPath)
                .setServerRouter(serverRouter)
                .build();

        serverContext.installSingleNodeLayoutIfAbsent();
        serverRouter.setServerContext(serverContext);
        serverContext.setServerEpoch(serverContext.getCurrentLayout().getEpoch(), serverRouter);
        LogUnitServer server = new LogUnitServer(serverContext);
        return new ImmutableSet.Builder<AbstractServer>()
                .add(server)
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        LogUnitHandler logUnitHandler = new LogUnitHandler();
        client = new LogUnitClient(router, 0L, UUID.fromString("00000000-0000-0000-0000-000000000000"));
        return new ImmutableSet.Builder<IClient>()
                .add(new BaseHandler())
                .add(logUnitHandler)
                .build();
    }

    @Test
    public void canReadWrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, testString, Collections.emptyMap()).get();
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);
    }

    @Test
    public void readingEmptyAddress() throws Exception {
        final long address0 = 0;
        LogData r = client.read(address0).get().getAddresses().get(0L);
        assertThat(r.isEmpty()).isTrue();
        assertThat(r.getGlobalAddress()).isEqualTo(address0);
        assertThat(LogData.getEmpty(0)).isEqualTo(LogData.getEmpty(0));
    }

    @Test
    public void writeNonSequentialRange() throws Exception {
        final long address0 = 0;
        final long address4 = 4;

        List<LogData> entries = new ArrayList<>();
        ByteBuf b = Unpooled.buffer();
        byte[] streamEntry = "Payload".getBytes();
        Serializers.CORFU.serialize(streamEntry, b);
        LogData ld0 = new LogData(DataType.DATA, b);
        ld0.setGlobalAddress(address0);
        entries.add(ld0);

        LogData ld4 = new LogData(DataType.DATA, b);
        ld4.setGlobalAddress(address4);
        entries.add(ld4);

        assertThatThrownBy(() -> client.writeRange(entries).get())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void writeRange() throws Exception {
        final int numIter = 100;

        List<LogData> entries = new ArrayList<>();
        for (int x = 0; x < numIter; x++) {
            ByteBuf b = Unpooled.buffer();
            byte[] streamEntry = "Payload".getBytes();
            Serializers.CORFU.serialize(streamEntry, b);
            LogData ld = new LogData(DataType.DATA, b);
            ld.setGlobalAddress((long) x);
            entries.add(ld);
        }

        client.writeRange(entries).get();

        // Ensure that the overwrite detection mechanism is working as expected.
        Assertions.assertThatExceptionOfType(ExecutionException.class).isThrownBy(
                () -> client.writeRange(entries).get())
                .withCauseInstanceOf(OverwriteException.class);

        // "Restart the logging unit
        LogUnitServer server2 = new LogUnitServer(serverContext);
        serverRouter.reset();
        serverRouter.addServer(server2);

        List<LogData> readEntries = new ArrayList<>();
        for (int x = 0; x < numIter; x++) {
            LogData ld = client.read(x).get().getAddresses().get((long) x);
            readEntries.add(ld);
        }

        assertThat(entries).isEqualTo(readEntries);
    }

    @Test
    public void quotaExceededTest() throws Exception {
        serverRouter.reset();
        String dirPath = PARAMETERS.TEST_TEMP_DIR;
        final int maxLogSizeInBytes = 1000;
        FileStore corfuDirBackend = Files.getFileStore(Paths.get(dirPath));
        final long fsSize = corfuDirBackend.getTotalSpace();
        final double maxLogSizeInPercentage = maxLogSizeInBytes * 100.0 / fsSize;
        ServerContext sc = serverContext = new ServerContextBuilder()
                .setMemory(false)
                .setLogPath(dirPath)
                .setServerRouter(serverRouter)
                .setLogSizeLimitPercentage(maxLogSizeInPercentage)
                .build();
        LogUnitServer server = new LogUnitServer(sc);
        serverRouter.addServer(server);

        final long address0 = 0L;
        final long address1 = 1L;
        final long address2 = 2L;
        final long address3 = 3L;
        final long address4 = 4L;

        byte[] payload = new byte[maxLogSizeInBytes / 2];

        client.write(address0, payload, Collections.emptyMap()).get();
        client.write(address1, payload, Collections.emptyMap()).get();
        assertThatThrownBy(() -> client.write(address2, payload, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(QuotaExceededException.class);
        // Before elevating the clients priority, try to write a hole and verify that it goes through
        // even after the quota has been exceeded
        client.write(LogData.getHole(address4)).get();
        // After the quota has been exceeded, we bump up the client priority level and write again
        client.setPriorityLevel(PriorityLevel.HIGH);
        client.write(address3, payload, Collections.emptyMap()).get();
        assertThat(client.read(address0).get().getAddresses().get(address0).getType()).isEqualTo(DataType.DATA);
        assertThat(client.read(address1).get().getAddresses().get(address1).getType()).isEqualTo(DataType.DATA);
        assertThat(client.read(address2).get().getAddresses().get(address2).getType()).isEqualTo(DataType.EMPTY);
        assertThat(client.read(address3).get().getAddresses().get(address3).getType()).isEqualTo(DataType.DATA);
        assertThat(client.read(address4).get().getAddresses().get(address4).getType()).isEqualTo(DataType.HOLE);
    }

    @Test
    public void readingTrimmedAddress() throws Exception {
        byte[] testString = "hello world".getBytes();
        final long address0 = 0;
        final long address1 = 1;
        client.write(address0, testString, Collections.emptyMap()).get();
        client.write(address1, testString, Collections.emptyMap()).get();
        LogData r = client.read(address0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        r = client.read(address1).get().getAddresses().get(1L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);

        client.prefixTrim(new Token(0L, address0)).get();
        client.compact().get();

        // For logunit cache flush
        LogUnitServer server2 = new LogUnitServer(serverContext);
        serverRouter.reset();
        serverRouter.addServer(server2);

        LogData trimmedAddress = client.read(address0).get().getAddresses().get(0L);

        assertThat(trimmedAddress.isTrimmed()).isTrue();
        assertThat(trimmedAddress.getGlobalAddress()).isEqualTo(address0);
    }

    @Test
    public void flushLogUnitCache() throws Exception {
        LogUnitServer server2 = new LogUnitServer(serverContext);
        serverRouter.reset();
        serverRouter.addServer(server2);

        assertThat(server2.getDataCache().getSize()).isEqualTo(0);
        byte[] testString = "hello world".getBytes();
        client.write(0, testString, Collections.emptyMap()).get();
        assertThat(server2.getDataCache().getSize()).isEqualTo(1);
        client.flushCache().get();
        assertThat(server2.getDataCache().getSize()).isEqualTo(0);
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(server2.getDataCache().getSize()).isEqualTo(1);
    }


    private ILogData.SerializationHandle createEmptyData(long position, DataType type) {
        ILogData data = new LogData(type);
        data.setGlobalAddress(position);
        return data.getSerializedForm(true);
    }

    @Test
    public void overwriteThrowsException()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, testString, Collections.emptyMap()).get();
        assertThatThrownBy(() -> client.write(0,
                testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillCannotBeOverwritten()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        final long address0 = 0;
        Token token = new Token(0, address0);
        client.write(LogData.getHole(token)).get();
        LogData r = client.read(address0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.HOLE);
        assertThat(r.getGlobalAddress()).isEqualTo(address0);

        assertThatThrownBy(() -> client.write(address0, testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillCannotOverwrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, testString, Collections.emptyMap()).get();

        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);

        Condition<Throwable> conditionOverwrite = new Condition<>(e -> {
            if (e.getCause().getClass().equals(OverwriteException.class)) {
                OverwriteException oe = (OverwriteException) e.getCause();
                return oe.getOverWriteCause().getId() == OverwriteCause.DIFF_DATA.getId();
            } else {
                return false;
            }
        }, "Expected overwrite cause to be DIFF_DATA");
        Token token = new Token(0, 0);
        assertThatThrownBy(() -> client.write(LogData.getHole(token)).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class)
                .has(conditionOverwrite);
    }

    @Test
    public void multiReadTest() throws Exception {
        byte[] payload = "payload".getBytes();

        CorfuRuntimeParameters p = CorfuRuntimeParameters.builder().build();
        final int numBatches = 3;
        for (long x = 0; x < numBatches * p.getBulkReadSize(); x++) {
            client.write(x, payload, Collections.emptyMap()).get();
        }

        // Read half a batch
        List<Long> halfBatch = new ArrayList<>();
        final int half = p.getBulkReadSize() / 2;
        for (long x = 0; x < half; x++) {
            halfBatch.add(x);
        }

        ReadResponse resp = client.read(halfBatch, false).get();
        assertThat(resp.getAddresses().size()).isEqualTo(half);

        // Read two batches
        List<Long> twoBatchAddresses = new ArrayList<>();
        final int twoBatches = p.getBulkReadSize() * 2;
        for (long x = 0; x < twoBatches; x++) {
            twoBatchAddresses.add(x);
        }

        resp = client.read(twoBatchAddresses, false).get();
        assertThat(resp.getAddresses().size()).isEqualTo(twoBatches);
    }

    @Test
    public void backpointersCanBeWrittenAndRead()
            throws Exception {
        final long ADDRESS_0 = 1337L;
        final long ADDRESS_1 = 1338L;

        byte[] testString = "hello world".getBytes();
        client.write(0, testString,
                ImmutableMap.<UUID, Long>builder()
                        .put(CorfuRuntime.getStreamID("hello"), ADDRESS_0)
                        .put(CorfuRuntime.getStreamID("hello2"), ADDRESS_1)
                        .build()).get();

        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getBackpointerMap())
                .containsEntry(CorfuRuntime.getStreamID("hello"), ADDRESS_0);
        assertThat(r.getBackpointerMap())
                .containsEntry(CorfuRuntime.getStreamID("hello2"), ADDRESS_1);
    }

    @Test
    public void CorruptedDataReadThrowsException() throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, testString, Collections.emptyMap()).get();
        client.write(StreamLogFiles.RECORDS_PER_LOG_FILE + 1,
                testString, Collections.emptyMap()).get();

        // Corrupt the written log entry
        String logDir = serverContext.getConfiguration().getLogDir();
        String logFilePath = logDir + File.separator + "0.log";
        RandomAccessFile file = new RandomAccessFile(logFilePath, "rw");

        ByteBuffer metaDataBuf = ByteBuffer.allocate(METADATA_SIZE);
        file.getChannel().read(metaDataBuf);
        metaDataBuf.flip();

        LogUnitServer server2 = new LogUnitServer(serverContext);
        serverRouter.reset();
        serverRouter.addServer(server2);

        Metadata metadata = Metadata.parseFrom(metaDataBuf.array());
        final int fileOffset = Integer.BYTES + METADATA_SIZE + metadata.getLength() + 20;
        final int CORRUPT_BYTES = 0xFFFF;
        file.seek(fileOffset); // Skip file header
        file.writeInt(CORRUPT_BYTES);
        file.close();

        // Verify that the correct inspect addresses fails properly when log entry
        // is corrupted
        final int start = 0;
        final int end = 10;

        assertThatThrownBy(() -> client.inspectAddresses(LongStream.range(start, end)
                .boxed().collect(Collectors.toList())).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseExactlyInstanceOf(DataCorruptionException.class);

        // Try to read a corrupted log entry
        assertThatThrownBy(() -> client.read(0).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseExactlyInstanceOf(DataCorruptionException.class);
    }

    /**
     * Testing that the clientId/ThreadId is persisted and that two
     * LogData entries are equal if they have the same runtime and are coming
     * from the same thread.
     * @throws Exception
     */
    @Test
    public void logDataWrittenIsEqualToLogDataRead() throws Exception {
        long address = 0L;
        LogData ld = getLogDataWithoutId(address);

        ld.setId(clientId1);
        client.write(ld).join();

        LogData ldPrime = client.read(address).get().getAddresses().get(address);

        assertThat(ld).isEqualTo(ldPrime);
        assertThat(ldPrime.getMetadataMap().get(IMetadata.LogUnitMetadataType.CLIENT_ID))
                .isEqualTo(clientId1);
        assertThat(ldPrime.getMetadataMap().get(IMetadata.LogUnitMetadataType.THREAD_ID))
                .isEqualTo(Thread.currentThread().getId());
    }

    /**
     * Ensure that same LogData payload written by two different thread
     * are not equals
     * @throws Exception
     */
    @Test
    public void logDataWrittenFromOtherThreadIsNotEqual() throws Exception {
        long address = 0L;
        LogData ldThisThread = getLogDataWithoutId(address);
        ldThisThread.setId(clientId1);
        LogData ldOtherThread = getLogDataWithoutId(address);

        // Set clientId from another thread
        t1(() -> ldOtherThread.setId(clientId1));
        client.write(ldOtherThread).join();

        LogData ldPrime = client.read(address).get().getAddresses().get(address);
        assertThat(ldThisThread).isNotEqualTo(ldPrime);
        assertThat(ldOtherThread).isEqualTo(ldPrime);
    }

    /**
     * Ensure that same LogData payload written by two different client
     * are not equals
     * @throws Exception
     */
    @Test
    public void logDataWrittenWithOtherClientIdIsNotEqual() throws Exception {
        long address = 0L;

        LogData ldOne = getLogDataWithoutId(address);
        LogData ldTwo = getLogDataWithoutId(address);

        ldOne.setId(clientId1);
        ldTwo.setId(clientId2);

        client.write(ldOne).join();

        LogData ldRead = client.read(address).get().getAddresses().get(address);
        assertThat(ldRead).isEqualTo(ldOne);
        assertThat(ldRead).isNotEqualTo(ldTwo);
    }

    /**
     * Ensure log unit client can query log tail.
     * @throws Exception
     */
    @Test
    public void canQueryLogTail() throws Exception {
        final long numEntries = 2L;

        // Write 2 Entries
        byte[] testString = "hello".getBytes();
        byte[] testString2 = "world".getBytes();
        client.write(0, testString, Collections.emptyMap()).get();
        client.write(1, testString2, Collections.emptyMap()).get();

        CompletableFuture<TailsResponse> cf = client.getLogTail();
        long tail = cf.get().getLogTail();
        assertThat(tail).isEqualTo(numEntries-1);
    }

    /**
     * Ensure log unit client can query stream's address space and log address space.
     * @throws Exception
     */
    @Test
    public void canQueryStreamAddressSpaceAndLogSpace() throws Exception {
        final UUID streamId = UUID.randomUUID();
        final long numEntries = 2L;
        final long addressOne = 0L;
        final long addressTwo = 1L;

        // Write 2 entries
        // 1. Entry in Address 0
        LogData ldOne = getLogDataWithoutId(addressOne);
        Map<UUID, Long> backpointerMap = new HashMap<>();
        backpointerMap.put(streamId, Address.NON_EXIST);
        ldOne.setBackpointerMap(backpointerMap);
        client.write(ldOne).join();

        // 2. Entry in address 1
        LogData ldTwo = getLogDataWithoutId(addressTwo);
        backpointerMap = new HashMap<>();
        backpointerMap.put(streamId, addressOne);
        ldTwo.setBackpointerMap(backpointerMap);
        client.write(ldTwo).join();

        // Get Stream's Address Space
        StreamAddressSpace addressSpace = client.getLogAddressSpace().join().getAddressMap().get(streamId);
        assertThat(addressSpace.getTrimMark()).isEqualTo(Address.NON_EXIST);
        assertThat(addressSpace.size()).isEqualTo(numEntries);
        assertThat(addressSpace.contains(addressOne));

        // Get Log Address Space (stream's address space + log tail)
        CompletableFuture<StreamsAddressResponse> cfLog = client.getLogAddressSpace();
        StreamsAddressResponse response = cfLog.get();
        addressSpace = response.getAddressMap().get(streamId);
        assertThat(addressSpace.getTrimMark()).isEqualTo(Address.NON_EXIST);
        assertThat(addressSpace.size()).isEqualTo(numEntries);
        assertThat(addressSpace.contains(addressOne));
        assertThat(response.getLogTail()).isEqualTo(addressTwo);
    }

    /**
     * Ensure log unit can return correct inspect address result in different scenarios.
     */
    @Test
    public void testInspectAddress() throws Exception {
        byte[] testString = "hello world".getBytes();
        final long startAddr = 0L;
        final long endAddr = 100L;
        final long holeAddr = 60L;
        final long trimAddr = 40L;

        for (long addr = startAddr; addr < endAddr; addr++) {
            if (addr != holeAddr) {
                client.write(addr, testString, Collections.emptyMap()).join();
            }
        }

        // InspectAddress should return an empty address at holeAddr.
        assertThat(client.inspectAddresses(LongStream.range(startAddr, endAddr)
                .boxed().collect(Collectors.toList())).join().getEmptyAddresses()
        ).isEqualTo(Collections.singletonList(holeAddr));

        // Perform a prefix trim.
        client.prefixTrim(Token.of(client.getEpoch(), trimAddr)).join();

        // If client attempt to inspect any address before the trim mark, a
        // TrimmedException should be thrown to inform the client to retry
        // auto commit from a larger address.
        assertThatThrownBy(() ->
                client.inspectAddresses(LongStream.range(startAddr, endAddr)
                .boxed().collect(Collectors.toList())).join()
        ).hasCauseExactlyInstanceOf(TrimmedException.class);

        // Fill the hole, even a LogData of HOLE type should work.
        client.write(LogData.getHole(holeAddr)).join();

        // Now every address should be consolidated.
        assertThat(client.inspectAddresses(LongStream.range(trimAddr + 1, endAddr)
                .boxed().collect(Collectors.toList())).join().getEmptyAddresses()
        ).isEqualTo(Collections.emptyList());
    }
}
