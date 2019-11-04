package org.corfudb.runtime.clients;

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

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.log.StreamLogParams;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.StreamLogParams.RECORDS_PER_SEGMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
                .setInitialToken(0)
                .setSingle(false)
                .setNoVerify(false)
                .setMemory(false)
                .setLogPath(dirPath)
                .setServerRouter(serverRouter)
                .build();
        LogUnitServer server = new LogUnitServer(serverContext);
        return new ImmutableSet.Builder<AbstractServer>()
                .add(server)
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        LogUnitHandler logUnitHandler = new LogUnitHandler();
        client = new LogUnitClient(router, 0L);
        return new ImmutableSet.Builder<IClient>()
                .add(new BaseHandler())
                .add(logUnitHandler)
                .build();
    }

    @Test
    public void canReadWrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, null, testString, Collections.emptyMap()).get();
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
                .setLogSizeLimitPercentage(Double.toString(maxLogSizeInPercentage))
                .build();
        LogUnitServer server = new LogUnitServer(sc);
        serverRouter.addServer(server);

        final long address0 = 0L;
        final long address1 = 1L;
        final long address2 = 2L;
        final long address3 = 3L;
        final long address4 = 4L;

        byte[] payload = new byte[maxLogSizeInBytes / 2];

        client.write(address0, null, payload, Collections.emptyMap()).get();
        client.write(address1, null, payload, Collections.emptyMap()).get();
        assertThatThrownBy(() -> client.write(address2, null, payload, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(QuotaExceededException.class);
        // Before elevating the clients priority, try to write a hole and verify that it goes through
        // even after the quota has been exceeded
        client.write(LogData.getHole(address4)).get();
        // After the quota has been exceeded, we bump up the client priority level and write again
        client.setPriorityLevel(PriorityLevel.HIGH);
        client.write(address3, null, payload, Collections.emptyMap()).get();
        assertThat(client.read(address0).get().getAddresses().get(address0).getType()).isEqualTo(DataType.DATA);
        assertThat(client.read(address1).get().getAddresses().get(address1).getType()).isEqualTo(DataType.DATA);
        assertThat(client.read(address2).get().getAddresses().get(address2).getType()).isEqualTo(DataType.EMPTY);
        assertThat(client.read(address3).get().getAddresses().get(address3).getType()).isEqualTo(DataType.DATA);
        assertThat(client.read(address4).get().getAddresses().get(address4).getType()).isEqualTo(DataType.HOLE);
    }

    @Test
    public void flushLogUnitCache() throws Exception {
        LogUnitServer server2 = new LogUnitServer(serverContext);
        serverRouter.reset();
        serverRouter.addServer(server2);

        assertThat(server2.getDataCache().getSize()).isEqualTo(0);
        byte[] testString = "hello world".getBytes();
        client.write(0, null, testString, Collections.emptyMap()).get();
        assertThat(server2.getDataCache().getSize()).isEqualTo(1);
        client.flushCache().get();
        assertThat(server2.getDataCache().getSize()).isEqualTo(0);
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(server2.getDataCache().getSize()).isEqualTo(1);
    }

    @Test
    public void canReadWriteRanked()
            throws Exception {
        byte[] testString = "hello world".getBytes();

        client.write(0, new IMetadata.DataRank(1), testString, Collections.emptyMap()).get();
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);

        byte[] testString2 = "hello world 2".getBytes();
        client.write(0, new IMetadata.DataRank(2), testString2, Collections.emptyMap()).get();
        r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString2);
    }


    @Test
    public void cannotOutrank() throws ExecutionException, InterruptedException {
        byte[] testString = "hello world".getBytes();

        client.write(0, new IMetadata.DataRank(2), testString, Collections.emptyMap()).get();
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);

        byte[] testString2 = "hello world 2".getBytes();
        try {
            client.write(0, new IMetadata.DataRank(1), testString2, Collections.emptyMap()).get();
            fail();
        } catch (ExecutionException e) {
            // expected
            assertEquals(DataOutrankedException.class, e.getCause().getClass());
        }
        r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);
    }

    @Test
    public void valueCanBeAdopted() throws ExecutionException, InterruptedException {
        byte[] testString = "hello world".getBytes();

        client.write(0, new IMetadata.DataRank(1), testString, Collections.emptyMap()).get();
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType()) .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);

        try {
            ILogData data = createEmptyData(0, DataType.RANK_ONLY,  new IMetadata.DataRank(2)).getSerialized();
            client.write(data).get();
            fail();
        } catch (Exception e) {
            // expected
            assertEquals(ValueAdoptedException.class, e.getCause().getClass());
            ValueAdoptedException ex = (ValueAdoptedException)e.getCause();
            ReadResponse read = ex.getReadResponse();
            LogData log = read.getAddresses().get(0l);
            assertThat(log.getType()).isEqualTo(DataType.DATA);
            assertThat(log.getPayload(new CorfuRuntime())).isEqualTo(testString);;
        }
        r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType()).isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime())).isEqualTo(testString);
    }

    private ILogData.SerializationHandle createEmptyData(long position, DataType type, IMetadata.DataRank rank) {
        ILogData data = new LogData(type);
        data.setRank(rank);
        data.setGlobalAddress(position);
        return data.getSerializedForm();
    }

    @Test
    public void overwriteThrowsException()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, null, testString, Collections.emptyMap()).get();
        assertThatThrownBy(() -> client.write(0, null,
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

        assertThatThrownBy(() -> client.write(address0, null, testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillCannotOverwrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, null, testString, Collections.emptyMap()).get();

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
            client.write(x, null, payload, Collections.emptyMap()).get();
        }

        // Read half a batch
        List<Long> halfBatch = new ArrayList<>();
        final int half = p.getBulkReadSize() / 2;
        for (long x = 0; x < half; x++) {
            halfBatch.add(x);
        }

        ReadResponse resp = client.readAll(halfBatch).get();
        assertThat(resp.getAddresses().size()).isEqualTo(half);

        // Read two batches
        List<Long> twoBatchAddresses = new ArrayList<>();
        final int twoBatches = p.getBulkReadSize() * 2;
        for (long x = 0; x < twoBatches; x++) {
            twoBatchAddresses.add(x);
        }

        resp = client.readAll(twoBatchAddresses).get();
        assertThat(resp.getAddresses().size()).isEqualTo(twoBatches);
    }

    @Test
    public void backpointersCanBeWrittenAndRead()
            throws Exception {
        final long ADDRESS_0 = 1337L;
        final long ADDRESS_1 = 1338L;

        byte[] testString = "hello world".getBytes();
        client.write(0, null, testString,
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

        client.write(0, null, testString, Collections.emptyMap()).get();
        client.write(RECORDS_PER_SEGMENT + 1, null,
                testString, Collections.emptyMap()).get();

        // Corrupt the written log entry
        String logDir = serverContext.getServerConfig().get("--log-path") + File.separator + "log";
        String logFilePath = logDir + File.separator + "0.log";
        RandomAccessFile file = new RandomAccessFile(logFilePath, "rw");

        ByteBuffer metaDataBuf = ByteBuffer.allocate(StreamLogParams.METADATA_SIZE);
        file.getChannel().read(metaDataBuf);
        metaDataBuf.flip();

        LogUnitServer server2 = new LogUnitServer(serverContext);
        serverRouter.reset();
        serverRouter.addServer(server2);

        Types.Metadata metadata = Types.Metadata.parseFrom(metaDataBuf.array());
        final int fileOffset = Integer.BYTES + StreamLogParams.METADATA_SIZE + metadata.getLength() + 20;
        final int CORRUPT_BYTES = 0xFFFF;
        file.seek(fileOffset); // Skip file header
        file.writeInt(CORRUPT_BYTES);
        file.close();

        // Try to read a corrupted log entry
        assertThatThrownBy(() -> client.read(0).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(DataCorruptionException.class);
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
        client.write(0, null, testString, Collections.emptyMap()).get();
        client.write(1, null, testString2, Collections.emptyMap()).get();

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
        client.write(ldOne);

        // 2. Entry in address 1
        LogData ldTwo = getLogDataWithoutId(addressTwo);
        backpointerMap = new HashMap<>();
        backpointerMap.put(streamId, addressOne);
        ldTwo.setBackpointerMap(backpointerMap);
        client.write(ldTwo);

        // Get Stream's Address Space
        CompletableFuture<StreamsAddressResponse> cf = client.getLogAddressSpace();
        StreamAddressSpace addressSpace = cf.get().getAddressMap().get(streamId);
        assertThat(addressSpace.getAddressMap().getLongCardinality()).isEqualTo(numEntries);
        assertThat(addressSpace.getAddressMap().contains(addressOne));

        // Get Log Address Space (stream's address space + log tail)
        CompletableFuture<StreamsAddressResponse> cfLog = client.getLogAddressSpace();
        StreamsAddressResponse response = cfLog.get();
        addressSpace = response.getAddressMap().get(streamId);
        assertThat(addressSpace.getAddressMap().getLongCardinality()).isEqualTo(numEntries);
        assertThat(addressSpace.getAddressMap().contains(addressOne));
        assertThat(response.getLogTail()).isEqualTo(addressTwo);
    }
}
