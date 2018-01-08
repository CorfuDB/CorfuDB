package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.infrastructure.log.StreamLogFiles.METADATA_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by mwei on 12/14/15.
 */
public class LogUnitClientTest extends AbstractClientTest {

    LogUnitClient client;
    ServerContext serverContext;
    String dirPath;
    LogUnitServer server;

    @Override
    Set<AbstractServer> getServersForTest() {
        dirPath = PARAMETERS.TEST_TEMP_DIR;
        serverContext = new ServerContextBuilder()
                .setInitialToken(0)
                .setSingle(false)
                .setNoVerify(false)
                .setMemory(false)
                .setLogPath(dirPath)
                .setServerRouter(serverRouter)
                .build();
        server = new LogUnitServer(serverContext);
        return new ImmutableSet.Builder<AbstractServer>()
                .add(server)
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        client = new LogUnitClient();
        return new ImmutableSet.Builder<IClient>()
                .add(new BaseClient())
                .add(client)
                .build();
    }

    @Test
    public void canReadWrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get();
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
    public void readingTrimmedAddress() throws Exception {
        byte[] testString = "hello world".getBytes();
        final long address0 = 0;
        final long address1 = 1;
        client.write(address0, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get();
        client.write(address1, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get();
        LogData r = client.read(address0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        r = client.read(address1).get().getAddresses().get(1L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);

        client.prefixTrim(address0);
        client.compact();

        // For logunit cach flush
        LogUnitServer server2 = new LogUnitServer(serverContext);
        serverRouter.reset();
        serverRouter.addServer(server2);

        LogData trimmedAddress = client.read(address0).get().getAddresses().get(0L);

        assertThat(trimmedAddress.isTrimmed()).isTrue();
        assertThat(trimmedAddress.getGlobalAddress()).isEqualTo(address0);
    }

    @Test
    public void flushLogunitCache() throws Exception {
        LogUnitServer server2 = new LogUnitServer(serverContext);
        serverRouter.reset();
        serverRouter.addServer(server2);

        assertThat(server2.getDataCache().asMap().size()).isEqualTo(0);
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get();
        assertThat(server2.getDataCache().asMap().size()).isEqualTo(1);
        client.flushCache().get();
        assertThat(server2.getDataCache().asMap().size()).isEqualTo(0);
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(server2.getDataCache().asMap().size()).isEqualTo(1);
    }

    @Test
    public void canReadWriteRanked()
            throws Exception {
        byte[] testString = "hello world".getBytes();

        client.write(0, Collections.<UUID>emptySet(), new IMetadata.DataRank(1), testString, Collections.emptyMap()).get();
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);

        byte[] testString2 = "hello world 2".getBytes();
        client.write(0, Collections.<UUID>emptySet(), new IMetadata.DataRank(2), testString2, Collections.emptyMap()).get();
        r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString2);
    }


    @Test
    public void cannotOutrank() throws ExecutionException, InterruptedException {
        byte[] testString = "hello world".getBytes();

        client.write(0, Collections.<UUID>emptySet(), new IMetadata.DataRank(2), testString, Collections.emptyMap()).get();
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);

        byte[] testString2 = "hello world 2".getBytes();
        try {
            client.write(0, Collections.<UUID>emptySet(), new IMetadata.DataRank(1), testString2, Collections.emptyMap()).get();
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

        client.write(0, Collections.<UUID>emptySet(), new IMetadata.DataRank(1), testString, Collections.emptyMap()).get();
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
        client.write(0, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get();
        assertThatThrownBy(() -> client.write(0, Collections.<UUID>emptySet(), null,
                testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillCannotBeOverwritten()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        final long address0 = 0;
        client.fillHole(address0).get();
        LogData r = client.read(address0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.HOLE);
        assertThat(r.getGlobalAddress()).isEqualTo(address0);

        assertThatThrownBy(() -> client.write(address0, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillCannotOverwrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get();

        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);

        assertThatThrownBy(() -> client.fillHole(0).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void multiReadTest() throws Exception {
        byte[] payload = "payload".getBytes();

        CorfuRuntimeParameters p = CorfuRuntimeParameters.builder().build();
        final int numBatches = 3;
        for (long x = 0; x < numBatches * p.getBulkReadSize(); x++) {
            client.write(x, Collections.emptySet(), null, payload, Collections.emptyMap()).get();
        }

        // Read half a batch
        List<Long> halfBatch = new ArrayList<>();
        final int half = p.getBulkReadSize() / 2;
        for (long x = 0; x < half; x++) {
            halfBatch.add(x);
        }

        ReadResponse resp = client.read(halfBatch).get();
        assertThat(resp.getAddresses().size()).isEqualTo(half);

        // Read two batches
        List<Long> twoBatchAddresses = new ArrayList<>();
        final int twoBatches = p.getBulkReadSize() * 2;
        for (long x = 0; x < twoBatches; x++) {
            twoBatchAddresses.add(x);
        }

        resp = client.read(twoBatchAddresses).get();
        assertThat(resp.getAddresses().size()).isEqualTo(twoBatches);
    }

    @Test
    public void backpointersCanBeWrittenAndRead()
            throws Exception {
        final long ADDRESS_0 = 1337L;
        final long ADDRESS_1 = 1338L;

        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), null, testString,
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
        client.write(0, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get();
        client.write(StreamLogFiles.RECORDS_PER_LOG_FILE + 1, Collections.<UUID>emptySet(), null,
                testString, Collections.emptyMap()).get();

        // Corrupt the written log entry
        String logDir = serverContext.getServerConfig().get("--log-path") + File.separator + "log";
        String logFilePath = logDir + File.separator + "0.log";
        RandomAccessFile file = new RandomAccessFile(logFilePath, "rw");

        ByteBuffer metaDataBuf = ByteBuffer.allocate(METADATA_SIZE);
        file.getChannel().read(metaDataBuf);
        metaDataBuf.flip();

        Types.Metadata metadata = Types.Metadata.parseFrom(metaDataBuf.array());
        final int fileOffset = Integer.BYTES + METADATA_SIZE + metadata.getLength() + 20;
        final int CORRUPT_BYTES = 0xFFFF;
        file.seek(fileOffset); // File header + delimiter
        file.writeInt(CORRUPT_BYTES);
        file.close();

        LogUnitServer server2 = new LogUnitServer(serverContext);
        serverRouter.reset();
        serverRouter.addServer(server2);

        // Try to read a corrupted log entry
        assertThatThrownBy(() -> client.read(0).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(DataCorruptionException.class);
    }
}
