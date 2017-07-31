package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.IToken;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.ValueAdoptedException;
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
    public void readingTrimmedAddress() throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get();
        client.write(1, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get();
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        r = client.read(1).get().getAddresses().get(1L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);

        client.prefixTrim(0);
        client.compact();

        // For logunit cach flush
        LogUnitServer server2 = new LogUnitServer(serverContext);
        serverRouter.reset();
        serverRouter.addServer(server2);

        assertThat(client.read(0).get().getAddresses().get(0L).isTrimmed()).isTrue();
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
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
        client.fillHole(0).get();
        LogData r = client.read(0).get().getAddresses().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.HOLE);

        assertThatThrownBy(() -> client.write(0, Collections.<UUID>emptySet(), null, testString, Collections.emptyMap()).get())
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

        final int numBatches = 3;
        for (long x = 0; x < numBatches * CorfuRuntime.BULK_READ_SIZE; x++) {
            client.write(x, Collections.emptySet(), null, payload, Collections.emptyMap()).get();
        }

        // Read half a batch
        List<Long> halfBatch = new ArrayList<>();
        final int half = CorfuRuntime.BULK_READ_SIZE / 2;
        for (long x = 0; x < half; x++) {
            halfBatch.add(x);
        }

        ReadResponse resp = client.read(halfBatch).get();
        assertThat(resp.getAddresses().size()).isEqualTo(half);

        // Read two batches
        List<Long> twoBatchAddresses = new ArrayList<>();
        final int twoBatches = CorfuRuntime.BULK_READ_SIZE * 2;
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
