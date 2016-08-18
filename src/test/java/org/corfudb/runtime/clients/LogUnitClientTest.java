package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 12/14/15.
 */
public class LogUnitClientTest extends AbstractClientTest {

    LogUnitClient client;

    @Override
    Set<AbstractServer> getServersForTest() {
        return new ImmutableSet.Builder<AbstractServer>()
                .add(new LogUnitServer(defaultServerContext()))
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
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        LogData r = client.read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);
    }

    @Test
    public void overwriteThrowsException()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        assertThatThrownBy(() -> client.write(0, Collections.<UUID>emptySet(), 0,
                testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillDoesNotOverwrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        client.fillHole(0).get();
        LogData r = client.read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);
    }

    @Test
    public void holeFillCannotBeOverwritten()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.fillHole(0).get();
        LogData r = client.read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.HOLE);

        assertThatThrownBy(() -> client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void backpointersCanBeWrittenAndRead()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString,
                ImmutableMap.<UUID, Long>builder()
                        .put(CorfuRuntime.getStreamID("hello"), 1337L)
                        .put(CorfuRuntime.getStreamID("hello2"), 1338L)
                        .build()).get();

        LogData r = client.read(0).get().getReadSet().get(0L);
        assertThat(r.getBackpointerMap())
                .containsEntry(CorfuRuntime.getStreamID("hello"), 1337L);
        assertThat(r.getBackpointerMap())
                .containsEntry(CorfuRuntime.getStreamID("hello2"), 1338L);
    }

    @Test
    public void streamTokenIsInitiallyZero()
            throws Exception {
        assertThat(client.getStreamToken(CorfuRuntime.getStreamID("test")).get())
                .isEqualTo(0L);
    }

    @Test
    public void streamTokenIncrementsWithWrite()
            throws Exception {
        assertThat(client.getStreamToken(CorfuRuntime.getStreamID("test")).get())
                .isEqualTo(0L);
        ByteBuf b = ByteBufAllocator.DEFAULT.buffer();
        b.writeBytes("hello".getBytes());
        client.writeStream(1L, CorfuRuntime.getStreamID("test"),
                Collections.singleton(CorfuRuntime.getStreamID("test")),
                        b);
        assertThat(client.getStreamToken(CorfuRuntime.getStreamID("test")).get())
                .isEqualTo(1L);
    }
}
