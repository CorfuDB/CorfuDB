package org.corfudb.runtime.clients;

import com.google.common.collect.*;
import org.corfudb.infrastructure.IServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.Utils;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
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
    Set<IServer> getServersForTest() {
        return new ImmutableSet.Builder<IServer>()
                .add(new LogUnitServer(defaultOptionsMap()))
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
    throws Exception
    {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        LogUnitReadResponseMsg.ReadResult r = client.read(0).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(r.getPayload(null))
                .isEqualTo(testString);
    }

    @Test
    public void overwriteThrowsException()
        throws Exception
    {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        assertThatThrownBy(() -> client.write(0,Collections.<UUID>emptySet(), 0,
                testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillDoesNotOverwrite()
        throws Exception
    {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        client.fillHole(0).get();
        LogUnitReadResponseMsg.ReadResult r = client.read(0).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(r.getPayload(null))
                .isEqualTo(testString);
    }

    @Test
    public void holeFillCannotBeOverwritten()
        throws Exception
    {
        byte[] testString = "hello world".getBytes();
        client.fillHole(0).get();
        LogUnitReadResponseMsg.ReadResult r = client.read(0).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.FILLED_HOLE);

        assertThatThrownBy(() -> client.write(0,Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void backpointersCanBeWrittenAndRead()
            throws Exception
    {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString,
                ImmutableMap.<UUID,Long>builder()
                    .put(CorfuRuntime.getStreamID("hello"), 1337L)
                    .put(CorfuRuntime.getStreamID("hello2"), 1338L)
                    .build()).get();

        LogUnitReadResponseMsg.ReadResult r = client.read(0).get();
        assertThat(r.getBackpointerMap())
                .containsEntry(CorfuRuntime.getStreamID("hello"), 1337L);
        assertThat(r.getBackpointerMap())
                .containsEntry(CorfuRuntime.getStreamID("hello2"), 1338L);
    }

    @Test
    public void contiguousTailIsCorrect()
            throws Exception
    {
        byte[] testString = "hello world".getBytes();
        assertThat(client.getContiguousTail(null).get().contiguousTail)
                .isEqualTo(-1);
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        client.forceCompact();
        assertThat(client.getContiguousTail(null).get().contiguousTail)
                .isEqualTo(0);
        client.write(1, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        client.forceCompact();
        assertThat(client.getContiguousTail(null).get().contiguousTail)
                .isEqualTo(1);
        client.write(100, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        client.forceCompact();
        assertThat(client.getContiguousTail(null).get().contiguousTail)
                .isEqualTo(1);
    }

    @Test
    public void contiguousStreamIsCorrect()
            throws Exception
    {
        byte[] testString = "hello world".getBytes();
        UUID streamA = CorfuRuntime.getStreamID("a");
        UUID streamB = CorfuRuntime.getStreamID("b");
        assertThat(client.getContiguousTail(streamA).get().getRange().contains(0L))
                .isFalse();

        client.write(0, Collections.singleton(streamA), 0, testString, Collections.emptyMap()).get();
        client.forceCompact();
        assertThat(client.getContiguousTail(streamA).get().getRange().contains(0L))
                .isTrue();

        client.write(1, Collections.singleton(streamA), 0, testString, Collections.emptyMap()).get();
        client.forceCompact();
        assertThat(client.getContiguousTail(streamA).get().getRange().contains(0L))
                .isTrue();
        assertThat(client.getContiguousTail(streamA).get().getRange().contains(1L))
                .isTrue();

        client.write(2, Collections.singleton(streamB), 0, testString, Collections.emptyMap()).get();
        client.forceCompact();
        assertThat(client.getContiguousTail(streamA).get().getRange().contains(0L))
                .isTrue();
        assertThat(client.getContiguousTail(streamA).get().getRange().contains(1L))
                .isTrue();
        assertThat(client.getContiguousTail(streamB).get().getRange().contains(2L))
                .isTrue();

        client.write(100, Collections.singleton(streamB), 0, testString, Collections.emptyMap()).get();
        client.forceCompact();
        assertThat(client.getContiguousTail(streamA).get().getRange().contains(0L))
                .isTrue();
        assertThat(client.getContiguousTail(streamA).get().getRange().contains(1L))
                .isTrue();
        assertThat(client.getContiguousTail(streamB).get().getRange().contains(2L))
                .isTrue();
    }

    @Test
    public void canReadRange()
            throws Exception
    {
        RangeSet<Long> ranges = TreeRangeSet.create();
        ranges.add(Range.closed(0L, 100L));
        for (int i = 0; i < 100; i++) {
            client.write(i, Collections.<UUID>emptySet(), 0,
                    Integer.toString(i).getBytes(), Collections.emptyMap()).get();
        }


       Map<Long, LogUnitReadResponseMsg.ReadResult> rm = client.readRange(ranges).get();
       for (int i = 0; i < 100; i++)
       {
           assertThat(rm)
                   .containsKey((long)i);
           assertThat(rm.get((long)i).getPayload(null))
                   .isEqualTo(Integer.toString(i).getBytes());
       }
    }
}
