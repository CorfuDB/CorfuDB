package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.IServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg;
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
        client.write(0, Collections.<UUID>emptySet(), 0, testString).get();
        LogUnitReadResponseMsg.ReadResult r = client.read(0).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(r.getPayload())
                .isEqualTo(testString);
    }

    @Test
    public void overwriteThrowsException()
        throws Exception
    {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString).get();
        assertThatThrownBy(() -> client.write(0,Collections.<UUID>emptySet(), 0, testString).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillDoesNotOverwrite()
        throws Exception
    {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString).get();
        client.fillHole(0).get();
        LogUnitReadResponseMsg.ReadResult r = client.read(0).get();
        assertThat(r.getResultType())
                .isEqualTo(LogUnitReadResponseMsg.ReadResultType.DATA);
        assertThat(r.getPayload())
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

        assertThatThrownBy(() -> client.write(0,Collections.<UUID>emptySet(), 0, testString).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }
}
