package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.IServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 12/21/15.
 */
public class LayoutClientTest extends AbstractClientTest {

    LayoutClient client;

    @Override
    Set<IServer> getServersForTest() {
        return new ImmutableSet.Builder<IServer>()
                .add(new LayoutServer(defaultOptionsMap()))
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        client = new LayoutClient();
        return new ImmutableSet.Builder<IClient>()
                .add(new BaseClient())
                .add(client)
                .build();
    }

    Layout getTestLayout() {
        String localAddress = "localhost:9999";
        return new Layout(
                Collections.singletonList(localAddress),
                Collections.singletonList(localAddress),
                Collections.singletonList(new Layout.LayoutSegment(
                        Layout.ReplicationMode.CHAIN_REPLICATION,
                        0L,
                        -1L,
                        Collections.singletonList(
                                new Layout.LayoutStripe(
                                        Collections.singletonList(localAddress)
                                )
                        )
                )),
                0L
        );
    }

    @Test
    public void nonBootstrappedServerThrowsException()
    {
        assertThatThrownBy(() -> {
          client.getLayout().get();
        }).hasCauseInstanceOf(NoBootstrapException.class);
    }

    @Test
    public void bootstrapServerInstallsNewLayout()
            throws Exception
    {
        assertThat(client.bootstrapLayout(getTestLayout()).get())
                .isEqualTo(true);

        assertThat(client.getLayout().get().asJSONString())
                .isEqualTo(getTestLayout().asJSONString());
    }

    @Test
    public void cannotBootstrapServerTwice()
            throws Exception
    {
        assertThat(client.bootstrapLayout(getTestLayout()).get())
                .isEqualTo(true);
        assertThat(client.bootstrapLayout(getTestLayout()).get())
                .isEqualTo(false);
    }

    @Test
    public void prepareRejectsLowerRanks()
            throws Exception
    {
        assertThat(client.bootstrapLayout(getTestLayout()).get())
                .isEqualTo(true);

        assertThat(client.prepare(10L).get())
                .isEqualTo(true);

        assertThatThrownBy(() -> {
            client.prepare(5L).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> {
            client.prepare(2L).get();
        }).hasCauseInstanceOf(OutrankedException.class);
    }

    @Test
    public void proposeRejectsLowerRanks()
            throws Exception
    {
        assertThat(client.bootstrapLayout(getTestLayout()).get())
                .isEqualTo(true);

        assertThat(client.prepare(10L).get())
                .isEqualTo(true);

        assertThatThrownBy(() -> {
            client.propose(5L, getTestLayout()).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThat(client.propose(10L, getTestLayout()).get())
                .isEqualTo(true);
    }

    @Test
    public void proposeRejectsAlreadyProposed()
            throws Exception
    {
        assertThat(client.bootstrapLayout(getTestLayout()).get())
                .isEqualTo(true);

        assertThat(client.prepare(10L).get())
                .isEqualTo(true);

        client.propose(10L, getTestLayout()).get();

        assertThatThrownBy(() -> {
            client.propose(5L, getTestLayout()).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> {
            client.propose(10L, getTestLayout()).get();
        }).hasCauseInstanceOf(OutrankedException.class);
    }

    @Test
    public void commitReturnsAck()
            throws Exception
    {
        assertThat(client.bootstrapLayout(getTestLayout()).get())
                .isEqualTo(true);

        assertThat(client.prepare(10L).get())
                .isEqualTo(true);

        assertThat(client.committed(10L).get())
                .isEqualTo(true);
    }

}
