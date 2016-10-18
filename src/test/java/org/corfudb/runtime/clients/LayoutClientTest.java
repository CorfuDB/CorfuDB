package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 12/21/15.
 */
public class LayoutClientTest extends AbstractClientTest {

    LayoutClient client;

    @Override
    Set<AbstractServer> getServersForTest() {
        return new ImmutableSet.Builder<AbstractServer>()
                .add(new LayoutServer(defaultServerContext()))
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

    @Test
    public void nonBootstrappedServerThrowsException() {
        assertThatThrownBy(() -> {
            client.getLayout().get();
        }).hasCauseInstanceOf(NoBootstrapException.class);
    }

    @Test
    public void bootstrapServerInstallsNewLayout()
            throws Exception {
        assertThat(client.bootstrapLayout(TestLayoutBuilder.single(9000)).get())
                .isEqualTo(true);

        assertThat(client.getLayout().get().asJSONString())
                .isEqualTo(TestLayoutBuilder.single(9000).asJSONString());
    }

    @Test
    public void cannotBootstrapServerTwice()
            throws Exception {
        assertThat(client.bootstrapLayout(TestLayoutBuilder.single(9000)).get())
                .isEqualTo(true);
        assertThatThrownBy(() -> client.bootstrapLayout(TestLayoutBuilder.single(9000)).get())
                .hasCauseInstanceOf(AlreadyBootstrappedException.class);
    }

    @Test
    public void canGetNewLayoutInDifferentEpoch()
            throws Exception {
        Layout l = TestLayoutBuilder.single(9000);
        l.setEpoch(42L);
        assertThat(client.bootstrapLayout(l).get())
                .isEqualTo(true);

        assertThat(client.getLayout().get().getEpoch())
                .isEqualTo(42L);
    }

    @Test
    public void prepareRejectsLowerRanks()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(9000);
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(layout, 10L).get().isAccepted())
                .isEqualTo(true);

        assertThatThrownBy(() -> {
            client.prepare(layout, 5L).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> {
            client.prepare(layout, 2L).get();
        }).hasCauseInstanceOf(OutrankedException.class);
    }

    @Test
    public void proposeRejectsLowerRanks()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(9000);
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(layout, 10L).get().isAccepted())
                .isEqualTo(true);

        assertThatThrownBy(() -> {
            client.propose(5L, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThat(client.propose(10L, TestLayoutBuilder.single(9000)).get())
                .isEqualTo(true);
    }

    @Test
    public void proposeRejectsAlreadyProposed()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(9000);
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(layout, 10L).get().isAccepted())
                .isEqualTo(true);

        client.propose(10L, layout).get();

        assertThatThrownBy(() -> {
            client.propose(5L, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> {
            client.propose(10L, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);
    }

    @Test
    public void commitReturnsAck()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(9000);
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(layout, 10L).get().isAccepted())
                .isEqualTo(true);

        layout.setEpoch(777);

        assertThat(client.committed(10L, layout).get())
                .isEqualTo(true);
    }

}
