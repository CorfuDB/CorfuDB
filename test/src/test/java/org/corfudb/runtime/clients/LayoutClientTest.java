package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LayoutCommittedRequest;
import org.corfudb.protocols.wireprotocol.LayoutProposeRequest;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.ServerRejectedException;
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
        long epoch = layout.getEpoch();
        assertThat(client.prepare(epoch, 10L).get() != null)
                .isEqualTo(true);

        assertThatThrownBy(() -> {
            client.prepare(epoch, 5L).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> {
            client.prepare(epoch, 2L).get();
        }).hasCauseInstanceOf(OutrankedException.class);
    }

    @Test
    public void proposeRejectsLowerRanks()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(9000);
        long epoch = layout.getEpoch();
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(epoch, 10L).get() != null)
                .isEqualTo(true);

        assertThatThrownBy(() -> {
            client.propose(epoch, 5L, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThat(client.propose(epoch, 10L, TestLayoutBuilder.single(9000)).get())
                .isEqualTo(true);
    }

    @Test
    public void proposeRejectsAlreadyProposed()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(9000);
        long epoch = layout.getEpoch();
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(epoch, 10L).get() != null)
                .isEqualTo(true);

        client.propose(epoch, 10L, layout).get();

        assertThatThrownBy(() -> {
            client.propose(epoch, 5L, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> {
            client.propose(epoch, 10L, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);
    }

    @Test
    public void proposeMalformedRejected()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(9000);
        long epoch = layout.getEpoch();
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(epoch, 10L).get() != null)
                .isEqualTo(true);

        long wrongAmount = 1L;    // Any non-zero amount is wrong.
        assertThatThrownBy(() -> {
            router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_PROPOSE.payloadMsg(new LayoutProposeRequest(epoch+wrongAmount, 10L, layout))).get();
        }).hasCauseInstanceOf(ServerRejectedException.class);
        assertThatThrownBy(() -> {
            router.sendMessageAndGetCompletable(CorfuMsgType.LAYOUT_COMMITTED.payloadMsg(new LayoutCommittedRequest(epoch+wrongAmount, layout))).get();
        }).hasCauseInstanceOf(ServerRejectedException.class);
    }

    @Test
    public void commitReturnsAck()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(9000);
        long epoch = layout.getEpoch();
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(epoch, 10L).get() != null)
                .isEqualTo(true);
        long newEpoch = 777;
        layout.setEpoch(newEpoch);

        assertThat(client.committed(newEpoch, layout).get())
                .isEqualTo(true);
    }

}
