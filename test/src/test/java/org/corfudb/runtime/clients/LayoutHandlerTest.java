package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutManagementView;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 12/21/15.
 */
public class LayoutHandlerTest extends AbstractClientTest {

    LayoutClient client;

    @Override
    Set<AbstractServer> getServersForTest() {
        ServerContext sc = defaultServerContext();
        serverRouter.setServerContext(sc);
        return new ImmutableSet.Builder<AbstractServer>()
                .add(new LayoutServer(sc))
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        LayoutHandler layoutHandler = new LayoutHandler();
        client = new LayoutClient(router, 0L, UUID.fromString("00000000-0000-0000-0000-000000000000"));
        return new ImmutableSet.Builder<IClient>()
                .add(new BaseHandler())
                .add(layoutHandler)
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
        Layout l = TestLayoutBuilder.single(SERVERS.PORT_0);
        assertThat(client.bootstrapLayout(l).get())
                .isEqualTo(true);

        assertThat(client.getLayout().get().asJSONString())
                .isEqualTo(l.asJSONString());
    }

    @Test
    public void cannotBootstrapServerTwice()
            throws Exception {
        assertThat(client.bootstrapLayout(TestLayoutBuilder.single(SERVERS.PORT_0)).get())
                .isEqualTo(true);
        assertThatThrownBy(() -> client.bootstrapLayout(TestLayoutBuilder.single(SERVERS.PORT_0)).get())
                .hasCauseInstanceOf(AlreadyBootstrappedException.class);
    }

    @Test
    public void canGetNewLayoutInDifferentEpoch()
            throws Exception {
        Layout l = TestLayoutBuilder.single(SERVERS.PORT_0);
        final long NEW_EPOCH = 42L;
        l.setEpoch(NEW_EPOCH);
        assertThat(client.bootstrapLayout(l).get())
                .isEqualTo(true);

        assertThat(client.getLayout().get().getEpoch())
                .isEqualTo(NEW_EPOCH);
    }

    final long RANK_LOW = 5L;
    final long RANK_HIGH = 10L;
    @Test
    public void prepareRejectsLowerRanks()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);
        long epoch = layout.getEpoch();
        assertThat(client.prepare(epoch, RANK_HIGH).get() != null)
                .isEqualTo(true);

        assertThatThrownBy(() -> {
            client.prepare(epoch, RANK_LOW).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> {
            client.prepare(epoch, 2L).get();
        }).hasCauseInstanceOf(OutrankedException.class);
    }

    /**
     * Ensure that the {@link LayoutManagementView#nextRank(long)}
     * is monotonically increasing but not strictly monotonically
     * increasing.
     */
    @Test
    public void testNextRankMonotonicity() {
        long currentRank = 0L;
        long nextRank = LayoutManagementView.nextRank(currentRank);
        while (nextRank == currentRank + 1) {
            currentRank = nextRank;
            nextRank = LayoutManagementView.nextRank(currentRank);
        }

        Assertions.assertThat(nextRank).isGreaterThan(currentRank);
    }

    @Test
    public void proposeRejectsLowerRanks()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(epoch, RANK_HIGH).get() != null)
                .isEqualTo(true);

        assertThatThrownBy(() -> {
            client.propose(epoch, RANK_LOW, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThat(client.propose(epoch, RANK_HIGH, TestLayoutBuilder.single(SERVERS.PORT_0)).get())
                .isEqualTo(true);
    }

    @Test
    public void proposeRejectsAlreadyProposed()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(epoch, RANK_HIGH).get() != null)
                .isEqualTo(true);

        client.propose(epoch, RANK_HIGH, layout).get();

        assertThatThrownBy(() -> {
            client.propose(epoch, RANK_LOW, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);

        assertThatThrownBy(() -> {
            client.propose(epoch, RANK_HIGH, layout).get();
        }).hasCauseInstanceOf(OutrankedException.class);
    }

    @Test
    public void commitReturnsAck()
            throws Exception {
        Layout layout = TestLayoutBuilder.single(SERVERS.PORT_0);
        long epoch = layout.getEpoch();
        assertThat(client.bootstrapLayout(layout).get())
                .isEqualTo(true);

        assertThat(client.prepare(epoch, RANK_HIGH).get() != null)
                .isEqualTo(true);
        final long TEST_EPOCH = 777;
        layout.setEpoch(TEST_EPOCH);

        assertThat(client.committed(TEST_EPOCH, layout).get())
                .isEqualTo(true);
    }

}
