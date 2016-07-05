package org.corfudb.runtime.view;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.TestClientRouter;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/6/16.
 */
public class LayoutViewTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    public void canGetLayout() {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap(),
                getServerRouterForEndpoint(getDefaultEndpoint())));
        wireRouters();

        CorfuRuntime r = getRuntime().connect();
        Layout l = r.getLayoutView().getCurrentLayout();
        assertThat(l.asJSONString())
                .isNotNull();
    }

    @Test
    public void canSetLayout()
            throws Exception {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap(),
                getServerRouterForEndpoint(getDefaultEndpoint())));
        wireRouters();

        CorfuRuntime r = getRuntime().connect();
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addSequencer(9000)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addToSegment()
                .addToLayout()
                .build();

        r.getLayoutView().updateLayout(l, 1L);
        r.invalidateLayout();
        assertThat(r.getLayoutView().getLayout().epoch)
                .isEqualTo(1L);
    }

    @Test
    public void canTolerateLayoutServerFailure()
            throws Exception {
        // No Bootstrap Option Map
        Map<String, Object> noBootstrap = new ImmutableMap.Builder<String, Object>()
                .put("--initial-token", "0")
                .put("--memory", true)
                .put("--single", false)
                .put("--max-cache", "256M")
                .put("--sync", false)
                .build();

        // Server @ 9000 : Layout, Sequencer, LogUnit
        addServerForTest(getDefaultEndpoint(), new LayoutServer(noBootstrap,
                getServerRouterForEndpoint(getDefaultEndpoint())));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));

        // Server @ 9001 : Layout
        LayoutServer failingServer = new LayoutServer(noBootstrap,
                getServerRouterForEndpoint(getEndpoint(9001)));

        addServerForTest(getEndpoint(9001), failingServer);
        wireRouters();

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addSequencer(9000)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addToSegment()
                .addToLayout()
                .build();

        // Bootstrap with this layout.
        getTestRouterForEndpoint(getEndpoint(9000L)).getClient(LayoutClient.class)
                .bootstrapLayout(l);
        getTestRouterForEndpoint(getEndpoint(9001L)).getClient(LayoutClient.class)
                .bootstrapLayout(l);

        CorfuRuntime r = getRuntime().connect();

        // Fail the network link between the client and test server
        TestClientRouter tcr = getTestRouterForEndpoint(getEndpoint(9001));
        tcr.setDropAllMessagesClientToServer(true);

        r.invalidateLayout();

        r.getStreamsView().get(CorfuRuntime.getStreamID("hi")).check();
    }
}
