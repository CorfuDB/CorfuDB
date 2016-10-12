package org.corfudb.runtime.view;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/6/16.
 */
public class LayoutViewTest extends AbstractViewTest {
    @Test
    public void canGetLayout() {
        CorfuRuntime r = getDefaultRuntime().connect();
        Layout l = r.getLayoutView().getCurrentLayout();
        assertThat(l.asJSONString())
                .isNotNull();
    }

    @Test
    public void canSetLayout()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime().connect();
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
        l.setRuntime(r);
        r.getLayoutView().updateLayout(l, 1L);
        r.invalidateLayout();
        assertThat(r.getLayoutView().getLayout().epoch)
                .isEqualTo(1L);
    }

    @Test
    public void canTolerateLayoutServerFailure()
            throws Exception {
        addServer(9000);
        addServer(9001);

        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addSequencer(9000)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addToSegment()
                .addToLayout()
                .build());

        CorfuRuntime r = getRuntime().connect();

        // Fail the network link between the client and test server
        addServerRule(9001, new TestRule()
                .always()
                .drop());

        r.invalidateLayout();

        r.getStreamsView().get(CorfuRuntime.getStreamID("hi")).check();
    }


    /**
     * Fail two layout servers and reconfigure.
     * Details:
     * Start with a configuration of 5 servers 9000, 9001, 9002, 9003 and 9004.
     * Query the layout. Fail 9002 and 9004 and wait for management server to detect failures.
     *
     * @throws Exception
     */
    @Test
    public void layoutServerFailure()
            throws Exception {
        addServer(9000);
        addServer(9001);
        addServer(9002);
        addServer(9003);
        addServer(9004);
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addLayoutServer(9002)
                .addLayoutServer(9003)
                .addLayoutServer(9004)
                .addSequencer(9000)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addLogUnit(9001)
                .addLogUnit(9003)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();

        // All three layout servers 9000, 9001 and 9002 present
        assertThat(corfuRuntime.getLayoutView().getLayout().getLayoutServers().size()).isEqualTo(5);

        // Adding a rule on 9002 to drop all packets
        addServerRule(9002, new TestRule().always().drop());
        addServerRule(9004, new TestRule().always().drop());
        // Waiting for failure detectors to detect and correct the layout. At least 3 polls(1 second each)
        Thread.sleep(5000);

        corfuRuntime.invalidateLayout();
        assertThat(corfuRuntime.getLayoutView().getLayout().getLayoutServers().size()).isEqualTo(3);

    }
}