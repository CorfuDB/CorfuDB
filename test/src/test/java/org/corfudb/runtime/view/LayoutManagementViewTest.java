package org.corfudb.runtime.view;

import static org.assertj.core.api.Assertions.assertThat;


import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout.ReplicationMode;
import org.junit.Test;

/**
 * Created by Maithem on 12/13/17.
 */
@Slf4j
public class LayoutManagementViewTest extends AbstractViewTest {

    /**
     * Verifies that remove node workflow works correctly and the primary sequencer
     * elected in the new layout is not any server in the unresponsive set.
     */
    @Test
    public void testRemoveNodeReassignsResponsiveSequencer() throws Exception {
        // Set up a 3 node cluster, where one server is marked unresponsive.
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .setReplicationMode(ReplicationMode.CHAIN_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.PORT_1)
                .build();
        bootstrapAllServers(l);

        // Shutdown management agent to prevent healing the unresponsive server.
        getManagementServer(SERVERS.PORT_0).getManagementAgent().shutdown();
        getManagementServer(SERVERS.PORT_1).getManagementAgent().shutdown();
        getManagementServer(SERVERS.PORT_2).getManagementAgent().shutdown();

        CorfuRuntime rt = getRuntime().connect();

        rt.invalidateLayout();
        Layout layout = new Layout(rt.getLayoutView().getLayout());

        // The expected layout should have primary sequencer that is not in
        // the unresponsive server set.
        Layout expectedLayout = new LayoutBuilder(layout)
                .removeLayoutServer(getEndpoint(SERVERS.PORT_2))
                .removeSequencerServer(getEndpoint(SERVERS.PORT_2))
                .removeLogunitServer(getEndpoint(SERVERS.PORT_2))
                .setEpoch(layout.getEpoch() + 1)
                .build();
        expectedLayout.setSequencers(Arrays.asList(SERVERS.ENDPOINT_0, SERVERS.ENDPOINT_1));

        // Remove the primary sequencer node.
        for (int x = 0; x < runtime.getParameters().getInvalidateRetry(); x++) {
            try {
                rt.getLayoutManagementView().removeNode(layout, getEndpoint(SERVERS.PORT_2));
                rt.invalidateLayout();
                break;
            } catch (WrongEpochException e) {
                // ignore and retry
            }
        }

        rt.invalidateLayout();
        Layout l2 = rt.getLayoutView().getLayout();

        // Verify that the node has been removed from the layout,
        // and the new layout should has primary sequencer that is not unresponsive.
        assertThat(l2).isEqualTo(expectedLayout);
    }
}
