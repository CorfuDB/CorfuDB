package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout.ReplicationMode;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by Maithem on 12/13/17.
 */
@Slf4j
public class LayoutManagementViewTest extends AbstractViewTest {

    @Test
    public void removeNodeTest() throws Exception {
        // Set up a 3 node cluster, remove node 2 and then attempt
        // to remove the 3rd node from a cluster that only has two
        // nodes
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .setReplicationMode(ReplicationMode.QUORUM_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        CorfuRuntime r = getRuntime().connect();

        // Remove one node from a three node cluster
        r.invalidateLayout();
        Layout layout = new Layout(r.getLayoutView().getLayout());
        Layout expectedLayout = new LayoutBuilder(layout)
                .removeLayoutServer(getEndpoint(SERVERS.PORT_2))
                .removeSequencerServer(getEndpoint(SERVERS.PORT_2))
                .removeLogunitServer(getEndpoint(SERVERS.PORT_2))
                .removeUnresponsiveServer(getEndpoint(SERVERS.PORT_2))
                .setEpoch(layout.getEpoch() + 1)
                .build();

        for (int x = 0; x < getRuntime().getParameters().getInvalidateRetry(); x++) {
            try {
                r.getLayoutManagementView().removeNode(layout, getEndpoint(SERVERS.PORT_2));
                r.invalidateLayout();
                break;
            } catch (WrongEpochException e) {
                // ignore and retry
            }
        }

        r.invalidateLayout();
        Layout l2 = r.getLayoutView().getLayout();
        // Verify that the node has been removed from the layout
        // and that other nodes aren't affected by the remove
        assertThat(l2).isEqualTo(expectedLayout);

        long epoch = r.getLayoutView().getLayout().getEpoch();

        // Attempt to remove a node from a two node cluster and verify that remove fails
        // due to an invalid modification (i.e. the remove results in a cluster that doesn't
        // meet the least number of nodes required to maintain redundancy).
        assertThatThrownBy(() -> r.getLayoutManagementView().removeNode(l2, getEndpoint(SERVERS.PORT_1)))
                .isInstanceOf(LayoutModificationException.class);

        // Verify that the epoch hasn't changed
        assertThat(r.getLayoutView().getLayout().getEpoch()).isEqualTo(epoch);
    }

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
        for (int x = 0; x < getRuntime().getParameters().getInvalidateRetry(); x++) {
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
