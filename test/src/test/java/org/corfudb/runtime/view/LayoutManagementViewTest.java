package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout.ReplicationMode;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by Maithem on 12/13/17.
 */
@Slf4j
public class LayoutManagementViewTest extends AbstractViewTest{


    @Test
    public void removeNodeTest() throws Exception {
        // Set up a 3 node cluster, remove node 2 and then attempt
        // to remove the 3rd node from a cluster that only has two
        // nodes
        addServer(SERVERS.ENDPOINT_0);
        addServer(SERVERS.ENDPOINT_1);
        addServer(SERVERS.ENDPOINT_2);

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addSequencer(SERVERS.ENDPOINT_0)
                .addSequencer(SERVERS.ENDPOINT_1)
                .addSequencer(SERVERS.ENDPOINT_2)
                .buildSegment()
                .setReplicationMode(ReplicationMode.QUORUM_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addLogUnit(SERVERS.ENDPOINT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        CorfuRuntime r = getRuntime().connect();

        // Remove one node from a three node cluster
        r.invalidateLayout();
        Layout layout = new Layout(r.getLayoutView().getLayout());
        Layout expectedLayout = new LayoutBuilder(layout)
                .removeLayoutServer(SERVERS.ENDPOINT_2)
                .removeSequencerServer(SERVERS.ENDPOINT_2.toEndpointUrl())
                .removeLogunitServer(SERVERS.ENDPOINT_2.toEndpointUrl())
                .removeUnresponsiveServer(SERVERS.ENDPOINT_2.toEndpointUrl())
                .setEpoch(layout.getEpoch() + 1)
                .build();

        for (int x = 0; x < runtime.getParameters().getInvalidateRetry(); x++) {
            try {
                r.getLayoutManagementView().removeNode(layout, SERVERS.ENDPOINT_2.toEndpointUrl());
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
        assertThatThrownBy(() -> r.getLayoutManagementView().removeNode(l2, SERVERS.ENDPOINT_1.toEndpointUrl()))
                .isInstanceOf(LayoutModificationException.class);

        // Verify that the epoch hasn't changed
        assertThat(r.getLayoutView().getLayout().getEpoch()).isEqualTo(epoch);
    }
}