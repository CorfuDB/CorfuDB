package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.exceptions.WrongEpochException;
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
        try {
            r.getLayoutManagementView().removeNode(l, getEndpoint(SERVERS.PORT_2));
        } catch (WrongEpochException e) {
            // Ignore wrong epoch exceptions
        }
        r.invalidateLayout();
        Layout l2 = r.getLayoutView().getLayout();
        assertThat(l2.getAllServers()).doesNotContain(getEndpoint(SERVERS.PORT_2));

        long epoch = r.getLayoutView().getLayout().getEpoch();

        // Attempt to remove a node from a two node cluster and verify that remove fails
        // due to an invalid modification (i.e. the remove results in a cluster that doesn't
        // meet the least number of nodes required to maintain redundancy).
        assertThatThrownBy(() -> {
                    try {
                        r.getLayoutManagementView().removeNode(l2,
                                getEndpoint(SERVERS.PORT_1));
                    } catch (WrongEpochException e) {
                        // ignore wronge epoch exceptions
                    }
                }
        ).isInstanceOf(LayoutModificationException.class);

        // Verify that the epoch hasn't changed
        assertThat(r.getLayoutView().getLayout().getEpoch()).isEqualTo(epoch);
    }
}
