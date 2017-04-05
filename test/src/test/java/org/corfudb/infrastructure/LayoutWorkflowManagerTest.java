package org.corfudb.infrastructure;

import org.assertj.core.api.Assertions;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by zlokhandwala on 10/26/16.
 */
public class LayoutWorkflowManagerTest extends AbstractCorfuTest {

    final int SERVER_ENTRY_3 = 3;
    final int SERVER_ENTRY_4 = 4;

    /**
     * Tests the Layout Workflow manager by removing nodes.
     *
     * @throws LayoutModificationException Throws error if all layout, sequencer or
     *                                     logunit nodes removed. Invalid removal.
     */
    @Test
    public void checkRemovalOfNodes() throws LayoutModificationException, CloneNotSupportedException {
        Layout originalLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addLayoutServer(SERVERS.PORT_3)
                .addLayoutServer(SERVERS.PORT_4)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_3)
                .addLogUnit(SERVERS.PORT_4)
                .addToSegment()
                .addToLayout()
                .build();

        List<String> allNodes = new ArrayList<>();
        allNodes.add(TestLayoutBuilder.getEndpoint(SERVERS.PORT_0));
        allNodes.add(TestLayoutBuilder.getEndpoint(SERVERS.PORT_1));
        allNodes.add(TestLayoutBuilder.getEndpoint(SERVERS.PORT_2));
        allNodes.add(TestLayoutBuilder.getEndpoint(SERVERS.PORT_3));
        allNodes.add(TestLayoutBuilder.getEndpoint(SERVERS.PORT_4));

        Set<String> failedNodes = new HashSet<String>();
        LayoutWorkflowManager layoutWorkflowManager = new LayoutWorkflowManager(originalLayout);

        //Preparing failed nodes set.
        failedNodes.addAll(allNodes);


        /*
         * Invalid Removals
         */

        // Deleting all Layout Servers
        assertThatThrownBy(() ->
                layoutWorkflowManager.removeLayoutServers(failedNodes).build()
        ).isInstanceOf(LayoutModificationException.class);

        //Deleting all Sequencer Servers
        assertThatThrownBy(() ->
                layoutWorkflowManager.removeSequencerServers(failedNodes).build()
        ).isInstanceOf(LayoutModificationException.class);

        //Deleting all Log unit Servers
        assertThatThrownBy(() ->
                layoutWorkflowManager.removeLogunitServers(failedNodes).build()
        ).isInstanceOf(LayoutModificationException.class);

        // Deleting all nodes in one stripe
        failedNodes.clear();
        failedNodes.add(allNodes.get(0));
        failedNodes.add(allNodes.get(2));
        assertThatThrownBy(() ->
                layoutWorkflowManager.removeLogunitServers(failedNodes).build()
        ).isInstanceOf(LayoutModificationException.class);


        /*
         *  Valid Removal
         */

        // Deleting SERVERS.PORT_0 and SERVERS.PORT_4
        // Preparing new layout
        Layout expectedLayout = new TestLayoutBuilder()
                .setClusterId(originalLayout.getClusterId())
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addLayoutServer(SERVERS.PORT_3)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_3)
                .addToSegment()
                .addToLayout()
                .build();

        failedNodes.clear();
        failedNodes.add(allNodes.get(0));
        failedNodes.add(allNodes.get(SERVER_ENTRY_4));

        Layout actualLayout = layoutWorkflowManager.removeLayoutServers(failedNodes)
                .removeLogunitServers(failedNodes)
                .removeSequencerServers(failedNodes)
                .build();
        Assertions.assertThat(actualLayout).isEqualTo(expectedLayout);


        /*
         * Deleting individual nodes
         */

        expectedLayout = new TestLayoutBuilder()
                .setClusterId(originalLayout.getClusterId())
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_3)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_3)
                .addToSegment()
                .addToLayout()
                .build();

        // Remove SERVERS.PORT_1 & SERVERS.PORT_2 from layout server
        layoutWorkflowManager.removeLayoutServer(allNodes.get(1));
        layoutWorkflowManager.removeLayoutServer(allNodes.get(2));
        // No effect on removing removed node
        layoutWorkflowManager.removeLayoutServer(allNodes.get(2));
        // Remove SERVERS.PORT_3 from layout server should throw error.
        assertThatThrownBy(() ->
                layoutWorkflowManager.removeLayoutServer(allNodes.get(SERVER_ENTRY_3))
        ).isInstanceOf(LayoutModificationException.class);
        // Remove SERVERS.PORT_1 from sequencers
        layoutWorkflowManager.removeSequencerServer(allNodes.get(1));
        // No effect on removing removed node
        layoutWorkflowManager.removeSequencerServer(allNodes.get(1));
        // Remove SERVERS.PORT_2 from sequencer server should throw error.
        assertThatThrownBy(() ->
                layoutWorkflowManager.removeSequencerServer(allNodes.get(2))
        ).isInstanceOf(LayoutModificationException.class);
        // Remove SERVERS.PORT_1 from logunits
        layoutWorkflowManager.removeLogunitServer(allNodes.get(1));
        // No effect on removing removed node
        layoutWorkflowManager.removeLogunitServer(allNodes.get(1));
        // Remove SERVERS.PORT_2 from logunit server should throw error.
        assertThatThrownBy(() ->
                layoutWorkflowManager.removeLogunitServer(allNodes.get(2))
        ).isInstanceOf(LayoutModificationException.class);
        assertThat(layoutWorkflowManager.build()).isEqualTo(expectedLayout);
    }
}
