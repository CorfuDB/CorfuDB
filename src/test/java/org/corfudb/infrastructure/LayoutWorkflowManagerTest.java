package org.corfudb.infrastructure;

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
public class LayoutWorkflowManagerTest {

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
                .addLayoutServer(9000)
                .addLayoutServer(9001)
                .addLayoutServer(9002)
                .addLayoutServer(9003)
                .addLayoutServer(9004)
                .addSequencer(9000)
                .addSequencer(9001)
                .addSequencer(9002)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9000)
                .addLogUnit(9002)
                .addToSegment()
                .buildStripe()
                .addLogUnit(9001)
                .addLogUnit(9003)
                .addLogUnit(9004)
                .addToSegment()
                .addToLayout()
                .build();

        List<String> allNodes = new ArrayList<>();
        allNodes.add(TestLayoutBuilder.getEndpoint(9000));
        allNodes.add(TestLayoutBuilder.getEndpoint(9001));
        allNodes.add(TestLayoutBuilder.getEndpoint(9002));
        allNodes.add(TestLayoutBuilder.getEndpoint(9003));
        allNodes.add(TestLayoutBuilder.getEndpoint(9004));

        Set<String> failedNodes = new HashSet<String>();
        LayoutWorkflowManager layoutWorkflowManager = new LayoutWorkflowManager(originalLayout);

        //Preparing failed nodes set.
        failedNodes.add(allNodes.get(0));
        failedNodes.add(allNodes.get(1));
        failedNodes.add(allNodes.get(2));
        failedNodes.add(allNodes.get(3));
        failedNodes.add(allNodes.get(4));


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

        // Deleting 9000 and 9004
        // Preparing new layout
        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9001)
                .addLayoutServer(9002)
                .addLayoutServer(9003)
                .addSequencer(9001)
                .addSequencer(9002)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9002)
                .addToSegment()
                .buildStripe()
                .addLogUnit(9001)
                .addLogUnit(9003)
                .addToSegment()
                .addToLayout()
                .build();

        failedNodes.clear();
        failedNodes.add(allNodes.get(0));
        failedNodes.add(allNodes.get(4));

        Layout actualLayout = layoutWorkflowManager.removeLayoutServers(failedNodes)
                .removeLogunitServers(failedNodes)
                .removeSequencerServers(failedNodes)
                .build();
        assertThat(actualLayout).isEqualTo(expectedLayout);


        /*
         * Deleting individual nodes
         */

        expectedLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(9003)
                .addSequencer(9002)
                .buildSegment()
                .buildStripe()
                .addLogUnit(9002)
                .addToSegment()
                .buildStripe()
                .addLogUnit(9003)
                .addToSegment()
                .addToLayout()
                .build();
        // Remove 9001 & 9002 from layout server
        layoutWorkflowManager.removeLayoutServer(allNodes.get(1));
        layoutWorkflowManager.removeLayoutServer(allNodes.get(2));
        // No effect on removing removed node
        layoutWorkflowManager.removeLayoutServer(allNodes.get(2));
        // Remove 9003 from layout server should throw error.
        assertThatThrownBy(() ->
                layoutWorkflowManager.removeLayoutServer(allNodes.get(3))
        ).isInstanceOf(LayoutModificationException.class);
        // Remove 9001 from sequencers
        layoutWorkflowManager.removeSequencerServer(allNodes.get(1));
        // No effect on removing removed node
        layoutWorkflowManager.removeSequencerServer(allNodes.get(1));
        // Remove 9002 from sequencer server should throw error.
        assertThatThrownBy(() ->
                layoutWorkflowManager.removeSequencerServer(allNodes.get(2))
        ).isInstanceOf(LayoutModificationException.class);
        // Remove 9001 from logunits
        layoutWorkflowManager.removeLogunitServer(allNodes.get(1));
        // No effect on removing removed node
        layoutWorkflowManager.removeLogunitServer(allNodes.get(1));
        // Remove 9002 from logunit server should throw error.
        assertThatThrownBy(() ->
                layoutWorkflowManager.removeLogunitServer(allNodes.get(2))
        ).isInstanceOf(LayoutModificationException.class);
        assertThat(layoutWorkflowManager.build()).isEqualTo(expectedLayout);
    }
}
