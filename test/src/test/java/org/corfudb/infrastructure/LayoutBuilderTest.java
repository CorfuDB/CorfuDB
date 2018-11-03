package org.corfudb.infrastructure;

import org.assertj.core.api.Assertions;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.ReplicationMode;
import org.corfudb.runtime.view.LayoutBuilder;
import org.corfudb.util.NodeLocator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by zlokhandwala on 10/26/16.
 */
public class LayoutBuilderTest extends AbstractCorfuTest {

    private static final int SERVER_ENTRY_3 = 3;
    private static final int SERVER_ENTRY_4 = 4;

    /**
     * Tests the Layout Builder by removing nodes.
     *
     * @throws LayoutModificationException Throws error if all layout, sequencer or
     *                                     logunit nodes removed. Invalid removal.
     */
    @Test
    public void checkRemovalOfNodes() throws LayoutModificationException {
        Layout originalLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addLayoutServer(SERVERS.ENDPOINT_3)
                .addLayoutServer(SERVERS.ENDPOINT_4)
                .addSequencer(SERVERS.ENDPOINT_0)
                .addSequencer(SERVERS.ENDPOINT_1)
                .addSequencer(SERVERS.ENDPOINT_2)
                .buildSegment()
                .setReplicationMode(ReplicationMode.QUORUM_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addLogUnit(SERVERS.ENDPOINT_2)
                .addLogUnit(SERVERS.ENDPOINT_3)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addLogUnit(SERVERS.ENDPOINT_3)
                .addLogUnit(SERVERS.ENDPOINT_4)
                .addToSegment()
                .addToLayout()
                .build();

        List<NodeLocator> allNodes = Arrays.asList(
                SERVERS.ENDPOINT_0, SERVERS.ENDPOINT_1, SERVERS.ENDPOINT_2, SERVERS.ENDPOINT_3, SERVERS.ENDPOINT_4
        );

        LayoutBuilder layoutBuilder = new LayoutBuilder(originalLayout);

        //Preparing failed nodes set.
        Set<NodeLocator> failedNodes = new HashSet<>(allNodes);

        /*
         * Invalid Removals
         */

        // Deleting all Layout Servers
        assertThatThrownBy(() -> layoutBuilder.removeLayoutServers(toStringNodes(failedNodes)).build())
                .isInstanceOf(LayoutModificationException.class);

        //Deleting all Sequencer Servers
        assertThatThrownBy(() -> layoutBuilder.removeSequencerServers(toStringNodes(failedNodes)).build())
                .isInstanceOf(LayoutModificationException.class);

        //Deleting all Log unit Servers
        assertThatThrownBy(() -> layoutBuilder.removeLogunitServers(toStringNodes(failedNodes)).build())
                .isInstanceOf(LayoutModificationException.class);

        // Deleting all nodes in one stripe
        failedNodes.clear();
        failedNodes.add(allNodes.get(0));
        failedNodes.add(allNodes.get(2));
        assertThatThrownBy(() -> layoutBuilder.removeLogunitServers(toStringNodes(failedNodes)).build())
                .isInstanceOf(LayoutModificationException.class);


        /*
         *  Valid Removal
         */

        // Deleting SERVERS.ENDPOINT_0 and SERVERS.ENDPOINT_4
        // Preparing new layout
        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addLayoutServer(SERVERS.ENDPOINT_3)
                .addSequencer(SERVERS.ENDPOINT_1)
                .addSequencer(SERVERS.ENDPOINT_2)
                .buildSegment()
                .setReplicationMode(ReplicationMode.QUORUM_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_2)
                .addLogUnit(SERVERS.ENDPOINT_3)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addLogUnit(SERVERS.ENDPOINT_3)
                .addToSegment()
                .addToLayout()
                .setClusterId(originalLayout.getClusterId())
                .build();

        failedNodes.clear();
        failedNodes.add(allNodes.get(0));
        failedNodes.add(allNodes.get(SERVER_ENTRY_4));

        Layout actualLayout = layoutBuilder
                .removeLayoutServers(toStringNodes(failedNodes))
                .removeLogunitServers(toStringNodes(failedNodes))
                .removeSequencerServers(toStringNodes(failedNodes))
                .build();
        Assertions.assertThat(actualLayout).isEqualTo(expectedLayout);

        expectedLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_3)
                .addSequencer(SERVERS.ENDPOINT_2)
                .buildSegment()
                .setReplicationMode(ReplicationMode.QUORUM_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_2)
                .addLogUnit(SERVERS.ENDPOINT_3)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addLogUnit(SERVERS.ENDPOINT_3)
                .addToSegment()
                .addToLayout()
                .build();

        /*
         * Deleting individual nodes
         */

        // Remove SERVERS.ENDPOINT_1 & SERVERS.ENDPOINT_2 from layout server
        layoutBuilder.removeLayoutServer(allNodes.get(1));
        layoutBuilder.removeLayoutServer(allNodes.get(2));

        // No effect on removing removed node
        layoutBuilder.removeLayoutServer(allNodes.get(2));

        // Remove SERVERS.ENDPOINT_3 from layout server should throw error.
        assertThatThrownBy(() -> layoutBuilder.removeLayoutServer(allNodes.get(SERVER_ENTRY_3)))
                .isInstanceOf(LayoutModificationException.class);

        // Remove SERVERS.ENDPOINT_1 from sequencers
        layoutBuilder.removeSequencerServer(allNodes.get(1).toEndpointUrl());

        // No effect on removing removed node
        layoutBuilder.removeSequencerServer(allNodes.get(1).toEndpointUrl());

        // Remove SERVERS.ENDPOINT_2 from sequencer server should throw error.
        assertThatThrownBy(() -> layoutBuilder.removeSequencerServer(allNodes.get(2).toEndpointUrl()))
                .isInstanceOf(LayoutModificationException.class);

        // Reject remove if redundancy is lost
        assertThatThrownBy(() -> layoutBuilder.removeLogunitServer(allNodes.get(1).toEndpointUrl()))
                .isInstanceOf(LayoutModificationException.class);

        // Reject remove if stripe size is one
        assertThatThrownBy(() -> layoutBuilder.removeLogunitServer(allNodes.get(2).toEndpointUrl()))
                .isInstanceOf(LayoutModificationException.class);

        // Assert the resulting layout is equal to the expected
        assertThat(layoutBuilder.build()).isEqualTo(expectedLayout);
    }

    private Set<String> toStringNodes(Set<NodeLocator> nodes) {
        return NodeLocator.transformToStringsSet(nodes);
    }

    @Test
    public void checkingForNullArgumentsInLayoutBuilder() throws Exception {
        Layout originalLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addLayoutServer(SERVERS.ENDPOINT_3)
                .addLayoutServer(SERVERS.ENDPOINT_4)
                .addSequencer(SERVERS.ENDPOINT_0)
                .addSequencer(SERVERS.ENDPOINT_1)
                .addSequencer(SERVERS.ENDPOINT_2)
                .buildSegment()
                .setReplicationMode(ReplicationMode.QUORUM_REPLICATION)
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addLogUnit(SERVERS.ENDPOINT_2)
                .addLogUnit(SERVERS.ENDPOINT_3)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addLogUnit(SERVERS.ENDPOINT_3)
                .addLogUnit(SERVERS.ENDPOINT_4)
                .addToSegment()
                .addToLayout()
                .build();

        // Test base layout can not be null
        assertThatThrownBy(() -> new LayoutBuilder(null))
                .isInstanceOf(NullPointerException.class);

        // Create a layout builder based on the original layout
        LayoutBuilder layoutBuilder = new LayoutBuilder(originalLayout);

        assertThatThrownBy(() -> layoutBuilder.addLogunitServerToSegment(null, 0, 0))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.addUnresponsiveServers(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.assignResponsiveSequencerAsPrimary(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.removeFromStripe(null,
                                                                null,
                                                                0))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.removeLayoutServer(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.removeLayoutServers(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.removeLogunitServer(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.removeLogunitServers(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.removeSequencerServer(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.removeSequencerServers(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.removeUnresponsiveServer(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> layoutBuilder.removeUnresponsiveServers(null))
                .isInstanceOf(NullPointerException.class);
    }

    /**
     * Tests the modification of the layout by the addition of new nodes.
     *
     * @throws Exception
     */
    @Test
    public void checkAdditionOfNodes() throws Exception {

        Layout originalLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addSequencer(SERVERS.ENDPOINT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addToSegment()
                .addToLayout()
                .build();

        LayoutBuilder layoutBuilder = new LayoutBuilder(originalLayout);

        final long globalLogTail = 50L;
        layoutBuilder.addLayoutServer(SERVERS.ENDPOINT_1);
        layoutBuilder.addSequencerServer(SERVERS.ENDPOINT_2);
        layoutBuilder.addLogunitServer(1, globalLogTail, SERVERS.ENDPOINT_3);

        // Expected layout
        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addSequencer(SERVERS.ENDPOINT_0)
                .addSequencer(SERVERS.ENDPOINT_2)
                .buildSegment()
                .setEnd(globalLogTail + 1)
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addToSegment()
                .addToLayout()
                .buildSegment()
                .setStart(globalLogTail + 1)
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_0)
                .addToSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_1)
                .addLogUnit(SERVERS.ENDPOINT_3)
                .addToSegment()
                .addToLayout()
                .build();

        assertThat(layoutBuilder.build()).isEqualTo(expectedLayout);
    }

    /**
     * Reassign a failover sequencer server from the set of responsive nodes.
     * It asserts that assigned sequencer server was not picked from
     * unresponsive servers
     *
     * @throws Exception
     */
    @Test
    public void reassignResponsiveSequencer() throws Exception {

        Layout originalLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addSequencer(SERVERS.ENDPOINT_0)
                .addSequencer(SERVERS.ENDPOINT_1)
                .addSequencer(SERVERS.ENDPOINT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_2)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.ENDPOINT_1)
                .build();

        LayoutBuilder layoutBuilder = new LayoutBuilder(originalLayout)
                .assignResponsiveSequencerAsPrimary(Collections.singleton(SERVERS.ENDPOINT_0.toEndpointUrl()))
                .addUnresponsiveServers(Collections.singleton(SERVERS.ENDPOINT_0.toEndpointUrl()));

        // Since nodes 0 and 1 are both unresponsive, ENDPOINT_2 should be chosen as the primary
        // sequencer server.
        // Expected layout
        Layout expectedLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addSequencer(SERVERS.ENDPOINT_2)
                .addSequencer(SERVERS.ENDPOINT_0)
                .addSequencer(SERVERS.ENDPOINT_1)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_2)
                .addToSegment()
                .addToLayout()
                .addUnresponsiveServer(SERVERS.ENDPOINT_1)
                .addUnresponsiveServer(SERVERS.ENDPOINT_0)
                .build();

        assertThat(layoutBuilder.build()).isEqualTo(expectedLayout);
    }

    /**
     * This test attempts to failover the sequencer server while all layout sequencers
     * are unresponsive. It verifies that {@link LayoutModificationException} is thrown
     * in case of such unsuccessful failover.
     *
     * @throws Exception
     */
    @Test (expected = LayoutModificationException.class)
    public void unsuccessfulSequencerFailoverThrowsException() throws Exception {
        Layout originalLayout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.ENDPOINT_0)
                .addLayoutServer(SERVERS.ENDPOINT_1)
                .addLayoutServer(SERVERS.ENDPOINT_2)
                .addSequencer(SERVERS.ENDPOINT_0)
                .addSequencer(SERVERS.ENDPOINT_1)
                .addSequencer(SERVERS.ENDPOINT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.ENDPOINT_2)
                .addToSegment()
                .addToLayout()
                .build();

        LayoutBuilder layoutBuilder = new LayoutBuilder(originalLayout)
                .addUnresponsiveServers(Collections.singleton(SERVERS.ENDPOINT_0.toEndpointUrl()))
                .addUnresponsiveServers(Collections.singleton(SERVERS.ENDPOINT_1.toEndpointUrl()))
                .addUnresponsiveServers(Collections.singleton(SERVERS.ENDPOINT_2.toEndpointUrl()))
                .assignResponsiveSequencerAsPrimary(Collections.singleton(SERVERS.ENDPOINT_0.toEndpointUrl()));
        layoutBuilder.build();
    }
}
