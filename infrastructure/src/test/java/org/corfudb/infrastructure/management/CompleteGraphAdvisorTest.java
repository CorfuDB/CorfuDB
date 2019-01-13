package org.corfudb.infrastructure.management;

import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;

import org.corfudb.infrastructure.management.ClusterGraph.NodeRank;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.runtime.view.Layout;
import org.junit.Test;

import java.util.Optional;

public class CompleteGraphAdvisorTest {

    @Test
    public void failedServer() {
        CompleteGraphAdvisor advisor = new CompleteGraphAdvisor();
        ClusterState clusterState = ClusterState.builder().build();

        Layout layout = mock(Layout.class);
        Optional<NodeRank> failedServer = advisor.failedServer(clusterState, layout, "a");

        fail("Check as much as possible cases for failure detector and all conditions in the method");
    }
}