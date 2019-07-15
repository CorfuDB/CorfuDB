package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableList;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;

public class ClusterStateTest {

    @Test
    public void isReady() {
        final String localEndpoint = "a";
        final long epoch1 = 1;
        final long epoch2 = 2;

        ClusterState invalidClusterState = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch1, OK),
                nodeState("b", epoch2, OK)
        );
        assertThat(invalidClusterState.isReady()).isFalse();

        ClusterState validClusterState = ClusterState.buildClusterState(
                localEndpoint,
                ImmutableList.of(),
                nodeState("a", epoch1, OK),
                nodeState("b", epoch1, OK)
        );
        assertThat(validClusterState.isReady()).isTrue();
    }
}
