package org.corfudb.protocols.wireprotocol.failuredetector;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;

public class NodeConnectivityTest {

    @Test
    public void testConnectedAndFailedNodes() {
        final long epoch = 1;
        NodeConnectivity nodeState = NodeConnectivity.connectivity(
                "a",
                epoch,
                ImmutableMap.of("a", OK, "b", OK, "c", FAILED)
        );

        assertThat(nodeState.getConnectedNodes()).isEqualTo(ImmutableSet.of("a", "b"));
        assertThat(nodeState.getFailedNodes()).isEqualTo(ImmutableSet.of("c"));
    }
}
