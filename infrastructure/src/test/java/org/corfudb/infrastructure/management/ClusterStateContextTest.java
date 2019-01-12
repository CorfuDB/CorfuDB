package org.corfudb.infrastructure.management;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.Layout.ReplicationMode;
import org.corfudb.util.NodeLocator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class ClusterStateContextTest {

    @Test
    public void updateNodeState() {
        List<String> servers = Arrays.asList("a", "b", "c");
        List<LayoutSegment> segments = new ArrayList<>();

        List<LayoutStripe> stripes = new ArrayList<>();
        stripes.add(new LayoutStripe(servers));

        segments.add(new LayoutSegment(ReplicationMode.CHAIN_REPLICATION, 0, -1, stripes));

        Layout layout = new Layout(servers, Arrays.asList("a"), segments, 1L, UUID.randomUUID());

        ClusterStateContext ctx = new ClusterStateContext("server:9000", layout);

        HashMap<String, NodeState> nodeStatusMap = new HashMap<>();
        NodeState nodeState = NodeState.builder()
                .epoch(1)
                .endpoint(NodeLocator.builder().host("a").port(9000).build())
                .heartbeatCounter(100)
                .sequencerMetrics(SequencerMetrics.UNKNOWN)
                .connectivityStatus(ImmutableMap.of("a", true, "b", false))
                .build();
        nodeStatusMap.put("a", nodeState);

        ClusterState clusterState = ClusterState.builder()
                .nodes(nodeStatusMap)
                .node(ClusterState.ClusterStateNode.CONNECTED)
                .build();
        ctx.updateNodeState(clusterState, layout);

        String result = ctx.getClusterView().toString();
        System.out.println(result);

        assertEquals(
                result,
                "{a=NodeHeartbeat(nodeState=NodeState(endpoint=tcp://a:9000/, epoch=1, heartbeatCounter=100, sequencerMetrics=SequencerMetrics(sequencerStatus=UNKNOWN), connectivityStatus={a=true, b=false}), localHeartbeatTimestamp=HeartbeatTimestamp(epoch=1, counter=1), decayed=false)}"
        );

        System.out.println("\n\n\n" +  ctx.getClusterView());
    }
}