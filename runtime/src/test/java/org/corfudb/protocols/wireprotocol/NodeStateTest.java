package org.corfudb.protocols.wireprotocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.wireprotocol.NodeState.HeartbeatTimestamp;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;
import org.junit.Test;

public class NodeStateTest {

    @Test
    public void testSerializeDeserialize() {
        NodeConnectivity co = NodeConnectivity.builder()
                .type(NodeConnectivityType.CONNECTED)
                .endpoint("localhost:9000")
                .connectivity(ImmutableMap.of())
                .build();

        NodeState nodeState = NodeState.builder()
                .sequencerMetrics(SequencerMetrics.UNKNOWN)
                .heartbeat(new HeartbeatTimestamp(0, 0))
                .connectivity(co)
                .build();

        ByteBuf buf = Unpooled.buffer();
        CorfuPayloadMsg<NodeState> msg = CorfuMsgType.NODE_STATE_RESPONSE.payloadMsg(nodeState);
        msg.serialize(buf);

        CorfuMsg deserializedMsg = CorfuMsg.deserialize(buf);
        assertEquals(CorfuMsgType.NODE_STATE_RESPONSE, deserializedMsg.msgType);
        assertNotNull(deserializedMsg.getBuf());
    }


}