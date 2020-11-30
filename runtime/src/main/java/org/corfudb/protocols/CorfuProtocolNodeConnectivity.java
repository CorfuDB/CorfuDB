package org.corfudb.protocols;

import java.util.stream.Collectors;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.EnumBiMap;
import com.google.common.collect.ImmutableMap;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus;
import org.corfudb.runtime.proto.NodeConnectivity.ConnectivityEntryMsg;
import org.corfudb.runtime.proto.NodeConnectivity.NodeConnectivityMsg;
import org.corfudb.runtime.proto.NodeConnectivity.NodeConnectivityType;

/**
 * This class provides methods for creating the Protobuf objects
 * defined in node_connectivity.proto. Used by Management during a
 * QUERY_NODE operation.
 */
public final class CorfuProtocolNodeConnectivity {
    // Prevent class from being instantiated
    private CorfuProtocolNodeConnectivity() {}

    private static final EnumBiMap<NodeConnectivity.NodeConnectivityType, NodeConnectivityType> connectivityTypeMap =
            EnumBiMap.create(ImmutableMap.of(
                    NodeConnectivity.NodeConnectivityType.NOT_READY, NodeConnectivityType.NOT_READY,
                    NodeConnectivity.NodeConnectivityType.CONNECTED, NodeConnectivityType.CONNECTED,
                    NodeConnectivity.NodeConnectivityType.UNAVAILABLE, NodeConnectivityType.UNAVAILABLE
            ));

    private static final ImmutableBiMap<ConnectionStatus, Boolean> statusTypeMap =
            ImmutableBiMap.of(ConnectionStatus.OK, true, ConnectionStatus.FAILED, false);

    /**
     * Returns the Protobuf representation of a NodeConnectivity object.
     *
     * @param nc   the desired NodeConnectivity object
     * @return     an equivalent NodeConnectivity message
     */
    public static NodeConnectivityMsg getNodeConnectivityMsg(NodeConnectivity nc) {
        return NodeConnectivityMsg.newBuilder()
                .setEndpoint(nc.getEndpoint())
                .setEpoch(nc.getEpoch())
                .setConnectivityType(connectivityTypeMap.get(nc.getType()))
                .addAllConnectivityInfo(nc.getConnectivity()
                        .entrySet()
                        .stream()
                        .map(e -> ConnectivityEntryMsg.newBuilder()
                                .setNode(e.getKey())
                                .setStatusOk(statusTypeMap.get(e.getValue()))
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    /**
     * Returns a NodeConnectivity object from its Protobuf representation.
     *
     * @param msg   the desired Protobuf NodeConnectivity message
     * @return      an equivalent NodeConnectivity object
     */
    public static NodeConnectivity getNodeConnectivity(NodeConnectivityMsg msg) {
        ImmutableMap.Builder<String, ConnectionStatus> immutableMapBuilder = ImmutableMap.builder();

        msg.getConnectivityInfoList().forEach(entryMsg ->
                immutableMapBuilder.put(entryMsg.getNode(), statusTypeMap.inverse().get(entryMsg.getStatusOk())));

        return new NodeConnectivity(msg.getEndpoint(),
                connectivityTypeMap.inverse().getOrDefault(msg.getConnectivityType(),
                        NodeConnectivity.NodeConnectivityType.UNAVAILABLE),
                immutableMapBuilder.build(),
                msg.getEpoch());
    }
}
