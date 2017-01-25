package org.corfudb.infrastructure;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.format.Types.NodeMetrics;
import org.corfudb.protocols.wireprotocol.ITypedEnum;

import java.util.EnumMap;

/**
 * Node Health Model:
 * Contains a map of metrics of the state of a node.
 * This state is utilized by the management server to
 * make decisions on the state of the cluster and handle
 * failures if needed.
 * <p>
 * Created by zlokhandwala on 1/13/17.
 */
public class NodeHealthModel {

    public EnumMap<NodeHealthDataType, Object> nodeHealthDataTypeMap;

    public NodeHealthModel() {
        nodeHealthDataTypeMap = new EnumMap<>(NodeHealthModel.NodeHealthDataType.class);
    }

    public String getEndpoint() {
        return (String) nodeHealthDataTypeMap.get(NodeHealthDataType.ENDPOINT);
    }

    public void setEndpoint(String endpoint) {
        nodeHealthDataTypeMap.put(NodeHealthDataType.ENDPOINT, endpoint);
    }

    public NodeMetrics getNodeMetrics() {
        return (NodeMetrics) nodeHealthDataTypeMap.get(NodeHealthDataType.NODE_METRICS);
    }

    public void setNodeMetrics(NodeMetrics nodeMetrics) {
        nodeHealthDataTypeMap.put(NodeHealthDataType.NODE_METRICS, nodeMetrics);
    }

    public Long getNetworkLatency() {
        return (Long) nodeHealthDataTypeMap.get(NodeHealthDataType.NETWORK_LATENCY);
    }

    public void setNetworkLatency(Long networkLatency) {
        nodeHealthDataTypeMap.put(NodeHealthDataType.NETWORK_LATENCY, networkLatency);
    }

    public Double getPollSuccessRate() {
        return (Double) nodeHealthDataTypeMap.get(NodeHealthDataType.POLL_SUCCESS_RATE);
    }

    public void setPollSuccessRate(Double pollSuccessRate) {
        nodeHealthDataTypeMap.put(NodeHealthDataType.POLL_SUCCESS_RATE, pollSuccessRate);
    }

    @RequiredArgsConstructor
    public enum NodeHealthDataType implements ITypedEnum {
        ENDPOINT(0, TypeToken.of(String.class)),
        NODE_METRICS(1, TypeToken.of(NodeMetrics.class)),
        NETWORK_LATENCY(2, TypeToken.of(Long.class)),
        POLL_SUCCESS_RATE(3, TypeToken.of(Double.class));
        final int type;
        @Getter
        final TypeToken<?> componentType;

        public byte asByte() {
            return (byte) type;
        }
    }
}
