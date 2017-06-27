package org.corfudb.infrastructure;

import lombok.Data;

import org.corfudb.format.Types.NodeMetrics;


/**
 * NodeHealth:
 * Contains the health status of the state of a node.
 * This state is utilized by the management server to
 * make decisions on the state of the cluster and handle
 * failures if needed.
 *
 * <p>Created by zlokhandwala on 1/13/17.
 */
@Data
public class NodeHealth {

    private final String endpoint;
    private final NodeMetrics nodeMetrics;
    private final Long networkLatency;
    private final Double pollSuccessRate;

    private NodeHealth(NodeHealthBuilder nodeHealthBuilder) {
        this.endpoint = nodeHealthBuilder.endpoint;
        this.nodeMetrics = nodeHealthBuilder.nodeMetrics;
        this.networkLatency = nodeHealthBuilder.networkLatency;
        this.pollSuccessRate = nodeHealthBuilder.pollSuccessRate;
    }

    /**
     * Builder for the NodeHealth.
     */
    public static class NodeHealthBuilder {

        private String endpoint;
        private NodeMetrics nodeMetrics;
        private Long networkLatency;
        private Double pollSuccessRate;

        public NodeHealthBuilder() {
        }

        public NodeHealthBuilder setEndpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public NodeHealthBuilder setNodeMetrics(NodeMetrics nodeMetrics) {
            this.nodeMetrics = nodeMetrics;
            return this;
        }

        public NodeHealthBuilder setNetworkLatency(Long networkLatency) {
            this.networkLatency = networkLatency;
            return this;
        }

        public NodeHealthBuilder setPollSuccessRate(Double pollSuccessRate) {
            this.pollSuccessRate = pollSuccessRate;
            return this;
        }

        public NodeHealth build() {
            return new NodeHealth(this);
        }
    }
}
