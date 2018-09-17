package org.corfudb.universe.service;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DockerClient;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.universe.cluster.docker.CorfuServerDockerized;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.util.ClassUtils;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static lombok.Builder.Default;
import static org.corfudb.universe.cluster.Cluster.ClusterParams;
import static org.corfudb.universe.node.CorfuServer.ServerParams;

/**
 * Provides Docker implementation of {@link Group}.
 */
@Builder
public class DockerGroup implements Group {

    @Default
    private final ConcurrentMap<String, Node> nodes = new ConcurrentHashMap<>();
    private final DockerClient docker;
    @Getter
    private final GroupParams<? extends NodeParams> params;
    private final ClusterParams clusterParams;

    @Override
    public DockerGroup deploy() {
        for (NodeParams nodeParams : params.getNodeParams()) {
            deployNode(nodeParams);
        }

        return this;
    }

    private <P extends NodeParams> void deployNode(P nodeParams) {
        switch (nodeParams.getNodeType()){
            case CORFU_SERVER:
                CorfuServer node = CorfuServerDockerized.builder()
                        .clusterParams(clusterParams)
                        .params(ClassUtils.cast(nodeParams, ServerParams.class))
                        .docker(docker)
                        .build();

                node.deploy();
                nodes.put(node.getParams().getName(), node);
                break;
            case CORFU_CLIENT:
                throw new IllegalStateException("Corfu client not implemented");
            default:
                throw new IllegalStateException("Corfu client not implemented");
        }
    }

    @Override
    public void stop(Duration timeout) {
        nodes.values().forEach(node -> node.stop(timeout));
    }

    @Override
    public void kill() {
        nodes.values().forEach(Node::kill);
    }

    @Override
    public <T extends NodeParams> Group add(T nodeParams) {
        deployNode(nodeParams);
        params.add(nodeParams);
        return this;
    }

    @Override
    public <T extends Node> T getNode(String nodeName) {
        return ClassUtils.cast(nodes.get(nodeName));
    }

    @Override
    public ImmutableMap<String, ? extends Node> nodes() {
        return ImmutableMap.copyOf(nodes);
    }
}
