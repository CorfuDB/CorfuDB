package org.corfudb.universe.service;

import com.google.common.collect.ImmutableList;
import com.spotify.docker.client.DockerClient;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.universe.cluster.docker.CorfuServerDockerized;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static lombok.Builder.Default;
import static org.corfudb.universe.cluster.Cluster.ClusterParams;
import static org.corfudb.universe.node.CorfuServer.ServerParams;

/**
 * Provides Docker implementation of {@link Service}.
 */
@Builder
public class DockerService implements Service {
    @Default
    private final ImmutableList<Node> nodes = ImmutableList.of();
    private final DockerClient docker;
    @Getter
    private final ServiceParams<ServerParams> params;
    private final ClusterParams clusterParams;

    @Override
    public DockerService deploy() {
        List<Node> nodesSnapshot = new ArrayList<>();
        for (ServerParams serverParam : params.getNodes()) {
            CorfuServer node = CorfuServerDockerized.builder()
                    .clusterParams(clusterParams)
                    .params(serverParam)
                    .docker(docker)
                    .build();

            node = node.deploy();
            nodesSnapshot.add(node);
        }

        return DockerService
                .builder()
                .clusterParams(clusterParams)
                .params(params)
                .nodes(ImmutableList.copyOf(nodesSnapshot))
                .docker(docker)
                .build();
    }

    @Override
    public void stop(Duration timeout) {
        nodes.forEach(n -> n.stop(timeout));
    }

    @Override
    public void kill() {
        nodes.forEach(Node::kill);
    }

    @Override
    public void unlink(Node node) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public ImmutableList<Node> nodes() {
        return nodes;
    }
}
