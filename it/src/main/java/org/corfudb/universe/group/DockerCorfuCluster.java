package org.corfudb.universe.group;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DockerClient;
import lombok.Builder;
import lombok.Getter;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.universe.node.CorfuServer;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.universe.docker.CorfuServerDockerized;
import org.corfudb.universe.util.ClassUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static lombok.Builder.Default;
import static org.corfudb.universe.node.CorfuServer.ServerParams;
import static org.corfudb.universe.universe.Universe.UniverseParams;

/**
 * Provides Docker implementation of {@link Group}.
 */
@Builder
public class DockerCorfuCluster implements CorfuCluster {

    @Default
    private final ConcurrentMap<String, Node> nodes = new ConcurrentHashMap<>();
    private final DockerClient docker;
    @Getter
    private final CorfuClusterParams params;
    private final UniverseParams universeParams;

    @Override
    public DockerCorfuCluster deploy() {
        for (ServerParams nodeParams : params.getNodesParams()) {
            deployNode(nodeParams);
        }

        bootstrap();

        return this;
    }

    private Node deployNode(ServerParams nodeParams) {
        CorfuServer node = CorfuServerDockerized.builder()
                .universeParams(universeParams)
                .params(ClassUtils.cast(nodeParams, ServerParams.class))
                .docker(docker)
                .build();

        node.deploy();
        nodes.put(node.getParams().getName(), node);
        return node;
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
    public Node add(NodeParams nodeParams) {
        ServerParams serverParams = ClassUtils.cast(nodeParams);
        Node node = deployNode(serverParams);
        params.add(serverParams);
        return node;
    }

    @Override
    public <T extends Node> T getNode(String nodeName) {
        return ClassUtils.cast(nodes.get(nodeName));
    }

    @Override
    public <T extends Node> ImmutableMap<String, T> nodes() {
        return ClassUtils.cast(ImmutableMap.copyOf(nodes));
    }

    @Override
    public void bootstrap() {
        BootstrapUtil.bootstrap(getLayout(), params.getBootStrapRetries(), params.getRetryTimeout());
    }


    private Layout getLayout() {
        long epoch = 0;
        UUID clusterId = UUID.randomUUID();
        List<String> servers = params.getServers();

        LayoutSegment segment = new LayoutSegment(
                Layout.ReplicationMode.CHAIN_REPLICATION,
                0L,
                -1L,
                Collections.singletonList(new LayoutStripe(params.getServers()))
        );
        return new Layout(servers, servers, Collections.singletonList(segment), epoch, clusterId);
    }
}
