package org.corfudb.universe.group.cluster;

import static lombok.Builder.Default;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.node.client.LocalCorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.util.ClassUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractCorfuCluster<P extends CorfuClusterParams, U extends UniverseParams>
        implements CorfuCluster {
    @Default
    protected final ConcurrentMap<String, CorfuServer> nodes = new ConcurrentHashMap<>();
    @Getter
    @NonNull
    protected final P params;
    @NonNull
    protected final U universeParams;
    private final ExecutorService executor;

    protected AbstractCorfuCluster(P params, U universeParams) {
        this.params = params;
        this.universeParams = universeParams;
        this.executor = Executors.newCachedThreadPool();
    }

    /**
     * Deploys a {@link Group}, including the following steps:
     * a) Deploy the Corfu nodes
     * b) Bootstrap all the nodes to form a cluster
     *
     * @return an instance of {@link AbstractCorfuCluster}
     */
    @Override
    public AbstractCorfuCluster<P, U> deploy() {
        log.info("Deploy vm corfu cluster. Params: {}", params);

        List<CompletableFuture<CorfuServer>> asyncDeployment = params
                .getNodesParams()
                .stream()
                .map(serverParams -> {
                    CorfuServer server = buildCorfuServer(serverParams);
                    nodes.put(serverParams.getName(), server);
                    return server;
                })
                .map(this::deployCorfuServerAsync)
                .collect(Collectors.toList());

        asyncDeployment.stream()
                .map(CompletableFuture::join)
                .forEach(server -> log.debug("Corfu server has deployed: {}", server.getParams().getName()));

        try {
            bootstrap();
        } catch (Exception ex) {
            throw new UniverseException("Can't deploy corfu cluster: " + params.getName(), ex);
        }

        return this;
    }

    private CompletableFuture<CorfuServer> deployCorfuServerAsync(CorfuServer corfuServer) {
        return CompletableFuture.supplyAsync(corfuServer::deploy, executor);
    }

    protected abstract CorfuServer buildCorfuServer(CorfuServerParams nodeParams);

    /**
     * Stop the cluster
     * @param timeout allowed time to gracefully stop the {@link Group}
     */
    @Override
    public void stop(Duration timeout) {
        log.info("Stop corfu cluster: {}", params.getName());

        nodes.values().forEach(node -> {
            try {
                node.stop(timeout);
            } catch (Exception e) {
                log.warn("Can't stop node: {} in group: {}", node.getParams().getName(), getParams().getName(), e);
            }
        });
    }

    /**
     * Attempt to kills all the nodes in arbitrary order.
     */
    @Override
    public void kill() {
        nodes.values().forEach(node -> {
            try {
                node.kill();
            } catch (Exception e) {
                log.warn("Can't kill node: {} in group: {}", node.getParams().getName(), getParams().getName(), e);
            }
        });
    }

    @Override
    public void destroy() {
        log.info("Destroy group: {}", params.getName());

        nodes.values().forEach(node -> {
            try {
                node.destroy();
            } catch (NodeException e) {
                log.warn("Can't destroy node: {} in group: {}", node.getParams().getName(), getParams().getName(), e);
            }
        });
    }

    @Override
    public Node add(NodeParams nodeParams) {
        CorfuServerParams corfuServerParams = ClassUtils.cast(nodeParams);
        params.add(corfuServerParams);

        return deployCorfuServerAsync(buildCorfuServer(corfuServerParams))
                .join();
    }

    @Override
    public <T extends Node> T getNode(String nodeNameSuffix) {
        String fullNodeName = params.getFullNodeName(nodeNameSuffix);
        Node node = nodes.get(fullNodeName);

        if (node == null) {
            throw new NodeException(String.format(
                    "Node not found. Node name: %s, cluster: %s, cluster nodes: %s",
                    fullNodeName, params.getName(), nodes.keySet())
            );
        }

        return ClassUtils.cast(node);
    }

    @Override
    public <T extends Node> ImmutableMap<String, T> nodes() {
        return ClassUtils.cast(ImmutableMap.copyOf(nodes));
    }

    @Override
    public LocalCorfuClient getLocalCorfuClient() {
        return LocalCorfuClient.builder()
                .serverEndpoints(getClusterLayoutServers())
                .build()
                .deploy();
    }

    protected abstract ImmutableList<String> getClusterLayoutServers();
}
