package org.corfudb.universe.group.cluster;

import com.google.common.collect.ImmutableSortedMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.server.SupportServer;
import org.corfudb.universe.node.server.SupportServerParams;
import org.corfudb.universe.node.server.docker.DockerSupportServer;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.UniverseParams;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static lombok.Builder.Default;

@Slf4j
public abstract class AbstractSupportCluster extends
        AbstractCluster<SupportServer, SupportServerParams, SupportClusterParams, UniverseParams> {

    @Default
    private final ConcurrentNavigableMap<String, SupportServer> nodes =
            new ConcurrentSkipListMap<>();

    @NonNull
    private final SupportClusterParams clusterParams;

    private final ExecutorService executor;

    protected AbstractSupportCluster(
            UniverseParams universeParams, SupportClusterParams monitoringParams) {

        super(monitoringParams, universeParams);
        this.clusterParams = monitoringParams;
        this.executor = Executors.newCachedThreadPool();
    }

    /**
     * Deploys a {@link Group}, including the following steps:
     * a) Deploy the Corfu nodes
     * b) Bootstrap all the nodes to form a cluster
     *
     * @return an instance of {@link AbstractSupportCluster}
     */
    @Override
    public AbstractSupportCluster deploy() {
        log.info("Deploy monitoring cluster. Params: {}", clusterParams);

        List<CompletableFuture<Node>> asyncMonDeployment = clusterParams
                .getNodesParams()
                .stream()
                .map(serverParams -> {
                    DockerSupportServer server = (DockerSupportServer) buildServer(serverParams);
                    nodes.put(server.getParams().getName(), server);
                    return server;
                })
                .map(this::deployMonitoringServerAsync)
                .collect(Collectors.toList());

        asyncMonDeployment.stream()
                .map(CompletableFuture::join)
                .forEach(server -> log.debug("Corfu server was deployed: {}",
                        server.getParams().getName()));

        try {
            bootstrap();
        } catch (Exception ex) {
            throw new UniverseException("Can't deploy cluster: " + clusterParams.getName(), ex);
        }

        return this;
    }

    private CompletableFuture<Node> deployMonitoringServerAsync(Node monitoringServer) {
        return CompletableFuture.supplyAsync(monitoringServer::deploy, executor);
    }


    @Override
    public ImmutableSortedMap<String, SupportServer> nodes() {
        return ImmutableSortedMap.copyOf(nodes);
    }
}
