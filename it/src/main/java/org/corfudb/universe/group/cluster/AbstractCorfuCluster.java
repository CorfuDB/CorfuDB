package org.corfudb.universe.group.cluster;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.client.LocalCorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.UniverseParams;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractCorfuCluster<U extends UniverseParams>
        extends AbstractCluster<CorfuServer, CorfuServerParams, CorfuClusterParams, U>
        implements CorfuCluster<CorfuServer, CorfuClusterParams> {

    private final ConcurrentNavigableMap<String, CorfuServer> nodes = new ConcurrentSkipListMap<>();

    public AbstractCorfuCluster(CorfuClusterParams params, U universeParams) {
        super(params, universeParams);
    }

    /**
     * Deploys a {@link Group}, including the following steps:
     * a) Deploy the Corfu nodes
     * b) Bootstrap all the nodes to form a cluster
     *
     * @return an instance of {@link AbstractCorfuCluster}
     */
    @Override
    public AbstractCorfuCluster deploy() {
        log.info("Deploy corfu cluster. Params: {}", params);

        List<CompletableFuture<Node>> asyncDeployment = params
                .<Node.NodeParams>getNodesParams()
                .stream()
                .map(serverParams -> {
                    CorfuServer server = (CorfuServer) buildServer(serverParams);
                    nodes.put(server.getEndpoint(), server);
                    return server;
                })
                .map(this::deployAsync)
                .collect(Collectors.toList());

        asyncDeployment.stream()
                .map(CompletableFuture::join)
                .forEach(server -> log.debug("Corfu server was deployed: {}",
                        server.getParams().getName()));

        try {
            bootstrap();
        } catch (Exception ex) {
            throw new UniverseException("Can't deploy corfu cluster: " + params.getName(), ex);
        }

        return this;
    }

    @Override
    public LocalCorfuClient getLocalCorfuClient() {
        return LocalCorfuClient.builder()
                .serverEndpoints(getClusterLayoutServers())
                .prometheusMetricsPort(Optional.empty())
                .build()
                .deploy();
    }

    @Override
    public LocalCorfuClient getLocalCorfuClient(int metricsPort) {
        return LocalCorfuClient.builder()
                .prometheusMetricsPort(Optional.of(metricsPort))
                .serverEndpoints(getClusterLayoutServers())
                .build()
                .deploy();
    }

    protected abstract ImmutableSortedSet<String> getClusterLayoutServers();

    @Override
    public ImmutableSortedMap<String, CorfuServer> nodes() {
        return ImmutableSortedMap.copyOf(nodes);
    }
}
