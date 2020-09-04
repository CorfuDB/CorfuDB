package org.corfudb.universe.group.cluster;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.client.LocalCorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.universe.node.server.CorfuServerParams;
import org.corfudb.universe.universe.UniverseException;
import org.corfudb.universe.universe.UniverseParams;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractCorfuCluster<P extends CorfuServerParams, U extends UniverseParams>
        extends AbstractCluster<CorfuServer, P, CorfuClusterParams<P>, U>
        implements CorfuCluster<CorfuServer, CorfuClusterParams<P>> {

    private final ConcurrentNavigableMap<String, CorfuServer> nodes = new ConcurrentSkipListMap<>();

    @NonNull
    protected final LoggingParams loggingParams;

    public AbstractCorfuCluster(CorfuClusterParams<P> params, U universeParams,
                                LoggingParams loggingParams) {
        super(params, universeParams);
        this.loggingParams = loggingParams;
    }

    protected void init() {
        params.getNodesParams().forEach(serverParams -> {
            CorfuServer server = (CorfuServer) buildServer(serverParams);
            nodes.put(server.getEndpoint(), server);
        });
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
        log.info("Deploy corfu cluster. Params: {}", params);

        List<CompletableFuture<Node>> asyncDeployment = nodes.values().stream()
                .map(this::deployAsync)
                .collect(Collectors.toList());

        asyncDeployment.stream()
                .map(CompletableFuture::join)
                .forEach(server ->
                        log.debug("Corfu server was deployed: {}", server.getParams().getName())
                );

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
                .corfuRuntimeParams(CorfuRuntime.CorfuRuntimeParameters.builder())
                .build()
                .deploy();
    }

    @Override
    public LocalCorfuClient getLocalCorfuClient(
            CorfuRuntimeParametersBuilder runtimeParametersBuilder) {
        return LocalCorfuClient.builder()
                .corfuRuntimeParams(runtimeParametersBuilder)
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
