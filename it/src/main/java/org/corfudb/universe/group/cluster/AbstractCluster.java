package org.corfudb.universe.group.cluster;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.group.Group;
import org.corfudb.universe.group.Group.GroupParams;
import org.corfudb.universe.node.Node;
import org.corfudb.universe.node.Node.NodeParams;
import org.corfudb.universe.node.NodeException;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.common.util.ClassUtils;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
public abstract class AbstractCluster<
        T extends Node,
        N extends NodeParams,
        P extends GroupParams<N>,
        U extends UniverseParams> implements Cluster<T, P> {

    @Getter
    @NonNull
    protected final P params;

    @NonNull
    protected final U universeParams;

    private final ExecutorService executor;

    protected AbstractCluster(P params, U universeParams) {
        this.params = params;
        this.universeParams = universeParams;
        this.executor = Executors.newCachedThreadPool();
    }

    protected CompletableFuture<Node> deployAsync(Node server) {
        return CompletableFuture.supplyAsync(server::deploy, executor);
    }

    protected abstract Node buildServer(N nodeParams);

    /**
     * Stop the cluster
     *
     * @param timeout allowed time to gracefully stop the {@link Group}
     */
    @Override
    public void stop(Duration timeout) {
        log.info("Stop corfu cluster: {}", params.getName());

        nodes().values().forEach(node -> {
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
        nodes().values().forEach(node -> {
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

        nodes().values().forEach(node -> {
            try {
                node.destroy();
            } catch (NodeException e) {
                log.warn("Can't destroy node: {} in group: {}", node.getParams().getName(), getParams().getName(), e);
            }
        });
    }

    @Override
    public Node add(NodeParams nodeParams) {
        N corfuServerParams = ClassUtils.cast(nodeParams);
        params.add(corfuServerParams);

        return deployAsync(buildServer(corfuServerParams)).join();
    }
}
