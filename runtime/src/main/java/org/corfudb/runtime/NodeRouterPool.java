package org.corfudb.runtime;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.util.NodeLocator;

/**
 * Pool of client routers.
 *
 * <p>Created by zlokhandwala on 2/20/18.
 */
@Slf4j
public class NodeRouterPool {

    private final Map<NodeLocator, Map<Integer, IClientRouter>> nodeRouters = new ConcurrentHashMap<>();

    /**
     * A function to handle getting routers. Used by test framework to inject
     * a test router. Can also be used to provide alternative logic for obtaining
     * a router.
     */
    @Getter
    @Setter
    private Function<String, IClientRouter> createRouterFunction;

    @Getter
    private final int maxNumConnectionsPerNode;

    public NodeRouterPool(int maxNumConnectionsPerNode, Function<String, IClientRouter> createRouterFunction) {
        this.maxNumConnectionsPerNode = maxNumConnectionsPerNode;
        this.createRouterFunction = createRouterFunction;
    }

    NodeRouterPool(Function<String, IClientRouter> createRouterFunction) {
        this(1, createRouterFunction);
    }

    /**
     * Fetches a router from the pool if already present. Else creates a new router using the
     * provided function and adds it to the pool.
     *
     * @param endpoint Endpoint to connect the router.
     * @return IClientRouter.
     */
    public IClientRouter getRouter(NodeLocator endpoint) {
        Map<Integer, IClientRouter> nodeChannels = nodeRouters.computeIfAbsent(endpoint,
                k -> new ConcurrentHashMap<>(maxNumConnectionsPerNode));
        int idx = ThreadLocalRandom.current().nextInt(0, maxNumConnectionsPerNode);
        return nodeChannels.computeIfAbsent(idx, k -> createRouterFunction.apply(endpoint.toEndpointUrl()));
    }

    public IClientRouter getRouter(NodeLocator endpoint, int idx) {
        Map<Integer, IClientRouter> nodeChannels = nodeRouters.computeIfAbsent(endpoint,
                k -> new ConcurrentHashMap<>(maxNumConnectionsPerNode));
        return nodeChannels.computeIfAbsent(idx, k -> createRouterFunction.apply(endpoint.toEndpointUrl()));
    }

    public void remove(NodeLocator endpoint) {
        Map<Integer, IClientRouter> nodeChannels  = nodeRouters.remove(endpoint);
        for (IClientRouter r : nodeChannels.values()) {
            try {
                r.stop();
            } catch (Exception e) {
                log.warn("remove: Exception in stopping and removing "
                        + "router connection to node {} :", endpoint, e);
            }
        }
    }

    public Collection<NodeLocator> getAllNodes() {
        return nodeRouters.keySet();
    }

    /**
     * Shutdown all the routers in the pool.
     */
    public void shutdown() {
        for (Map<Integer, IClientRouter> channels : nodeRouters.values()) {
            for (IClientRouter r : channels.values()) {
                r.stop();
            }
        }
    }
}
