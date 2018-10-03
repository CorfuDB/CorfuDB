package org.corfudb.runtime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import lombok.AccessLevel;
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

    @Getter(AccessLevel.PROTECTED)
    private final Map<NodeLocator, IClientRouter> nodeRouters = new ConcurrentHashMap<>();

    /**
     * A function to handle getting routers. Used by test framework to inject
     * a test router. Can also be used to provide alternative logic for obtaining
     * a router.
     */
    @Getter
    @Setter
    private Function<String, IClientRouter> createRouterFunction;

    NodeRouterPool(Function<String, IClientRouter> createRouterFunction) {
        this.createRouterFunction = createRouterFunction;
    }

    /**
     * Fetches a router from the pool if already present. Else creates a new router using the
     * provided function and adds it to the pool.
     *
     * @param endpoint Endpoint to connect the router.
     * @return IClientRouter.
     */
    public IClientRouter getRouter(NodeLocator endpoint) {
        return nodeRouters.computeIfAbsent(endpoint,
                s -> createRouterFunction.apply(NodeLocator.getLegacyEndpoint(s)));
    }

    /**
     * Shutdown all the routers in the pool.
     */
    public void shutdown() {
        for (IClientRouter r : nodeRouters.values()) {
            r.stop();
        }
    }
}
