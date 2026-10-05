package org.corfudb.runtime;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Pool of client routers.
 *
 * <p>Created by zlokhandwala on 2/20/18.
 *
 * <p>All mutation of and iteration over the router map lives in this class, behind this
 * object's own monitor -- nothing outside this class touches the map directly. That is
 * deliberate: every method here must agree on what "the pool is shut down" means at the
 * same instant, or a router can end up created (or left behind) outside anything that will
 * ever call stop() on it. See {@link #getRouter} for the specific failure this prevents.
 */
@Slf4j
public class NodeRouterPool {

    private final Map<NodeLocator, IClientRouter> nodeRouters = new ConcurrentHashMap<>();

    /**
     * A function to handle getting routers. Used by test framework to inject
     * a test router. Can also be used to provide alternative logic for obtaining
     * a router.
     */
    @Getter
    @Setter
    private Function<String, IClientRouter> createRouterFunction;

    /**
     * Set once {@link #shutdown()} has run. Guarded by this object's own monitor along with
     * {@link #nodeRouters}; see {@link #getRouter} for why a router created after this point
     * must not be tracked the same way as one created before it.
     */
    private boolean shutdown = false;

    NodeRouterPool(Function<String, IClientRouter> createRouterFunction) {
        this.createRouterFunction = createRouterFunction;
    }

    /**
     * Fetches a router from the pool if already present. Else creates a new router using the
     * provided function and adds it to the pool.
     * <p>
     * If the pool has already been shut down, a router is still created and returned,
     * but it is stopped before being handed back, and it is never added to the
     * pool. Without this, such a router would never be visited by any future shutdown() sweep
     * and could keep retrying its target forever, invisible to anything that only inspects a
     * tracked NodeRouterPool.
     *
     * @param endpoint Endpoint to connect the router.
     * @return IClientRouter.
     */
    public IClientRouter getRouter(NodeLocator endpoint) {
        synchronized (this) {
            if (!shutdown) {
                return nodeRouters.computeIfAbsent(endpoint,
                        s -> createRouterFunction.apply(s.toEndpointUrl()));
            }
        }
        // Pool is already shut down. Construction and stop() both happen outside the lock,
        // same as every other router I/O in this class: neither touches nodeRouters, and
        // stop() can block (e.g. closing a Netty channel), which would otherwise stall every
        // concurrent getRouter()/shutdown()/reconnect()/pruneRemovedRouters() caller.
        IClientRouter router = createRouterFunction.apply(endpoint.toEndpointUrl());
        router.stop();
        return router;
    }

    /**
     * Shutdown all the routers in the pool, and ensure no router created from this point on
     * can be left untracked. See {@link #getRouter} for why untracked routers are a problem.
     */
    public void shutdown() {
        List<IClientRouter> toStop;
        synchronized (this) {
            shutdown = true;
            toStop = new ArrayList<>(nodeRouters.values());
        }
        for (IClientRouter r : toStop) {
            r.stop();
        }
    }

    /**
     * Reestablish all connections in the pool. A no-op once the pool has been shut down --
     * there is nothing left to reestablish, and reconnecting after shutdown() has already
     * begun its sweep would race it for no benefit.
     */
    public void reconnect() {
        List<IClientRouter> toReconnect;
        synchronized (this) {
            if (shutdown) {
                return;
            }
            toReconnect = new ArrayList<>(nodeRouters.values());
        }
        for (IClientRouter r : toReconnect) {
            r.reconnect();
        }
    }

    /**
     * Stop and remove every tracked router whose endpoint is not in {@code validEndpoints}.
     *
     * <p>This is the only place routers are removed from the pool. Removal and stop() must
     * be decided under the same lock used by {@link #getRouter}/{@link #shutdown}/
     * {@link #reconnect} -- if a caller outside this class read the router map, decided what
     * to remove, and mutated it separately, that decision could be based on a view that is
     * already stale by the time it acts on it, racing whichever of those methods runs
     * concurrently.
     *
     * @param validEndpoints The full set of endpoints (in {@code host:port} form) that
     *                        should remain tracked.
     */
    public void pruneRemovedRouters(@Nonnull Set<String> validEndpoints) {
        List<IClientRouter> toStop = new ArrayList<>();
        synchronized (this) {
            if (shutdown) {
                return;
            }
            for (NodeLocator endpoint : new ArrayList<>(nodeRouters.keySet())) {
                if (!validEndpoints.contains(endpoint.toEndpointUrl())) {
                    IClientRouter router = nodeRouters.remove(endpoint);
                    if (router != null) {
                        toStop.add(router);
                    }
                }
            }
        }
        for (IClientRouter router : toStop) {
            try {
                // Stop the channel from keeping connecting/reconnecting to server.
                // Also if channel is not closed properly, router will be garbage collected.
                router.stop();
            } catch (Exception e) {
                log.warn("pruneRemovedRouters: Exception in stopping and removing "
                        + "router connection: ", e);
            }
        }
    }
}
