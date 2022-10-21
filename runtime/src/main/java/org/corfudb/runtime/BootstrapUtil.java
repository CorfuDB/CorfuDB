package org.corfudb.runtime;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.clients.BaseHandler;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LayoutHandler;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.ManagementHandler;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Utility to bootstrap a cluster.
 *
 * <p>Created by zlokhandwala on 1/19/18.
 */
@Slf4j
public class BootstrapUtil {

    private BootstrapUtil() {
        // prevent instantiation of this class
    }

    /**
     * Bootstraps the given layout.
     * Attempts to bootstrap each node finite number of times.
     * If the retries are exhausted, the utility throws the responsible exception.
     *
     * @param layout       Layout to bootstrap the cluster.
     * @param retries      Number of retries to bootstrap each node before giving up.
     * @param retryDuration Duration between retries.
     */
    public static void bootstrap(@NonNull Layout layout,
                                 int retries,
                                 @NonNull Duration retryDuration) {
        bootstrap(layout, CorfuRuntimeParameters.builder().build(), retries, retryDuration);
    }

    /**
     * Bootstraps a layout server connected to the specified router.
     *
     * @param router Router connected to the node.
     * @param layout Layout to bootstrap with
     */
    private static void bootstrapLayoutServer(IClientRouter router, Layout layout)
            throws ExecutionException, InterruptedException, AlreadyBootstrappedException {
        LayoutClient layoutClient = new LayoutClient(router, layout.getEpoch(), layout.getClusterId());

        try {
            CFUtils.getUninterruptibly(layoutClient.bootstrapLayout(layout),
                    AlreadyBootstrappedException.class);
        } catch (AlreadyBootstrappedException abe) {
            if (!layoutClient.getLayout().get().equals(layout)) {
                log.error("BootstrapUtil: Layout Server {}:{} already bootstrapped with different "
                        + "layout.", router.getHost(), router.getPort());
                throw abe;
            }
        }
    }

    /**
     * Bootstraps a management server connected to the specified router.
     *
     * @param router Router connected to the node.
     * @param layout Layout to bootstrap with
     */
    private static void bootstrapManagementServer(IClientRouter router, Layout layout)
            throws ExecutionException, InterruptedException, AlreadyBootstrappedException {
        ManagementClient managementClient
                = new ManagementClient(router, layout.getEpoch(), layout.getClusterId());

        try {
            CFUtils.getUninterruptibly(managementClient.bootstrapManagement(layout),
                    AlreadyBootstrappedException.class);
        } catch (AlreadyBootstrappedException abe) {
            if (!managementClient.getLayout().get().equals(layout)) {
                log.error("BootstrapUtil: Management Server {}:{} already bootstrapped with "
                        + "different layout.", router.getHost(), router.getPort());
                throw abe;
            }
        }
    }

    /**
     * Bootstraps the given layout.
     * Attempts to bootstrap each node finite number of times.
     * If the retries are exhausted, the utility throws the responsible exception.
     *
     * @param layout                 Layout to bootstrap the cluster.
     * @param corfuRuntimeParameters CorfuRuntimeParameters can specify security parameters.
     * @param retries                Number of retries to bootstrap each node before giving up.
     * @param retryDuration          Duration between retries.
     */
    public static void bootstrap(@NonNull Layout layout,
                                 @NonNull CorfuRuntimeParameters corfuRuntimeParameters,
                                 int retries,
                                 @NonNull Duration retryDuration) {
        for (String server : layout.getAllServers()) {
            int retry = retries;

            try(NettyClientRouter router = new  NettyClientRouter(NodeLocator.parseString(server),
                    corfuRuntimeParameters)) {
                router.addClient(new LayoutHandler())
                        .addClient(new ManagementHandler())
                        .addClient(new BaseHandler());

                while (retry-- > 0) {
                    try {
                        log.info("Attempting to bootstrap node: {} with layout:{}", server, layout);
                        bootstrapLayoutServer(router, layout);
                        bootstrapManagementServer(router, layout);
                        break;
                    } catch (AlreadyBootstrappedException abe) {
                        log.error("Bootstrapping node: {} failed with exception:", server, abe);
                        log.error("Cannot retry since already bootstrapped.");
                        throw new RuntimeException(abe);
                    } catch (Exception e) {
                        log.error("Bootstrapping node: {} failed with exception:", server, e);
                        if (retry == 0) {
                            throw new RetryExhaustedException("Bootstrapping node: retry exhausted");
                        }
                        log.warn("Retrying bootstrap {} times in {}ms.", retry, retryDuration.toMillis());
                        Sleep.sleepUninterruptibly(retryDuration);
                    }
                }
            }
        }
        log.info("Successfully Bootstrapped layout:{} .", layout);
    }

    /**
     * Bootstrap the given layout for all the nodes in the router map with the configured
     * routers. This implementation ignores the AlreadyBootstrappedException.
     * @param routerMap A configured router map
     * @param layout A layout to bootstrap
     * @param retries Num retries
     * @param retryDuration Duration between retries
     */
    public static void bootstrapWithRouterMap(Map<String, NettyClientRouter> routerMap,
                                              Layout layout, int retries,
                                              @NonNull Duration retryDuration) {
        for (String server : routerMap.keySet()) {
            int retry = retries;
            try (NettyClientRouter router = routerMap.get(server)) {
                while (retry-- > 0) {
                    try {
                        log.info("Attempting to bootstrap node: {} with layout:{}", server, layout);
                        bootstrapLayoutServer(router, layout);
                        bootstrapManagementServer(router, layout);
                        break;
                    } catch (AlreadyBootstrappedException abe) {
                        log.warn("Node already bootstrapped. Skipping.");
                        break;
                    } catch (Exception e) {
                        log.error("Bootstrapping node: {} failed with exception:", server, e);
                        if (retry == 0) {
                            throw new RetryExhaustedException("Bootstrapping node: retry exhausted");
                        }
                        log.warn("Retrying bootstrap {} times in {}ms.", retry, retryDuration.toMillis());
                        Sleep.sleepUninterruptibly(retryDuration);
                    }
                }
            }
            log.info("Successfully Bootstrapped layout:{} from the router map.", layout);
        }
    }
}
