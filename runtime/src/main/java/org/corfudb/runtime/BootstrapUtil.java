package org.corfudb.runtime;

import java.time.Duration;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.clients.BaseHandler;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LayoutHandler;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.ManagementHandler;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;

/**
 * Utility to bootstrap a cluster.
 *
 * <p>Created by zlokhandwala on 1/19/18.
 */
@Slf4j
public class BootstrapUtil {

    /**
     * Bootstraps the given layout.
     * Attempts to bootstrap each node finite number of times.
     * If the retries are exhausted, the utility throws the responsible exception.
     *
     * @param layout       Layout to bootstrap the cluster.
     * @param retries      Number of retries to bootstrap each node before giving up.
     * @param retryTimeout Duration between retries.
     */
    public static void bootstrap(@Nonnull Layout layout,
                                 int retries,
                                 @Nonnull Duration retryTimeout) {
        bootstrap(layout, CorfuRuntimeParameters.builder().build(), retries, retryTimeout);
    }

    /**
     * Bootstraps the given layout.
     * Attempts to bootstrap each node finite number of times.
     * If the retries are exhausted, the utility throws the responsible exception.
     *
     * @param layout                 Layout to bootstrap the cluster.
     * @param corfuRuntimeParameters CorfuRuntimeParameters can specify security parameters.
     * @param retries                Number of retries to bootstrap each node before giving up.
     * @param retryTimeout           Duration between retries.
     */
    public static void bootstrap(@Nonnull Layout layout,
                                 @Nonnull CorfuRuntimeParameters corfuRuntimeParameters,
                                 int retries,
                                 @Nonnull Duration retryTimeout) {
        for (String server : layout.getAllServers()) {
            int retry = retries;
            while (retry-- > 0) {
                try {
                    log.info("Attempting to bootstrap node:{} with layout:{}", server, layout);
                    IClientRouter router = new NettyClientRouter(NodeLocator.parseString(server),
                            corfuRuntimeParameters);
                    router.addClient(new LayoutHandler())
                            .addClient(new ManagementHandler())
                            .addClient(new BaseHandler());

                    new LayoutClient(router, layout.getEpoch())
                            .bootstrapLayout(layout).get();
                    new ManagementClient(router, layout.getEpoch())
                            .bootstrapManagement(layout).get();
                    router.stop();
                    break;
                } catch (Exception e) {
                    log.error("Bootstrapping node:{} failed with exception:", server, e);
                    if (retry == 0) {
                        throw new RuntimeException(e);
                    }
                    log.warn("Retrying {} times in {}ms.", retry, retryTimeout.toMillis());
                    Sleep.MILLISECONDS.sleepUninterruptibly(retryTimeout.toMillis());
                }
            }
        }
    }
}
