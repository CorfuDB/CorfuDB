package org.corfudb.runtime;

import java.time.Duration;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.ManagementClient;
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
        for (String server : layout.getAllServers()) {
            int retry = retries;
            while (retry-- > 0) {
                try {
                    log.info("Attempting to bootstrap node:{} with layout:{}", server, layout);
                    IClientRouter router = new NettyClientRouter(NodeLocator.parseString(server),
                            CorfuRuntime.CorfuRuntimeParameters.builder().build());
                    router.addClient(new LayoutClient())
                            .addClient(new ManagementClient())
                            .addClient(new BaseClient());

                    router.getClient(LayoutClient.class).bootstrapLayout(layout).get();
                    router.getClient(ManagementClient.class).bootstrapManagement(layout).get();
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
