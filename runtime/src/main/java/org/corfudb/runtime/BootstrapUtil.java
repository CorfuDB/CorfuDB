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
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

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
     * @param layout        Layout to bootstrap the cluster.
     * @param retries       Number of retries to bootstrap each node before giving up.
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
    public static void bootstrapLayoutServer(IClientRouter router, Layout layout) {
        LayoutClient layoutClient =
                new LayoutClient(router, layout.getEpoch(), layout.getClusterId());

        bootstrap(router, layout,
                () -> layoutClient.bootstrapLayout(layout),
                layoutClient::getLayout);
    }

    /**
     * Bootstraps a management server connected to the specified router.
     *
     * @param router Router connected to the node.
     * @param layout Layout to bootstrap with
     */
    public static void bootstrapManagementServer(IClientRouter router, Layout layout) {
        ManagementClient managementClient
                = new ManagementClient(router, layout.getEpoch(), layout.getClusterId());

        bootstrap(router, layout,
                () -> managementClient.bootstrapManagement(layout),
                managementClient::getLayout);
    }

    /**
     *
     * Bootstrap a server with a specific function.
     *
     * @param router          Router connected to the node.
     * @param layout          Layout to bootstrap with.
     * @param bootstrapLayoutSupplier A supplier for the function to bootstrap a server.
     * @param layoutFutureSupplier A supplier for the function to get a layout.
     */
    private static void bootstrap(IClientRouter router, Layout layout,
                                 Supplier<CompletableFuture<Boolean>> bootstrapLayoutSupplier,
                                 Supplier<CompletableFuture<Layout>> layoutFutureSupplier) {
        CompletableFuture<Void> bootstrapServer = bootstrapLayoutSupplier
                .get()
                .exceptionally(ex -> {
                    try {
                        CFUtils.unwrap(ex, AlreadyBootstrappedException.class);
                    } catch (AlreadyBootstrappedException e) {
                        log.warn("BootstrapUtil: Server {}:{} already bootstrapped.",
                                router.getHost(), router.getPort());
                        return false;
                    }
                    return true;
                })
                .thenCompose(bootstrapSucceeded -> {
                    if (bootstrapSucceeded) {
                        return CompletableFuture.completedFuture(layout);
                    }
                    return layoutFutureSupplier.get();
                })
                .thenAccept(retrievedLayout -> {
                    if (retrievedLayout.equals(layout)) {
                        return;
                    }
                    throw new AlreadyBootstrappedException(
                            String.format("Server is already bootstrapped with " +
                                    "%s but intended bootstrap layout is %s.", retrievedLayout, layout));
                });
        CFUtils.getUninterruptibly(bootstrapServer);
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
            bootstrapServer(server, layout, corfuRuntimeParameters, retries, retryDuration);
        }
        log.info("Successfully bootstrapped a layout:{}.", layout);
    }

    /**
     * Bootstraps the given server's layout and management servers.
     * Attempts to bootstrap a server finite number of times.
     * If the server is already bootstrapped with a different layout, throw an exception.
     * If the retries are exhausted, the utility throws the responsible exception.
     *
     * @param server                 A particular server.
     * @param layout                 Layout to bootstrap the server.
     * @param corfuRuntimeParameters CorfuRuntimeParameters can specify security parameters.
     * @param retries                Number of retries to bootstrap a node before giving up.
     * @param retryDuration          Duration between retries.
     */
    public static void bootstrapServer(@NonNull String server,
                                       @NonNull Layout layout,
                                       @NonNull CorfuRuntimeParameters corfuRuntimeParameters,
                                       int retries,
                                       @NonNull Duration retryDuration) {

        try (NettyClientRouter router = new NettyClientRouter(NodeLocator.parseString(server),
                corfuRuntimeParameters)) {
            router.addClient(new LayoutHandler())
                    .addClient(new ManagementHandler())
                    .addClient(new BaseHandler());
            BiConsumer<IClientRouter, Layout> bootstrapLayoutServer =
                    BootstrapUtil::bootstrapLayoutServer;
            BiConsumer<IClientRouter, Layout> bootstrapManagementServer =
                    BootstrapUtil::bootstrapManagementServer;
            retryBootstrap(server, layout, retries, retryDuration, router,
                    bootstrapLayoutServer.andThen(bootstrapManagementServer));
        }
    }

    /**
     * Bootstrap the given server with a provided router, bootstrap function, number of retries and an interval.
     * A bootstrap function is a layout server bootstrap function, a management server bootstrap function,
     * or the composition of both.
     * If the server is already bootstrapped with a different layout, throw an exception.
     * If the client's epoch is ahead of server's epoch, throw an exception.
     * If the retries are exhausted, throw the responsible exception.
     *
     * @param server            A particular server.
     * @param layout            Layout to bootstrap the server.
     * @param retries           Number of retries to bootstrap a node before giving up.
     * @param retryDuration     Duration between retries.
     * @param router            An instance of a client router to the server.
     * @param bootstrapFunction A layout server bootstrap function,
     *                          a management server bootstrap function or their composition.
     */
    public static void retryBootstrap(@NonNull String server,
                                      @NonNull Layout layout,
                                      int retries,
                                      @NonNull Duration retryDuration,
                                      @NonNull IClientRouter router,
                                      @NonNull BiConsumer<IClientRouter, Layout> bootstrapFunction) {
        int retry = retries;
        while (retry-- > 0) {
            try {
                log.info("Attempting to bootstrap node: {} with layout:{}", server, layout);
                bootstrapFunction.accept(router, layout);
                break;
            } catch (AlreadyBootstrappedException abe) {
                log.error("Bootstrapping node: {} failed because the node " +
                        "is bootstrapped with a different layout.", server);
                throw abe;
            } catch (WrongEpochException wee) {
                log.error("Bootstrapping node: {} failed because the client's " +
                        "epoch is ahead of the layout server's epoch.", server);
                throw wee;
            } catch (Exception e) {
                log.error("Bootstrapping node: {} failed with exception: {}", server, e.getMessage());
                if (retry == 0) {
                    throw e;
                }
                log.warn("Retrying bootstrap {} times in {}ms.", retry, retryDuration.toMillis());
                Sleep.sleepUninterruptibly(retryDuration);
            }
        }
        log.info("Successfully bootstrapped a server:{} .", server);
    }
}
