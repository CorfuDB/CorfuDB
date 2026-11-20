package org.corfudb.runtime.clients;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.NodeLocator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * This class tests the side-effects of closing the client router on the thread pool
 * it uses.
 *
 * Created by Maithem on 2/28/20.
 */

public class NettyClientRouterTest {

    @Test
    public void eventLoopCleanUpTest() {
        String host = "localhost";
        final int port = 9000;

        NettyClientRouter managedRouter = new NettyClientRouter(NodeLocator.builder().host(host).port(port).build(),
                CorfuRuntime.CorfuRuntimeParameters.builder().build());

        assertThat(managedRouter.getEventLoopGroup().isShutdown()).isFalse();
        assertThat(managedRouter.getEventLoopGroup().isShuttingDown()).isFalse();

        // close the router and verify that the event loop has been shutdown
        managedRouter.close();
        assertThat(managedRouter.getEventLoopGroup().isShuttingDown()).isTrue();

        final int numThreads = 1;
        EventLoopGroup eventLoopGroup = ChannelImplementation.NIO.getGenerator().generate(numThreads,
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("netty1-%d")
                .build());

        NettyClientRouter router = new NettyClientRouter(NodeLocator.builder().host(host).port(port).build(),
                eventLoopGroup,
                CorfuRuntime.CorfuRuntimeParameters.builder().build());

        assertThat(router.getEventLoopGroup().isShuttingDown()).isFalse();
        assertThat(router.getEventLoopGroup().isShutdown()).isFalse();
        // Verify that when the router is closed the event loop isn't shutdown
        router.close();
        assertThat(router.getEventLoopGroup().isShuttingDown()).isFalse();
        assertThat(router.getEventLoopGroup().isShutdown()).isFalse();
    }

    /**
     * {@link NodeRouterPool#getRouter} has no shutdown-awareness: it will create and
     * register a new {@link NettyClientRouter} regardless of whether the owning
     * {@link CorfuRuntime} has already been shut down. Once created, a router's
     * Netty-level reconnect loop is self-sustaining and needs nothing further from
     * {@link CorfuRuntime}/{@link NodeRouterPool} to keep running -- the only way to
     * stop it is an explicit {@link NettyClientRouter#stop()} call.
     *
     * <p>A caller that still holds a reference to a {@link CorfuRuntime} and calls
     * {@link CorfuRuntime#getRouter} on it -- not knowing that something else already
     * called {@link CorfuRuntime#shutdown} on the same instance -- is a real, ordinary
     * usage pattern: {@code FailureDetector} and {@code ManagementView} both call
     * {@code getRouter()} directly. The router such a call returns is fully armed and
     * will keep reconnecting to its target forever, invisible to anything that only
     * inspects a tracked {@link NodeRouterPool}.
     */
    @Test
    public void testGetRouterAfterShutdownLeaksNettyClientRouter() {
        CorfuRuntime.overrideGetRouterFunction = null;

        CorfuRuntime victimRuntime = CorfuRuntime.fromParameters(
                CorfuRuntime.CorfuRuntimeParameters.builder().build());

        // Shut the runtime down first. At this point its NodeRouterPool is empty, so there
        // is nothing to stop.
        victimRuntime.shutdown();

        NettyClientRouter orphanRouter = (NettyClientRouter) victimRuntime.getRouter("localhost:9000");
        assertThat(orphanRouter).isNotNull();

        // The bug: shutdown was false -- nothing will ever call stop() on this router, so it
        // remains fully armed and will keep retrying this endpoint forever, invisible to
        // anything that only inspects a tracked NodeRouterPool.
        assertThat(orphanRouter.shutdown).isTrue();
    }

    /**
     * Complementary to {@link #testGetRouterAfterShutdownLeaksNettyClientRouter}: a router
     * obtained from an already-shut-down {@link NodeRouterPool} must not be memoized. If it
     * were, a later caller could look it up by endpoint and mistake it for a live, tracked
     * router instead of the orphan it actually is.
     */
    @Test
    public void testGetRouterAfterShutdownDoesNotMemoizeRouter() {
        CorfuRuntime.overrideGetRouterFunction = null;

        CorfuRuntime victimRuntime = CorfuRuntime.fromParameters(
                CorfuRuntime.CorfuRuntimeParameters.builder().build());
        victimRuntime.shutdown();

        NettyClientRouter firstRouter = (NettyClientRouter) victimRuntime.getRouter("localhost:9000");
        NettyClientRouter secondRouter = (NettyClientRouter) victimRuntime.getRouter("localhost:9000");

        assertThat(secondRouter).isNotSameAs(firstRouter);
    }

}
