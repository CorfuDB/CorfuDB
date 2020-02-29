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

}
