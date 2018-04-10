package org.corfudb.test.benchmark;

import static org.corfudb.test.parameters.Servers.SERVER_0;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ManagementServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.test.ServerOptionsMap;
import org.corfudb.util.NodeLocator;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/** State for a Corfu benchmark. Initializes a simple single-server instance.
 *  Benchmark states for {@link AbstractCorfuBenchmark} should extend from this
 *  class.
 */
@State(Scope.Benchmark)
public class CorfuBenchmarkState {

    /** A single boss group for the trial. */
    @Getter
    EventLoopGroup bossGroup;

    /** A single worker group for the trial. */
    @Getter
    EventLoopGroup workerGroup;

    /** A single client group for the trial. */
    @Getter
    EventLoopGroup clientGroup;

    /** A single runtime group for the trial. */
    @Getter
    EventLoopGroup runtimeGroup;

    /** A map of servers, updated on each iteration. */
    @Getter
    Map<NodeLocator, CorfuServer> serverMap;

    /** Do iteration level setup, which sets up the server.
     *
     * You most likely want to override this method to load objects or initialize
     * iteration state, after calling this supermethod.
     */
    @Setup(Level.Iteration)
    public void initializeIteration() {
        final ServerContext serverContext = new ServerContext(ServerOptionsMap
            .builder()
            .port("0")
            .bossGroup(getBossGroup())
            .workerGroup(getWorkerGroup())
            .clientGroup(getClientGroup())
            .build().toMap());

        CorfuServer singleServer =
            new CorfuServer(serverContext,
                ImmutableMap.<Class<? extends AbstractServer>, AbstractServer>builder()
                    .put(BaseServer.class, new BaseServer(serverContext))
                    .put(SequencerServer.class, new SequencerServer(serverContext))
                    .put(LayoutServer.class, new LayoutServer(serverContext))
                    .put(LogUnitServer.class, new LogUnitServer(serverContext))
                    .build()
                );

        singleServer.getServer(SequencerServer.class)
            .setBootstrapEpoch(0);

        singleServer.start();

        serverMap.put(SERVER_0.getLocator(), singleServer);
    }

    /** Do trial level setup, which sets up the event loops. */
    @Setup(Level.Trial)
    public void initializeTrial() {
        bossGroup = new DefaultEventLoopGroup(1,
            new ThreadFactoryBuilder()
                .setNameFormat("boss-%d")
                .setDaemon(true)
                .build());
        int numThreads = Runtime.getRuntime().availableProcessors() * 2;

        workerGroup = new DefaultEventLoopGroup(numThreads,
            new ThreadFactoryBuilder()
                .setNameFormat("worker-%d")
                .setDaemon(true)
                .build());

        clientGroup = new DefaultEventLoopGroup(numThreads,
            new ThreadFactoryBuilder()
                .setNameFormat("client-%d")
                .setDaemon(true)
                .build());

        runtimeGroup = new DefaultEventLoopGroup(numThreads,
            new ThreadFactoryBuilder()
                .setNameFormat("netty-%d")
                .setDaemon(true)
                .build());


        serverMap = new HashMap<>();
    }

    /** Tear down the trial state, which shuts down the event loops. */
    @TearDown(Level.Trial)
    public void teardownTrial() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
        runtimeGroup.shutdownGracefully();
    }

    /** Tear down the iteration state, which shuts down the servers. */
    @TearDown(Level.Iteration)
    public void teardownIteration() {
        serverMap.values().forEach(CorfuServer::close);
    }

    /** Get a new {@link CorfuRuntime} which points to the first server. This {@link CorfuRuntime}
     *  will not be cached.
     * @return  A new {@link CorfuRuntime}.
     */
    public CorfuRuntime getNewRuntime() {
        CorfuRuntime rt = CorfuRuntime.fromParameters(CorfuRuntimeParameters.builder()
            .layoutServer(SERVER_0.getLocator())
            .socketType(ChannelImplementation.LOCAL)
            .nettyEventLoop(getRuntimeGroup())
            .shutdownNettyEventLoop(false)
            .build());
        rt.connect();
        return rt;
    }
}
