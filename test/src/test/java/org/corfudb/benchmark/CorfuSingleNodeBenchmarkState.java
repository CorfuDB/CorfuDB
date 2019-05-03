package org.corfudb.benchmark;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;

import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.CorfuServerNode;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.util.NodeLocator;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * State for a Corfu benchmark. Initializes a simple single-server instance.
 * Benchmark states for {@link AbstractCorfuBenchmark} should extend from this class.
 *
 * Created by zlokhandwala on 5/3/19.
 */
@State(Scope.Benchmark)
public class CorfuSingleNodeBenchmarkState {

    /**
     * A single boss group for the trial.
     */
    @Getter
    EventLoopGroup bossGroup;

    /**
     * A single worker group for the trial.
     */
    @Getter
    EventLoopGroup workerGroup;

    /**
     * A single client group for the trial.
     */
    @Getter
    EventLoopGroup clientGroup;

    /**
     * A single runtime group for the trial.
     */
    @Getter
    EventLoopGroup runtimeGroup;

    /**
     * A map of servers, updated on each iteration.
     */
    @Getter
    Map<NodeLocator, CorfuServerNode> serverMap;

    private final String SERVER_HOST = "localhost";
    private final int SERVER_PORT = 9000;
    private final NodeLocator SERVER_NODE = NodeLocator.parseString(SERVER_HOST + ":" + SERVER_PORT);

    /**
     * Do iteration level setup, which sets up the server.
     *
     * You most likely want to override this method to load objects or initialize
     * iteration state, after calling this super method.
     */
    @Setup(Level.Iteration)
    public void initializeIteration() {

        final ServerContext serverContext = new ServerContextBuilder()
                .setTest(false)
                .setAddress(SERVER_HOST)
                .setPort(SERVER_PORT)
                .setImplementation("nio")
                .setBossGroup(getBossGroup())
                .setWorkerGroup(getWorkerGroup())
                .setClientGroup(getClientGroup())
                .build();

        CorfuServerNode singleServer =
                new CorfuServerNode(serverContext,
                        ImmutableMap.<Class, AbstractServer>builder()
                                .put(BaseServer.class, new BaseServer(serverContext))
                                .put(SequencerServer.class, new SequencerServer(serverContext))
                                .put(LayoutServer.class, new LayoutServer(serverContext))
                                .put(LogUnitServer.class, new LogUnitServer(serverContext))
                                .build()
                );

        singleServer.getServer(SequencerServer.class)
                .setSequencerEpoch(0);

        singleServer.start();

        serverMap.put(SERVER_NODE, singleServer);
    }

    /**
     * Do trial level setup, which sets up the event loops.
     */
    @Setup(Level.Trial)
    public void initializeTrial() {
        bossGroup = new NioEventLoopGroup(1,
                new ThreadFactoryBuilder()
                        .setNameFormat("boss-%d")
                        .setDaemon(true)
                        .build());
        int numThreads = Runtime.getRuntime().availableProcessors() * 2;

        workerGroup = new NioEventLoopGroup(numThreads,
                new ThreadFactoryBuilder()
                        .setNameFormat("worker-%d")
                        .setDaemon(true)
                        .build());

        clientGroup = new NioEventLoopGroup(numThreads,
                new ThreadFactoryBuilder()
                        .setNameFormat("client-%d")
                        .setDaemon(true)
                        .build());

        runtimeGroup = new NioEventLoopGroup(numThreads,
                new ThreadFactoryBuilder()
                        .setNameFormat("netty-%d")
                        .setDaemon(true)
                        .build());


        serverMap = new HashMap<>();
    }

    /**
     * Tear down the trial state, which shuts down the event loops.
     */
    @TearDown(Level.Trial)
    public void teardownTrial() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
        runtimeGroup.shutdownGracefully();
    }

    /**
     * Tear down the iteration state, which shuts down the servers.
     */
    @TearDown(Level.Iteration)
    public void teardownIteration() {
        serverMap.values().forEach(CorfuServerNode::close);
    }

    /**
     * Get a new {@link CorfuRuntime} which points to the first server.
     * The netty event loop group is reused between trials and iterations.
     * This can be overridden in the BenchmarkState if required.
     *
     * @return A new {@link CorfuRuntime}.
     */
    public CorfuRuntime getNewRuntime() {
        CorfuRuntime rt = CorfuRuntime.fromParameters(CorfuRuntimeParameters.builder()
                .layoutServer(SERVER_NODE)
                .nettyEventLoop(getRuntimeGroup())
                .shutdownNettyEventLoop(false)
                .build());
        rt.connect();
        return rt;
    }
}
