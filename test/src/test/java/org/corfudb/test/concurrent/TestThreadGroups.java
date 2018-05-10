package org.corfudb.test.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import javax.annotation.Nonnull;
import org.corfudb.util.concurrent.SingletonResource;

public class TestThreadGroups {

    /**
     * Netty "boss" thread group which is reused in tests.
     */
    public static SingletonResource<EventLoopGroup> NETTY_BOSS_GROUP =
        SingletonResource.withInitial(() ->
            getEventLoopGroup(1, "boss-%d"));

    /**
     * Netty "worker" thread group which is reused in tests.
     */
    public static SingletonResource<EventLoopGroup> NETTY_WORKER_GROUP =
        SingletonResource.withInitial(() ->
            getEventLoopGroup(getTestThreadCount(), "worker-%d"));

    /**
     * Netty "client" thread group which is reused in tests.
     */
    public static SingletonResource<EventLoopGroup> NETTY_CLIENT_GROUP =
        SingletonResource.withInitial(() ->
            getEventLoopGroup(getTestThreadCount(), "client-%d"));


    /**
     * Gracefully shutdown the event loop groups.
     */
    public static void shutdownThreadGroups() {
        NETTY_BOSS_GROUP.cleanup(EventLoopGroup::shutdownGracefully);
        NETTY_WORKER_GROUP.cleanup(EventLoopGroup::shutdownGracefully);
        NETTY_CLIENT_GROUP.cleanup(EventLoopGroup::shutdownGracefully);
    }

    /**
     * Get the number of threads to be used in a test.
     * @return  The number of threads to use.
     */
    private static int getTestThreadCount(){
        return Runtime.getRuntime().availableProcessors() * 2;
    }

    /** Get an {@link io.netty.channel.EventLoopGroup} for Netty.
     *
     * @param numThreads    The number of threads in the {@link EventLoopGroup}.
     * @param nameFormat    A format string for the threads in the {@link EventLoopGroup}
     * @return              The {@link EventLoopGroup} generated.
     */
    private static EventLoopGroup getEventLoopGroup(int numThreads, @Nonnull String nameFormat) {
        return new DefaultEventLoopGroup(numThreads,
            new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(true)
                .build());
    }
}
